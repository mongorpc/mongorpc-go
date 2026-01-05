package mongorpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
)

// DocumentSnapshot represents the current state of a document.
type DocumentSnapshot struct {
	// ID is the document's unique identifier.
	ID string
	// Data contains the document's fields. Nil if the document doesn't exist.
	Data Document
	// Exists indicates whether the document exists.
	Exists bool
}

// OnSnapshotCallback is called when document state changes.
// It receives the new snapshot and any error that occurred.
type OnSnapshotCallback func(snapshot *DocumentSnapshot, err error)

// OnSnapshot listens to real-time updates for a specific document.
// It first fetches the current state, then streams subsequent changes.
// The callback is invoked:
//   - Once immediately with the initial state (or Exists=false if not found).
//   - On every update, replace, or delete to the document.
//
// The function blocks until the context is cancelled or an error occurs.
// Returns nil on clean context cancellation.
func (c *Collection) OnSnapshot(ctx context.Context, docID string, callback OnSnapshotCallback) error {
	// Validate docID is a valid 24-character hex string (ObjectID format)
	if len(docID) != 24 {
		err := fmt.Errorf("invalid document ID length: expected 24, got %d", len(docID))
		callback(nil, err)
		return err
	}
	if _, err := hex.DecodeString(docID); err != nil {
		err := fmt.Errorf("invalid document ID: not a valid hex string")
		callback(nil, err)
		return err
	}

	// Buffer for events that arrive before initial fetch completes
	eventBuffer := make([]*ChangeEvent, 0)
	var bufferMu sync.Mutex
	initialFetchDone := false

	// Create a derived context for the watch goroutine
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	// Start watch with document ID filter
	pipeline := []Document{
		{
			"$match": Document{
				"documentKey._id": Document{"$oid": docID},
			},
		},
	}

	// Start watching in background, buffering events until initial fetch completes
	eventChan, err := c.Watch(watchCtx, pipeline)
	if err != nil {
		callback(nil, err)
		return err
	}

	// Goroutine to buffer events until initial fetch is done
	go func() {
		for {
			select {
			case <-watchCtx.Done():
				return
			case event, ok := <-eventChan:
				if !ok {
					return
				}

				bufferMu.Lock()
				if !initialFetchDone {
					// Buffer the event
					eventBuffer = append(eventBuffer, event)
					bufferMu.Unlock()
				} else {
					bufferMu.Unlock()
					// Process event directly
					snapshot := applyChangeEvent(docID, event)
					if snapshot != nil {
						callback(snapshot, nil)
					}
				}
			}
		}
	}()

	// Perform initial fetch
	doc, err := c.FindByID(ctx, docID)
	if err != nil {
		callback(nil, err)
		return err
	}

	// Emit initial state
	initialSnapshot := &DocumentSnapshot{
		ID:     docID,
		Data:   doc,
		Exists: doc != nil,
	}
	callback(initialSnapshot, nil)

	// Mark initial fetch as done and process buffered events
	bufferMu.Lock()
	initialFetchDone = true
	bufferedEvents := make([]*ChangeEvent, len(eventBuffer))
	copy(bufferedEvents, eventBuffer)
	eventBuffer = nil // Allow GC
	bufferMu.Unlock()

	// Process buffered events
	for _, event := range bufferedEvents {
		snapshot := applyChangeEvent(docID, event)
		if snapshot != nil {
			callback(snapshot, nil)
		}
	}

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// applyChangeEvent converts a ChangeEvent to a DocumentSnapshot.
func applyChangeEvent(docID string, event *ChangeEvent) *DocumentSnapshot {
	if event == nil {
		return nil
	}

	snapshot := &DocumentSnapshot{
		ID: docID,
	}

	switch event.OperationType {
	case "insert", "update", "replace":
		snapshot.Exists = true
		snapshot.Data = event.FullDocument
	case "delete":
		snapshot.Exists = false
		snapshot.Data = nil
	case "invalidate":
		// Collection invalidated, treat as non-existent
		snapshot.Exists = false
		snapshot.Data = nil
		slog.Warn("OnSnapshot: Collection invalidated", "docID", docID)
	default:
		// Unknown event type, log and skip
		slog.Debug("OnSnapshot: Unhandled event type", "type", event.OperationType)
		return nil
	}

	return snapshot
}

// QuerySnapshot represents the current state of a query result.
type QuerySnapshot struct {
	// Documents contains all documents currently matching the query.
	Documents []Document
	// Count is the number of documents in the result.
	Count int
}

// OnQuerySnapshotCallback is called when query results change.
type OnQuerySnapshotCallback func(snapshot *QuerySnapshot, err error)

// OnQuerySnapshot listens to real-time updates for a filtered query.
// It first fetches documents matching the filter, then streams updates.
// The callback is invoked:
//   - Once immediately with the initial matching documents.
//   - Whenever a document enters or leaves the result set.
//
// Note: Uses broad watch with client-side filtering for accuracy.
// The function blocks until the context is cancelled or an error occurs.
func (c *Collection) OnQuerySnapshot(ctx context.Context, filter Filter, callback OnQuerySnapshotCallback) error {
	// Local state: map of ID -> Document for matching docs
	state := make(map[string]Document)
	var stateMu sync.Mutex

	// Create a derived context for the watch goroutine
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	// Buffer for events that arrive before initial fetch completes
	eventBuffer := make([]*ChangeEvent, 0)
	var bufferMu sync.Mutex
	initialFetchDone := false

	// Start watching the entire collection (broad watch)
	eventChan, err := c.Watch(watchCtx, nil, ChangeStreamOptions{FullDocument: "updateLookup"}) // No pipeline filter
	if err != nil {
		callback(nil, err)
		return err
	}

	// Goroutine to buffer events until initial fetch is done
	go func() {
		for {
			select {
			case <-watchCtx.Done():
				return
			case event, ok := <-eventChan:
				if !ok {
					return
				}

				bufferMu.Lock()
				if !initialFetchDone {
					eventBuffer = append(eventBuffer, event)
					bufferMu.Unlock()
				} else {
					bufferMu.Unlock()
					// Process event and update state
					if processQueryEvent(event, filter, state, &stateMu) {
						callback(buildQuerySnapshot(state, &stateMu), nil)
					}
				}
			}
		}
	}()

	// Perform initial fetch
	docs, err := c.Find(ctx, filter)
	if err != nil {
		callback(nil, err)
		return err
	}

	// Populate initial state
	stateMu.Lock()
	for _, doc := range docs {
		if id, ok := doc["_id"].(string); ok {
			state[id] = doc
		}
	}
	stateMu.Unlock()

	// Emit initial state
	callback(buildQuerySnapshot(state, &stateMu), nil)

	// Mark initial fetch as done and process buffered events
	bufferMu.Lock()
	initialFetchDone = true
	bufferedEvents := make([]*ChangeEvent, len(eventBuffer))
	copy(bufferedEvents, eventBuffer)
	eventBuffer = nil
	bufferMu.Unlock()

	// Process buffered events
	for _, event := range bufferedEvents {
		if processQueryEvent(event, filter, state, &stateMu) {
			callback(buildQuerySnapshot(state, &stateMu), nil)
		}
	}

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// processQueryEvent processes a change event and updates the state map.
// Returns true if the state changed (callback should be invoked).
func processQueryEvent(event *ChangeEvent, filter Filter, state map[string]Document, mu *sync.Mutex) bool {
	if event == nil {
		return false
	}

	// Extract document ID
	var docID string
	if event.FullDocument != nil {
		if id, ok := event.FullDocument["_id"].(string); ok {
			docID = id
		}
	}
	if docID == "" && event.DocumentKey != nil {
		if id, ok := event.DocumentKey["_id"].(string); ok {
			docID = id
		}
	}
	if docID == "" {
		return false
	}

	mu.Lock()
	defer mu.Unlock()

	switch event.OperationType {
	case "insert", "update", "replace":
		if event.FullDocument != nil {
			if matchesFilter(event.FullDocument, filter) {
				// Document matches filter, add/update in state
				state[docID] = event.FullDocument
				return true
			} else {
				// Document doesn't match filter, remove if present
				if _, exists := state[docID]; exists {
					delete(state, docID)
					return true
				}
			}
		}
	case "delete":
		if _, exists := state[docID]; exists {
			delete(state, docID)
			return true
		}
	case "invalidate":
		// Clear all state
		for k := range state {
			delete(state, k)
		}
		return true
	}

	return false
}

// matchesFilter checks if a document matches the given filter.
// This is a simple implementation that checks for equality of top-level fields.
func matchesFilter(doc Document, filter Filter) bool {
	if len(filter) == 0 {
		return true // Empty filter matches all
	}

	for key, filterValue := range filter {
		docValue, exists := doc[key]
		if !exists {
			return false
		}
		// Simple equality check
		if fmt.Sprintf("%v", docValue) != fmt.Sprintf("%v", filterValue) {
			return false
		}
	}
	return true
}

// buildQuerySnapshot creates a QuerySnapshot from the current state.
func buildQuerySnapshot(state map[string]Document, mu *sync.Mutex) *QuerySnapshot {
	mu.Lock()
	defer mu.Unlock()

	docs := make([]Document, 0, len(state))
	for _, doc := range state {
		docs = append(docs, doc)
	}

	return &QuerySnapshot{
		Documents: docs,
		Count:     len(docs),
	}
}
