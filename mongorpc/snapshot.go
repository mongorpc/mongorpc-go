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
