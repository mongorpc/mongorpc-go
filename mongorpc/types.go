package mongorpc

import "errors"

// Document represents a MongoDB document.
type Document map[string]any

// Filter represents a query filter.
type Filter map[string]any

// Update represents an update operation.
type Update map[string]any

// FindOptions configures find operations.
type FindOptions struct {
	// Projection specifies fields to return
	Projection map[string]int
	// Sort specifies the sort order
	Sort map[string]int
	// Limit limits the number of results
	Limit int64
	// Skip skips a number of results
	Skip int64
}

// InsertResult is returned by InsertOne.
type InsertResult struct {
	InsertedID string
}

// InsertManyResult is returned by InsertMany.
type InsertManyResult struct {
	InsertedIDs   []string
	InsertedCount int64
}

// UpdateResult is returned by update operations.
type UpdateResult struct {
	MatchedCount  int64
	ModifiedCount int64
	UpsertedID    string
}

// DeleteResult is returned by delete operations.
type DeleteResult struct {
	DeletedCount int64
}

// Common errors
var (
	ErrNotFound     = errors.New("mongorpc: document not found")
	ErrInvalidID    = errors.New("mongorpc: invalid document ID")
	ErrConnection   = errors.New("mongorpc: connection error")
	ErrUnauthorized = errors.New("mongorpc: unauthorized")
)
