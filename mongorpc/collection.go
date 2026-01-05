package mongorpc

import (
	"context"
)

// Collection provides CRUD operations for a MongoDB collection.
type Collection struct {
	database *Database
	name     string
}

// Name returns the collection name.
func (c *Collection) Name() string {
	return c.name
}

// DatabaseName returns the database name.
func (c *Collection) DatabaseName() string {
	return c.database.name
}

// FindByID finds a document by its ID.
func (c *Collection) FindByID(ctx context.Context, id string) (Document, error) {
	// TODO: Implement via gRPC GetDocument
	return nil, nil
}

// FindOne finds a single document matching the filter.
func (c *Collection) FindOne(ctx context.Context, filter Filter) (Document, error) {
	docs, err := c.Find(ctx, filter, FindOptions{Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(docs) == 0 {
		return nil, ErrNotFound
	}
	return docs[0], nil
}

// Find finds documents matching the filter.
func (c *Collection) Find(ctx context.Context, filter Filter, opts ...FindOptions) ([]Document, error) {
	// TODO: Implement via gRPC ListDocuments
	return nil, nil
}

// InsertOne inserts a single document.
func (c *Collection) InsertOne(ctx context.Context, doc Document) (*InsertResult, error) {
	// TODO: Implement via gRPC CreateDocument
	return &InsertResult{}, nil
}

// InsertMany inserts multiple documents.
func (c *Collection) InsertMany(ctx context.Context, docs []Document) (*InsertManyResult, error) {
	// TODO: Implement via gRPC InsertMany
	return &InsertManyResult{InsertedCount: int64(len(docs))}, nil
}

// UpdateByID updates a document by its ID.
func (c *Collection) UpdateByID(ctx context.Context, id string, update Update) (*UpdateResult, error) {
	return c.UpdateOne(ctx, Filter{"_id": id}, update)
}

// UpdateOne updates a single document.
func (c *Collection) UpdateOne(ctx context.Context, filter Filter, update Update) (*UpdateResult, error) {
	// TODO: Implement via gRPC UpdateDocument
	return &UpdateResult{}, nil
}

// UpdateMany updates multiple documents.
func (c *Collection) UpdateMany(ctx context.Context, filter Filter, update Update) (*UpdateResult, error) {
	// TODO: Implement via gRPC UpdateMany
	return &UpdateResult{}, nil
}

// DeleteByID deletes a document by its ID.
func (c *Collection) DeleteByID(ctx context.Context, id string) (*DeleteResult, error) {
	return c.DeleteOne(ctx, Filter{"_id": id})
}

// DeleteOne deletes a single document.
func (c *Collection) DeleteOne(ctx context.Context, filter Filter) (*DeleteResult, error) {
	// TODO: Implement via gRPC DeleteDocument
	return &DeleteResult{}, nil
}

// DeleteMany deletes multiple documents.
func (c *Collection) DeleteMany(ctx context.Context, filter Filter) (*DeleteResult, error) {
	// TODO: Implement via gRPC DeleteMany
	return &DeleteResult{}, nil
}

// CountDocuments counts documents matching the filter.
func (c *Collection) CountDocuments(ctx context.Context, filter Filter) (int64, error) {
	// TODO: Implement via gRPC CountDocuments
	return 0, nil
}

// Distinct returns distinct values for a field.
func (c *Collection) Distinct(ctx context.Context, field string, filter Filter) ([]any, error) {
	// TODO: Implement via gRPC
	return nil, nil
}

// Aggregate runs an aggregation pipeline.
func (c *Collection) Aggregate(ctx context.Context, pipeline []Document) ([]Document, error) {
	// TODO: Implement via gRPC Aggregate
	return nil, nil
}

// Query returns a fluent query builder.
func (c *Collection) Query() *QueryBuilder {
	return newQueryBuilder(c)
}
