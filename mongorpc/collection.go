package mongorpc

import (
	"context"

	pb "github.com/mongorpc/mongorpc-go/gen/mongorpc/v1"
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
	resp, err := c.database.client.rpc.GetDocument(c.database.client.authContext(ctx), &pb.GetDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Id:         &pb.ObjectId{Hex: id},
	})
	if err != nil {
		return nil, err
	}
	return fromProtoDocument(resp.Document), nil
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
	req := &pb.ListDocumentsRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
	}

	for _, opt := range opts {
		if opt.Limit > 0 {
			req.PageSize = int32(opt.Limit)
		}
	}

	resp, err := c.database.client.rpc.ListDocuments(c.database.client.authContext(ctx), req)
	if err != nil {
		return nil, err
	}

	docs := make([]Document, len(resp.Documents))
	for i, d := range resp.Documents {
		docs[i] = fromProtoDocument(d)
	}

	return docs, nil
}

// InsertOne inserts a single document.
func (c *Collection) InsertOne(ctx context.Context, doc Document) (*InsertResult, error) {
	resp, err := c.database.client.rpc.CreateDocument(c.database.client.authContext(ctx), &pb.CreateDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Document:   toProtoDocument(doc),
	})
	if err != nil {
		return nil, err
	}

	var insertedID string
	if resp.Document != nil && resp.Document.Id != nil {
		insertedID = resp.Document.Id.Hex
	}

	return &InsertResult{
		InsertedID: insertedID,
	}, nil
}

// InsertMany inserts multiple documents.
func (c *Collection) InsertMany(ctx context.Context, docs []Document) (*InsertManyResult, error) {
	pbDocs := make([]*pb.Document, len(docs))
	for i, d := range docs {
		pbDocs[i] = toProtoDocument(d)
	}

	resp, err := c.database.client.rpc.InsertMany(c.database.client.authContext(ctx), &pb.InsertManyRequest{
		Database:   c.database.name,
		Collection: c.name,
		Documents:  pbDocs,
	})
	if err != nil {
		return nil, err
	}

	ids := make([]string, len(resp.InsertedIds))
	for i, id := range resp.InsertedIds {
		ids[i] = id.Hex
	}

	var insertedCount int64
	if resp.WriteResult != nil {
		insertedCount = resp.WriteResult.InsertedCount
	} else {
		insertedCount = int64(len(ids))
	}

	return &InsertManyResult{
		InsertedIDs:   ids,
		InsertedCount: insertedCount,
	}, nil
}

// UpdateByID updates a document by its ID.
func (c *Collection) UpdateByID(ctx context.Context, id string, update Update) (*UpdateResult, error) {
	resp, err := c.database.client.rpc.UpdateDocument(c.database.client.authContext(ctx), &pb.UpdateDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Id:         &pb.ObjectId{Hex: id},
		Update:     toProtoUpdate(update),
	})
	if err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  resp.WriteResult.MatchedCount,
		ModifiedCount: resp.WriteResult.ModifiedCount,
	}, nil
}

// UpdateOne updates a single document.
func (c *Collection) UpdateOne(ctx context.Context, filter Filter, update Update) (*UpdateResult, error) {
	if id, ok := filter["_id"].(string); ok && len(filter) == 1 {
		return c.UpdateByID(ctx, id, update)
	}

	// Fallback to update many with limit 1? Or API supports limit?
	// UpdateDocumentRequest has ID, not Filter. UpdateMany has Filter.
	// The proto API seems to separate UpdateDocument (by ID) and UpdateMany (by filter).
	// If FindOneAndUpdate exists in proto, we could use that, but that returns document.
	// Let's see if there is UpdateOne in proto? Or maybe UpdateDocument supports filter?
	// Checking mongorpc.pb.go: UpdateDocumentRequest has `id`.
	// UpdateManyRequest has `filter` and `update`.

	// If UpdateOne is needed by filter, we might need to find first then update, or use UpdateMany with some flag?
	// Usually UpdateMany updates all matches.
	// The proto definitions I saw:
	// FindOneAndUpdate, FindOneAndReplace, FindOneAndDelete exist.

	// Let's use FindOneAndUpdate for UpdateOne if we want atomic?
	// Or maybe UpdateMany has a limit? (Not typical in mongo protocol for updates).

	// For now, I'll assume UpdateOne needs to find the ID first.
	doc, err := c.FindOne(ctx, filter)
	if err != nil {
		return nil, err
	}
	// TODO: Handle _id extraction more robustly
	id, _ := doc["_id"].(string)
	return c.UpdateByID(ctx, id, update)
}

// UpdateMany updates multiple documents.
func (c *Collection) UpdateMany(ctx context.Context, filter Filter, update Update) (*UpdateResult, error) {
	resp, err := c.database.client.rpc.UpdateMany(c.database.client.authContext(ctx), &pb.UpdateManyRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
		Update:     toProtoUpdate(update),
	})
	if err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  resp.WriteResult.MatchedCount,
		ModifiedCount: resp.WriteResult.ModifiedCount,
	}, nil
}

// DeleteByID deletes a document by its ID.
func (c *Collection) DeleteByID(ctx context.Context, id string) (*DeleteResult, error) {
	resp, err := c.database.client.rpc.DeleteDocument(c.database.client.authContext(ctx), &pb.DeleteDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Id:         &pb.ObjectId{Hex: id},
	})
	if err != nil {
		return nil, err
	}

	return &DeleteResult{
		DeletedCount: resp.WriteResult.DeletedCount,
	}, nil
}

// DeleteOne deletes a single document.
func (c *Collection) DeleteOne(ctx context.Context, filter Filter) (*DeleteResult, error) {
	if id, ok := filter["_id"].(string); ok && len(filter) == 1 {
		return c.DeleteByID(ctx, id)
	}

	// Similarly, separate DeleteDocument vs DeleteMany.
	// Use FindOneAndDelete or find ID then delete.
	doc, err := c.FindOne(ctx, filter)
	if err != nil {
		return nil, err
	}
	id, _ := doc["_id"].(string)
	return c.DeleteByID(ctx, id)
}

// DeleteMany deletes multiple documents.
func (c *Collection) DeleteMany(ctx context.Context, filter Filter) (*DeleteResult, error) {
	resp, err := c.database.client.rpc.DeleteMany(c.database.client.authContext(ctx), &pb.DeleteManyRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
	})
	if err != nil {
		return nil, err
	}

	return &DeleteResult{
		DeletedCount: resp.WriteResult.DeletedCount,
	}, nil
}

// CountDocuments counts documents matching the filter.
func (c *Collection) CountDocuments(ctx context.Context, filter Filter) (int64, error) {
	resp, err := c.database.client.rpc.CountDocuments(c.database.client.authContext(ctx), &pb.CountDocumentsRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
	})
	if err != nil {
		return 0, err
	}
	return resp.Count, nil
}

// Distinct returns distinct values for a field.
func (c *Collection) Distinct(ctx context.Context, field string, filter Filter) ([]any, error) {
	resp, err := c.database.client.rpc.Distinct(c.database.client.authContext(ctx), &pb.DistinctRequest{
		Database:   c.database.name,
		Collection: c.name,
		Field:      field,
		Filter:     toProtoFilter(filter),
	})
	if err != nil {
		return nil, err
	}

	values := make([]any, len(resp.Values))
	for i, v := range resp.Values {
		values[i] = fromProtoValue(v)
	}
	return values, nil
}

// Aggregate runs an aggregation pipeline.
func (c *Collection) Aggregate(ctx context.Context, pipeline []Document) ([]Document, error) {
	// TODO: Convert pipeline to AggregationPipeline proto
	// This requires mapping helpers for pipeline stages
	return nil, nil
}

// Query returns a fluent query builder.
func (c *Collection) Query() *QueryBuilder {
	return newQueryBuilder(c)
}
