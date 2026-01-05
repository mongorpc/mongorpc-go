package mongorpc

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "github.com/mongorpc/mongorpc-go/gen/mongorpc/v1"
)

// AdminClient provides elevated access to MongoRPC with rule bypass.
type AdminClient struct {
	conn        *grpc.ClientConn
	rpc         pb.MongoRPCClient
	adminKey    string
	adminSecret string
}

// NewAdminClient creates an admin client with API key authentication.
// Admin clients bypass all security rules on the server.
func NewAdminClient(address, adminKey, adminSecret string) (*AdminClient, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, err
	}

	return &AdminClient{
		conn:        conn,
		rpc:         pb.NewMongoRPCClient(conn),
		adminKey:    adminKey,
		adminSecret: adminSecret,
	}, nil
}

// Close closes the admin client connection.
func (a *AdminClient) Close() error {
	return a.conn.Close()
}

// adminContext adds admin credentials to the context.
func (a *AdminClient) adminContext(ctx context.Context) context.Context {
	md := metadata.Pairs(
		"x-admin-key", a.adminKey,
		"x-admin-secret", a.adminSecret,
	)
	return metadata.NewOutgoingContext(ctx, md)
}

// Database returns an AdminDatabase handle for the specified database.
func (a *AdminClient) Database(name string) *AdminDatabase {
	return &AdminDatabase{
		client: a,
		name:   name,
	}
}

// AdminDatabase provides admin operations on a database.
type AdminDatabase struct {
	client *AdminClient
	name   string
}

// Collection returns an AdminCollection handle for the specified collection.
func (d *AdminDatabase) Collection(name string) *AdminCollection {
	return &AdminCollection{
		database: d,
		name:     name,
	}
}

// AdminCollection provides admin operations on a collection.
// All operations bypass security rules.
type AdminCollection struct {
	database *AdminDatabase
	name     string
}

// ctx returns the admin context.
func (c *AdminCollection) ctx(ctx context.Context) context.Context {
	return c.database.client.adminContext(ctx)
}

// rpc returns the gRPC client.
func (c *AdminCollection) rpc() pb.MongoRPCClient {
	return c.database.client.rpc
}

// InsertOne inserts a single document (bypasses rules).
func (c *AdminCollection) InsertOne(ctx context.Context, doc Document) (*InsertResult, error) {
	resp, err := c.rpc().CreateDocument(c.ctx(ctx), &pb.CreateDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Document:   toProtoDocument(doc),
	})
	if err != nil {
		return nil, err
	}

	result := fromProtoDocument(resp.Document)
	id, _ := result["_id"].(string)
	return &InsertResult{InsertedID: id}, nil
}

// FindByID finds a single document by ID (bypasses rules).
func (c *AdminCollection) FindByID(ctx context.Context, id string) (Document, error) {
	resp, err := c.rpc().GetDocument(c.ctx(ctx), &pb.GetDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Id:         &pb.ObjectId{Hex: id},
	})
	if err != nil {
		return nil, err
	}

	return fromProtoDocument(resp.Document), nil
}

// Find finds documents matching the filter (bypasses rules).
func (c *AdminCollection) Find(ctx context.Context, filter Filter, opts ...FindOptions) ([]Document, error) {
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

	resp, err := c.rpc().ListDocuments(c.ctx(ctx), req)
	if err != nil {
		return nil, err
	}

	docs := make([]Document, len(resp.Documents))
	for i, d := range resp.Documents {
		docs[i] = fromProtoDocument(d)
	}

	return docs, nil
}

// UpdateByID updates a document by ID (bypasses rules).
func (c *AdminCollection) UpdateByID(ctx context.Context, id string, update Update) (*UpdateResult, error) {
	resp, err := c.rpc().UpdateDocument(c.ctx(ctx), &pb.UpdateDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Id:         &pb.ObjectId{Hex: id},
		Update:     toProtoUpdate(update),
	})
	if err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  resp.WriteResult.GetMatchedCount(),
		ModifiedCount: resp.WriteResult.GetModifiedCount(),
	}, nil
}

// UpdateMany updates multiple documents (bypasses rules).
func (c *AdminCollection) UpdateMany(ctx context.Context, filter Filter, update Update) (*UpdateResult, error) {
	resp, err := c.rpc().UpdateMany(c.ctx(ctx), &pb.UpdateManyRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
		Update:     toProtoUpdate(update),
	})
	if err != nil {
		return nil, err
	}

	return &UpdateResult{
		MatchedCount:  resp.WriteResult.GetMatchedCount(),
		ModifiedCount: resp.WriteResult.GetModifiedCount(),
	}, nil
}

// DeleteByID deletes a document by ID (bypasses rules).
func (c *AdminCollection) DeleteByID(ctx context.Context, id string) (*DeleteResult, error) {
	resp, err := c.rpc().DeleteDocument(c.ctx(ctx), &pb.DeleteDocumentRequest{
		Database:   c.database.name,
		Collection: c.name,
		Id:         &pb.ObjectId{Hex: id},
	})
	if err != nil {
		return nil, err
	}

	return &DeleteResult{
		DeletedCount: resp.WriteResult.GetDeletedCount(),
	}, nil
}

// DeleteMany deletes multiple documents (bypasses rules).
func (c *AdminCollection) DeleteMany(ctx context.Context, filter Filter) (*DeleteResult, error) {
	resp, err := c.rpc().DeleteMany(c.ctx(ctx), &pb.DeleteManyRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
	})
	if err != nil {
		return nil, err
	}

	return &DeleteResult{
		DeletedCount: resp.WriteResult.GetDeletedCount(),
	}, nil
}

// CountDocuments counts documents matching the filter (bypasses rules).
func (c *AdminCollection) CountDocuments(ctx context.Context, filter Filter) (int64, error) {
	resp, err := c.rpc().CountDocuments(c.ctx(ctx), &pb.CountDocumentsRequest{
		Database:   c.database.name,
		Collection: c.name,
		Filter:     toProtoFilter(filter),
	})
	if err != nil {
		return 0, err
	}

	return resp.Count, nil
}
