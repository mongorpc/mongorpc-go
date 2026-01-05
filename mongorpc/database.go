package mongorpc

import (
	"context"

	pb "github.com/mongorpc/mongorpc-go/gen/mongorpc/v1"
)

// Database represents a MongoDB database.
type Database struct {
	client *Client
	name   string
}

// Name returns the database name.
func (d *Database) Name() string {
	return d.name
}

// Collection returns a collection handle.
func (d *Database) Collection(name string) *Collection {
	return &Collection{
		database: d,
		name:     name,
	}
}

// ListCollections lists all collections in the database.
func (d *Database) ListCollections() ([]string, error) {
	resp, err := d.client.rpc.ListCollections(d.client.authContext(context.Background()), &pb.ListCollectionsRequest{
		Database: d.name,
	})
	if err != nil {
		return nil, err
	}

	collections := make([]string, len(resp.Collections))
	for i, c := range resp.Collections {
		collections[i] = c.Name
	}
	return collections, nil
}

// CreateCollection creates a new collection.
func (d *Database) CreateCollection(name string) (*Collection, error) {
	_, err := d.client.rpc.CreateCollection(d.client.authContext(context.Background()), &pb.CreateCollectionRequest{
		Database:   d.name,
		Collection: name,
	})
	if err != nil {
		return nil, err
	}
	return d.Collection(name), nil
}

// DropCollection drops a collection.
func (d *Database) DropCollection(name string) error {
	_, err := d.client.rpc.DropCollection(d.client.authContext(context.Background()), &pb.DropCollectionRequest{
		Database:   d.name,
		Collection: name,
	})
	return err
}
