package mongorpcgo

import (
	"context"

	"github.com/mongorpc/mongorpc"
	"github.com/mongorpc/mongorpc/proto"
)

type Database struct {
	client *MongoRPCClient
	name   string
}

// Initiliaze a new database
func (client *MongoRPCClient) Database(name string) *Database {
	// Return a new database
	return &Database{
		name:   name,
		client: client,
	}
}

// List collections in the database
func (db *Database) ListCollectionNames(ctx context.Context) ([]string, error) {

	// Do the RPC call
	resp, err := db.client.mongorpc.ListCollections(ctx, &proto.ListCollectionsRequest{
		Database: db.name,
	})
	if err != nil {
		return nil, err
	}

	// Decode the response
	collections := []string{}
	for _, value := range resp.Collections.Values {
		collections = append(collections, mongorpc.DecodeValue(value).(string))
	}

	// Return the result
	return collections, nil
}
