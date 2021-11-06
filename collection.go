package mongorpcgo

import (
	"context"

	"github.com/mongorpc/mongorpc"
	"github.com/mongorpc/mongorpc/proto"
)

func (c *MongoRPCClient) ListCollections(ctx context.Context, database string) ([]string, error) {
	resp, err := c.mongorpc.ListCollections(ctx, &proto.ListCollectionsRequest{
		Database: database,
	})
	if err != nil {
		return nil, err
	}

	collections := []string{}
	for _, value := range resp.Collections.Values {
		collections = append(collections, mongorpc.DecodeValue(value).(string))
	}

	return collections, nil
}
