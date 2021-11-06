package mongorpcgo

import (
	"context"

	"github.com/mongorpc/mongorpc"
	"github.com/mongorpc/mongorpc/proto"
	"google.golang.org/grpc"
)

type MongoRPCClient struct {
	address  string
	client   *grpc.ClientConn
	mongorpc proto.MongoRPCClient
}

func NewClient(address string) *MongoRPCClient {
	return &MongoRPCClient{
		address: address,
	}
}

func (c *MongoRPCClient) Connect(opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	conn, err := grpc.Dial(
		c.address,
		opts...,
	)
	if err != nil {
		return nil, err
	}

	m := proto.NewMongoRPCClient(conn)
	c.client = conn
	c.mongorpc = m

	return conn, nil
}

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
