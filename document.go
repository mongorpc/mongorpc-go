package mongorpcgo

import (
	"context"

	"github.com/mongorpc/mongorpc"
	"github.com/mongorpc/mongorpc/proto"
)

func (c *MongoRPCClient) GetDocument(ctx context.Context, database string, collection string, documentID string) (interface{}, error) {
	resp, err := c.mongorpc.GetDocument(ctx, &proto.GetDocumentRequest{
		Database:   database,
		Collection: collection,
		DocumentId: documentID,
	})
	if err != nil {
		return nil, err
	}
	doc := mongorpc.DecodeValue(resp.Document)
	return doc, nil
}
