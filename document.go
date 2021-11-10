package mongorpcgo

import (
	"context"

	"github.com/mongorpc/mongorpc"
	"github.com/mongorpc/mongorpc/proto"
)

// Document is a wrapper for a document in a collection.
type Document struct {
	client     *MongoRPCClient
	documentID string
	parent     *Collection
}

// Initilize the document with the document id.
func (coll *Collection) Document(documentID string) *Document {
	//	return document with the given id.
	return &Document{
		documentID: documentID,
		client:     coll.client,
		parent:     coll,
	}
}

// Get returns the document with the given id.
func (doc *Document) Get(ctx context.Context) (interface{}, error) {

	database := doc.parent.parent
	collection := doc.parent

	// crate mongorpc get document request
	resp, err := doc.client.mongorpc.GetDocument(ctx, &proto.GetDocumentRequest{
		Database:   database.name,
		Collection: collection.name,
		DocumentId: doc.documentID,
	})
	if err != nil {
		return nil, err
	}

	// decode mongorpc proto result to interface
	result := mongorpc.Decode(resp.Document)
	return result, nil
}
