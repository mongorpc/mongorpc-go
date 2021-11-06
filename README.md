# mongorpc-go

A Go Client implementation of the MongoRPC with Synthetic Sugar Syntex.

## Example


```go
// List Collections
collections, err := db.ListCollectionNames(context.TODO())
if err != nil {
  fmt.Println(err)
}
fmt.Println(collections)

// Get Document By ID
doc, err := db.Collection("movies").Document("573a13b0f29313caabd35231").Get(context.TODO())
if err != nil {
  fmt.Println(err)
}
fmt.Println(doc)

```
