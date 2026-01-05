# mongorpc-go

Go client for MongoRPC - a gRPC proxy for MongoDB.

## Installation

```bash
go get github.com/mongorpc/mongorpc-go/mongorpc
```

## Quick Start

```go
package main

import (
    "context"
    "log"
    
    "github.com/mongorpc/mongorpc-go/mongorpc"
)

func main() {
    // Connect
    client, err := mongorpc.NewClient("localhost:50051",
        mongorpc.WithAPIKey("your-api-key"),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    ctx := context.Background()
    users := client.Database("mydb").Collection("users")

    // Insert
    result, err := users.InsertOne(ctx, mongorpc.Document{
        "name": "Alice",
        "age":  30,
    })

    // Find by ID
    doc, err := users.FindByID(ctx, result.InsertedID)

    // Query builder
    docs, err := users.Query().
        Where("active", true).
        Gte("age", 21).
        SortDesc("createdAt").
        Limit(10).
        All(ctx)

    // Update
    _, err = users.UpdateByID(ctx, result.InsertedID, mongorpc.Update{
        "$set": mongorpc.Document{"verified": true},
    })

    // Delete
    _, err = users.DeleteByID(ctx, result.InsertedID)
}
```

## API

### Client

```go
client, err := mongorpc.NewClient(address,
    mongorpc.WithAPIKey(key),
    mongorpc.WithToken(jwt),
    mongorpc.WithTimeout(10*time.Second),
)
```

### Collection Methods

| Method | Description |
|--------|-------------|
| `FindByID(ctx, id)` | Find by ID |
| `FindOne(ctx, filter)` | Find single document |
| `Find(ctx, filter, opts)` | Find documents |
| `InsertOne(ctx, doc)` | Insert document |
| `InsertMany(ctx, docs)` | Insert multiple |
| `UpdateByID(ctx, id, update)` | Update by ID |
| `UpdateOne/Many(ctx, filter, update)` | Update |
| `DeleteByID(ctx, id)` | Delete by ID |
| `DeleteOne/Many(ctx, filter)` | Delete |
| `CountDocuments(ctx, filter)` | Count |
| `Aggregate(ctx, pipeline)` | Aggregation |
| `Query()` | Fluent query builder |

### Query Builder

```go
users.Query().
    Where("field", value).
    Eq("field", value).
    Gt("field", value).
    In("field", values...).
    Regex("field", "pattern").
    Select("field1", "field2").
    SortAsc("field").
    SortDesc("field").
    Limit(10).
    Skip(20).
    All(ctx)
```

## License

Apache-2.0
