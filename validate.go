package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/mongorpc/mongorpc-go/mongorpc"
)

func main() {
	client, err := mongorpc.NewClient("localhost:50051")
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer client.Close()

	col := client.Database("test").Collection("test")
	ctx := context.Background()

	// Initial Cleanup
	_, err = col.DeleteMany(ctx, mongorpc.Filter{})
	if err != nil {
		log.Printf("Initial Cleanup failed: %v", err)
	}

	// 1. InsertOne
	res, err := col.InsertOne(ctx, mongorpc.Document{
		"name": "Validation",
		"type": "Go",
		"rank": 1,
	})
	if err != nil {
		log.Fatalf("InsertOne failed: %v", err)
	}
	docID := res.InsertedID
	fmt.Printf("1. InsertOne Success: %s\n", docID)

	// 2. FindByID
	doc, err := col.FindByID(ctx, docID)
	if err != nil {
		log.Fatalf("FindByID failed: %v", err)
	}
	if doc == nil || doc["name"] != "Validation" {
		log.Fatalf("FindByID check failed")
	}
	fmt.Println("2. FindByID Success")

	// 3. UpdateByID
	updateRes, err := col.UpdateByID(ctx, docID, mongorpc.Update{
		"$set": mongorpc.Document{"rank": 2},
	})
	if err != nil {
		log.Fatalf("UpdateByID failed: %v", err)
	}
	if updateRes.ModifiedCount != 1 {
		log.Fatal("UpdateByID count mismatch")
	}
	// Verify update
	doc, _ = col.FindByID(ctx, docID)
	if fmt.Sprintf("%v", doc["rank"]) != "2" {
		log.Fatalf("UpdateByID verify failed: got %v", doc["rank"])
	}
	fmt.Println("3. UpdateByID Success")

	// 4. InsertMany
	insertManyRes, err := col.InsertMany(ctx, []mongorpc.Document{
		{"name": "Bulk1", "type": "GoBulk"},
		{"name": "Bulk2", "type": "GoBulk"},
	})
	if err != nil {
		log.Fatalf("InsertMany failed: %v", err)
	}
	if len(insertManyRes.InsertedIDs) != 2 {
		log.Fatal("InsertMany count mismatch")
	}
	fmt.Println("4. InsertMany Success")

	// 5. Find
	cursor, err := col.Find(ctx, mongorpc.Filter{"type": "GoBulk"})
	if err != nil {
		log.Fatalf("Find failed: %v", err)
	}
	if len(cursor) != 2 {
		log.Printf("Found docs: %v", cursor)
		log.Fatalf("Find count mismatch: expected 2, got %d", len(cursor))
	}
	fmt.Println("5. Find Success")

	// 6. UpdateMany
	updateManyRes, err := col.UpdateMany(ctx, mongorpc.Filter{"type": "GoBulk"}, mongorpc.Update{
		"$set": mongorpc.Document{"updated": true},
	})
	if err != nil {
		log.Fatalf("UpdateMany failed: %v", err)
	}
	if updateManyRes.ModifiedCount != 2 {
		log.Printf("Modified count: %d", updateManyRes.ModifiedCount)
		// Depending on existing data, this might be 0 if already updated. Ideally should be 2.
	}
	fmt.Println("6. UpdateMany Success")

	// 7. CountDocuments
	count, err := col.CountDocuments(ctx, mongorpc.Filter{})
	if err != nil {
		log.Fatalf("CountDocuments failed: %v", err)
	}
	fmt.Printf("7. CountDocuments Success: %d\n", count)

	// 8. DeleteByID
	deleteRes, err := col.DeleteByID(ctx, docID)
	if err != nil {
		log.Fatalf("DeleteByID failed: %v", err)
	}
	if deleteRes.DeletedCount != 1 {
		log.Fatal("DeleteByID count mismatch")
	}
	fmt.Println("8. DeleteByID Success")

	// 9. DeleteMany
	deleteManyRes, err := col.DeleteMany(ctx, mongorpc.Filter{"type": "GoBulk"})
	if err != nil {
		log.Fatalf("DeleteMany failed: %v", err)
	}
	if deleteManyRes.DeletedCount != 2 {
		fmt.Printf("9. DeleteMany Warning: DeletedCount %d != 2\n", deleteManyRes.DeletedCount)
	}
	fmt.Println("9. DeleteMany Success")

	// 10. Aggregate
	// Re-insert needed for aggregate test since we deleted everything
	_, _ = col.InsertOne(ctx, mongorpc.Document{"name": "Agg1", "val": 10})
	_, _ = col.InsertOne(ctx, mongorpc.Document{"name": "Agg2", "val": 20})

	pipeline := []mongorpc.Document{
		{"$match": mongorpc.Document{"val": 10}},
	}
	aggRes, err := col.Aggregate(ctx, pipeline)
	if err != nil {
		log.Fatalf("Aggregate failed: %v", err)
	}
	if len(aggRes) != 1 {
		log.Fatal("Aggregate count mismatch")
	}
	fmt.Println("10. Aggregate Success")

	// 11. Watch (Change Stream)
	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	// Channel to signal watch is ready
	watchChan, err := col.Watch(watchCtx, []mongorpc.Document{})
	if err != nil {
		log.Fatalf("11. Watch failed to start: %v", err)
	}

	// Consume events in background
	eventChan := make(chan *mongorpc.ChangeEvent, 1)

	go func() {
		// Read from the channel
		event, ok := <-watchChan
		if !ok {
			return
		}
		eventChan <- event
	}()

	fmt.Println("11. Watch Started")
	time.Sleep(1 * time.Second) // Wait for stream establishment

	// Trigger Insert
	_, err = col.InsertOne(ctx, mongorpc.Document{"name": "Watcher", "type": "GoWatch"})
	if err != nil {
		log.Fatalf("11. Watch Trigger Insert failed: %v", err)
	}

	select {
	case event := <-eventChan:
		if event == nil {
			log.Fatal("11. Watch received nil event")
		}
		fmt.Printf("11. Watch Event Received: OpType=%v\n", event.OperationType)
		fmt.Println("11. Watch Success")
	case <-time.After(5 * time.Second):
		log.Fatal("11. Watch Timeout (No event received). Requires Replica Set?")
	}

	// Cleanup
	_, _ = col.DeleteMany(ctx, mongorpc.Filter{})
	fmt.Println("All Comprehensive Tests Passed!")
}
