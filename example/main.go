package main

import (
	"context"

	mongorpcgo "github.com/mongorpc/mongorpc-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	client := mongorpcgo.NewClient("localhost:27051")
	conn, err := client.Connect(
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		logrus.Fatalln(err)
	}
	defer conn.Close()

	// Initilize database
	db := client.Database("sample_mflix")

	// List Collections
	collections, err := db.ListCollectionNames(context.TODO())
	if err != nil {
		logrus.Fatalln(err)
	}
	logrus.Println(collections)

	// Get Document By ID
	doc, err := db.Collection("movies").Document("573a13b0f29313caabd35231").Get(context.TODO())
	if err != nil {
		logrus.Errorln(err)
	}
	logrus.Infoln(doc)
}
