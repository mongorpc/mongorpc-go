package main

import (
	"context"

	mongorpcgo "github.com/mongorpc/mongorpc-go"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func main() {
	client := mongorpcgo.NewClient("localhost:8080")
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
	doc, err := db.Collection("movies").Document("573a1390f29313caabcd4135").Get(context.TODO())
	if err != nil {
		logrus.Errorln(err)
	}
	logrus.Infoln(doc)
}
