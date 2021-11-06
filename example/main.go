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

	collections, err := client.ListCollections(context.TODO(), "sample_mflix")
	if err != nil {
		logrus.Errorln(err)
	}
	logrus.Infoln(collections)
}
