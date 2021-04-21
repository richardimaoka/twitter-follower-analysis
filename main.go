package main

import (
	"context"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	"github.com/richardimaoka/twitter-follower-analysis/functions"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()
	msg := pubsub.Message{}
	msg.Data = []byte("1")
	functions.QueryUserId(ctx, &msg)
}
