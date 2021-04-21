package main

import (
	"context"

	"cloud.google.com/go/pubsub"
	"github.com/richardimaoka/twitter-follower-analysis/functions"
)

func main() {
	ctx := context.Background()
	msg := pubsub.Message{}
	msg.Data = []byte("1")
	functions.QueryUserId(ctx, &msg)
}
