package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
)

type User struct {
	UserId string `bigquery:"user_id"`
}

// queryBasic demonstrates issuing a query and reading results.
func queryBasic(nthRow int, w io.Writer, projectID string) (*User, error) {
	fmt.Println("asd")
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	q := client.Query(
		"SELECT user_id FROM `richard-twitter-extraction.twitter.user_ids`")
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = "US"
	// Run the query and print results when the query job is completed.
	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if err := status.Err(); err != nil {
		return nil, err
	}
	it, err := job.Read(ctx)

	row := 0
	var user User
	for {
		err := it.Next(&user)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if row == nthRow {
			break
		}
		row++
	}
	return &user, nil
}

func pub(projectId string) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}

	topicId := "user-id"
	topic := client.Topic(topicId)
	res := topic.Publish(ctx, &pubsub.Message{Data: []byte("payload")})

	serverId, err := res.Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("pub done %v", serverId)

	return nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	userId, err := queryBasic(1, os.Stdout, "richard-twitter-extraction")
	if err != nil {
		fmt.Printf("Error in queryBasic %v", err)
	}
	fmt.Println(userId)

	err = pub("richard-twitter-extraction")
	if err != nil {
		fmt.Printf("%v", err)
	}
}
