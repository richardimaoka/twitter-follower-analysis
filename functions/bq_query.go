package functions

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"
	"strconv"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

type User struct {
	UserId string `bigquery:"user_id"`
}

func QueryIntoBq(ctx context.Context, client *bigquery.Client, cursor int) (string, error) {
	q := client.Query("SELECT user_id FROM `richard-twitter-extraction.twitter.user_ids`")
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = "US"

	// Run the query and print results when the query job is completed.
	it, err := q.Read(ctx)
	if err != nil {
		return "", err
	}

	//look for the matching row
	var u User
	for row := 0; ; row++ {
		err := it.Next(&u)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}
		if row == cursor {
			return u.UserId, nil
		}
	}

	return "", errors.New("No matching row in users BQ table")
}

func BqQuery(projectId string, userCursor int) (string, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("Failed to initialize bq client:%v", err)
	}
	defer client.Close()

	userId, err := QueryIntoBq(ctx, client, userCursor)
	if err != nil {
		return "", err
	}

	return userId, nil
}

type TwitterRequest struct {
	UserId              string `json:"user_id"`
	NextPagenationToken string `json:"next_pagenation_token"`
}

func ConstructTwitterRequestMessage(userId string) (*pubsub.Message, error) {
	bytes, err := json.Marshal(TwitterRequest{userId, ""})
	if err != nil {
		return nil, err
	}
	return &pubsub.Message{Data: bytes}, nil
}

func PublishUserId(projectId string, message *pubsub.Message) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	topic := client.Topic("twitter-request")

	result := topic.Publish(ctx, message)
	_, err = result.Get(ctx)
	if err != nil {
		return err
	}
	return nil
}

func QueryUserId(ctx context.Context, m pubsub.Message) error {
	projectId := os.Getenv("GCP_PROJECT")

	data := string(m.Data)
	userCursor, err := strconv.Atoi(data)
	if err != nil {
		log.Fatalf("error getting user cursor value: %v\n"+"%s", err, data)
		return err
	}
	log.Printf("Received pubsub message ID = %s, data = %s, converted to userCursor = %d", m.ID, m.Data, userCursor)

	userId, err := BqQuery(projectId, userCursor)
	if err != nil {
		log.Fatalf("error in BqQuery: %v", err)
		return err
	}
	log.Printf("Retrieved from BigQuery: userId = %s", userId)

	message, err := ConstructTwitterRequestMessage(userId)
	if err != nil {
		log.Fatalf("error in ConstructTwitterRequestMessage: %v", err)
		return err
	}
	log.Printf("Constructed message: Data = %s", *&message.Data)

	err = PublishUserId(projectId, message)
	if err != nil {
		log.Fatalf("error publishing to PubSub: %v", err)
		return err
	}
	log.Println("Published message")

	return nil
}
