package p

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"google.golang.org/api/iterator"
)

type user struct {
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
	var u user
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
	ctx = context.Background()
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

func PublishUserId(userId string) {

}

func QueryUserId(ctx context.Context, m pubsub.Message) {
	projectId := os.Getenv("GCP_PROJECT")

	var userCursor int
	err := json.Unmarshal([]byte(m.Data), &userCursor)
	if err != nil {
		log.Fatalf("error getting user cursor value: %s\n"+"%v", err.Error(), m.Data)
	}

	userId, err := BqQuery(projectId, userCursor)
	if err != nil {
		log.Fatalf("error in BqQuery: %s", err.Error())
	}

	PublishUserId(userId)
}
