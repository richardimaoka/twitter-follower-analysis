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
	var user User
	for row := 0; ; row++ {
		err := it.Next(&user)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}
		if row == cursor {
			return user.UserId, nil
		}
	}

	return "", errors.New("No matching row in users BQ table")
}

// queryBasic demonstrates issuing a query and reading results.
func queryBq2(cursor int, projectID string) (*User, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	return &user, nil
}

func BqQuery(ctx context.Context, m pubsub.Message) error {
	projectId := os.Getenv("GCP_PROJECT")

	var i Iteration
	err := json.Unmarshal([]byte(m.Data), &i)
	if err != nil {
		log.Fatalf("error unmarshaling json %v", m.Data)
	}

	queryBq2(0, projectId)

	return nil
}
