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

// queryBasic demonstrates issuing a query and reading results.
func queryBq2(cursor int, projectID string) (*User, error) {
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	q := client.Query("SELECT user_id FROM `richard-twitter-extraction.twitter.user_ids`")
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

	//look for the matching row
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
		if row == cursor {
			break
		}
		row++
	}

	if row < cursor {
		return nil, errors.New("too big cursor")
	}
	return &user, nil
}

func BqQuery(ctx context.Context, m pubsub.Message) error {
	projectId := os.Getenv("GCP_PROJECT")

	var i Iteration
	err := json.Unmarshal([]byte(m.Data), &i)
	if err != nil {
		log.Fatalf("error unmarshaling json %v", m.Data)
	}

	bucket := "my-new-buckettttttttttt-francepddan"
	object := "twitttt.json"

	user, err := queryBq(i.UserCursor, projectId)

	//maybe not needed, and assume that the bucket is creaated beforehand?
	if err := createGcsBucketIfNotExist(projectId, bucket); err != nil {
		log.Fatalf("Failed to create a GCS bucket: %v\n", err)
	}

	if err := fetchAndSaveJson(user.UserId, bearerToken, bucket, object); err != nil {
		log.Fatalf("Failed to fetch and save json %v\n", err)
	}
	return nil
}
