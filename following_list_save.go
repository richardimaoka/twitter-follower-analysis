package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

type User struct {
	UserId string `bigquery:"user_id"`
}

// queryBasic demonstrates issuing a query and reading results.
func queryBq(cursor int, projectID string) (*User, error) {
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
	return &user, nil
}

func saveJsonToGCSObject(jsonReader io.Reader, bucket, object string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage NewClient: %v", err)
	}
	defer client.Close()

	writer := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = io.Copy(writer, jsonReader); err != nil {
		return fmt.Errorf("Failed to writer into GCS bucket = %s object = %s by io.Copy: %v", bucket, object, err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("Failed to close GCS writer by Write.Close: %v", err)
	}
	return nil
}

func createGcsBucketIfNotExist(projectId, bucket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("Failed to get a GCS client")
	}

	// Creates a Bucket handle
	bucketHandle := client.Bucket(bucket)
	if _, err := bucketHandle.Attrs(ctx); err == nil {
		// bucket already exists
		return nil
	}
	// Creates the new bucket.
	if err := bucketHandle.Create(ctx, projectId, nil); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	return nil
}

func fetchAndSaveJson(userId, bearerToken, bucket, object string) error {
	UserFields := "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld"
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitter.com/2/users/%s/followers?max_results=1000&user.fields=%s", userId, UserFields), nil)
	if err != nil {
		return err
	}

	req.Header.Add("Authorization", "Bearer "+bearerToken)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if err := saveJsonToGCSObject(resp.Body, bucket, object); err != nil {
		log.Fatalf("Failed to save json to GCS object: %v\n", err)
	}

	return nil
}

type Iteration struct {
	UserCursor          int    `json:"user_cursor"`
	NextPagenationToken string `json:"next_pagenation_token"`
}

func FollowingListSave(ctx context.Context, m pubsub.Message) error {
	projectId := os.Getenv("GCP_PROJECT")
	bearerToken := os.Getenv("BEARER_TOKEN")

	var i Iteration
	err := json.Unmarshal([]byte(m.Data), &i)
	if err != nil {
		log.Fatalf("error unmarshaling json %v", m.Data)
	}

	bucket := "my-new-buckettttttttttt-francepddan"
	object := "twitttt.json"

	userId, err := queryBq(i.UserCursor, projectId)

	//maybe not needed, and assume that the bucket is creaated beforehand?
	if err := createGcsBucketIfNotExist(projectId, bucket); err != nil {
		log.Fatalf("Failed to create a GCS bucket: %v\n", err)
	}

	if err := fetchAndSaveJson(userId, bearerToken, bucket, object); err != nil {
		log.Fatalf("Failed to fetch and save json %v\n", err)
	}
	return nil
}
