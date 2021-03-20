package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/joho/godotenv"
)

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

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	projectId := "richard-twitter-extraction"
	bucket := "my-new-buckettttttttttt-francepddan"
	userId := "2875908842"
	object := "twitttt.json"

	if err := createGcsBucketIfNotExist(projectId, bucket); err != nil {
		log.Fatalf("Failed to create a GCS bucket: %v\n", err)
	}

	if err := fetchAndSaveJson(userId, os.Getenv("BEARER_TOKEN"), bucket, object); err != nil {
		log.Fatalf("Failed to fetch and save json %v\n", err)
	}
}
