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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*20)
	defer cancel()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("Failed to get a GCS client")
	}

	// Creates a Bucket handle
	bucketHandle := client.Bucket(bucket)
	fmt.Println("trying to fetch attrs")
	if _, err := bucketHandle.Attrs(ctx); err == nil {
		// bucket already exists
		return nil
	}
	fmt.Println("trying to create a backet")
	// Creates the new bucket.
	if err := bucketHandle.Create(ctx, projectId, nil); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	return nil
}

func fetchJson(userId, bearerToken string) (io.Reader, error) {
	UserFields := "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld"
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitter.com/2/users/%s/followers?max_results=1000&user.fields=%s", userId, UserFields), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Authorization", "Bearer "+bearerToken)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return resp.Body, nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	fmt.Println("GAC = ", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"))

	projectId := "richard-twitter-extraction"
	bucket := "my-new-buckettttttttttt-francepddan"

	createGcsBucketIfNotExist(projectId, bucket)

	// // Creates the new bucket.
	// ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	// defer cancel()
	// if err := bucket.Create(ctx, projectID, nil); err != nil {
	// 	log.Fatalf("Failed to create bucket: %v", err)
	// }

	// bucket.Object("newfile")
	// fmt.Printf("Bucket %v created.\n", bucketName)

	// BearerToken := os.Getenv("BEARER_TOKEN")

	// body, err := ioutil.ReadAll(resp.Body)

	// if err != nil {
	// 	log.Fatal("HTTP failed")
	// }
	// fmt.Printf("%s", body)
}
