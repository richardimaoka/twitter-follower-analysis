package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	ctx := context.Background()
	// Sets your Google Cloud Platform project ID.
	projectID := "YOUR_PROJECT_ID"

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	bucketName := "my-new-bucket"

	// Creates a Bucket instance.
	bucket := client.Bucket(bucketName)

	// Creates the new bucket.
	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()
	if err := bucket.Create(ctx, projectID, nil); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}

	fmt.Printf("Bucket %v created.\n", bucketName)

	BearerToken := os.Getenv("BEARER_TOKEN")

	UserId := "2875908842"
	UserFields := "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld"
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitter.com/2/users/%s/followers?max_results=1000&user.fields=%s", UserId, UserFields), nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", BearerToken)
	req.Header.Add("Authorization", "Bearer "+BearerToken)

	httpClient := &http.Client{}
	resp, err := httpClient.Do(req)

	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	if err != nil {
		log.Fatal("HTTP failed")
	}
	fmt.Printf("%s", body)
}
