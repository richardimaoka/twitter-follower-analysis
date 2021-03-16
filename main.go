package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
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

func uploadFile(w io.Writer, bucket, object string) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage NewClient: %v", err)
	}
	defer client.Close()

	f, err := os.Open("notes.txt")
	if err != nil {
		return fmt.Errorf("os.Open: %v", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()

	wc := client.Bucket(bucket).Object(object).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("io.Copy: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Write.Close: %v", err)
	}
	fmt.Fprintf(w, "Blob %v uploaded.\n", object)
	return nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	fmt.Println("GAC = ", os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"))
	ctx := context.Background()
	// Sets your Google Cloud Platform project ID.
	projectID := "richard-twitter-extraction"

	// Creates a client.
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	bucketName := "my-new-buckettttttttttt-francepan"

	// Creates a Bucket instance.
	bucket := client.Bucket(bucketName)

	// Creates the new bucket.
	ctx, cancel := context.WithTimeout(ctx, time.Second*20)
	// defer cancel()
	// if err := bucket.Create(ctx, projectID, nil); err != nil {
	// 	log.Fatalf("Failed to create bucket: %v", err)
	// }

	bucket.Object("newfile")
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
