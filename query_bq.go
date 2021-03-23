package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/joho/godotenv"
	"google.golang.org/api/iterator"
)

type User struct {
	UserId string `bigquery:"user_id"`
}

// queryBasic demonstrates issuing a query and reading results.
func queryBasic(w io.Writer, projectID string) error {
	fmt.Println("asd")
	// projectID := "my-project-id"
	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	q := client.Query(
		"SELECT user_id FROM `richard-twitter-extraction.twitter.user_ids`")
	// Location must match that of the dataset(s) referenced in the query.
	q.Location = "US"
	// Run the query and print results when the query job is completed.
	job, err := q.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	if err := status.Err(); err != nil {
		return err
	}
	it, err := job.Read(ctx)
	for {
		var row User
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}
		fmt.Fprintln(w, row)
	}
	return nil
}

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	err = queryBasic(os.Stdout, "richard-twitter-extraction")
	if err != nil {
		fmt.Printf("Error in queryBasic %v", err)
	}
}
