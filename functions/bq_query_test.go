package functions

import (
	"context"
	"log"
	"testing"

	"cloud.google.com/go/bigquery"
	bqiface "github.com/googleapis/google-cloud-go-testing/bigquery/bqiface"
	"github.com/joho/godotenv"
)

func TestIntMinBasic(t *testing.T) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	projectId := "richard-twitter-extraction"
	ctx := context.Background()
	c, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		t.Fatal(err)
	}
	client := bqiface.AdaptClient(c)
	defer client.Close()

	userId, err := QueryIntoBq(ctx, client, 0)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("userId = %v", userId)

}
