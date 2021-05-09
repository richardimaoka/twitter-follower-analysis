package functions

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
)

func saveOperation1(dirName, fileName string, jsonBytes []byte) error {
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}

	handle := client.Bucket("my-new-buckettttttttttt-francepddan").Object(dirName + "/" + fileName)
	writer := handle.NewWriter(ctx)
	defer writer.Close()

	saveOperation2(ctx, writer, jsonBytes)
	return nil
}

func saveOperation2(ctx context.Context, writer *storage.Writer, jsonBytes []byte) error {
	if _, err := fmt.Fprintf(writer, "%s", jsonBytes); err != nil {
		return err
	}
	return nil
}

func SaveIntoGCS(ctx context.Context, message pubsub.Message) error {
	projectId := os.Getenv("GCP_PROJECT")

	log.Printf("Received a pubsub message: m.Attributes = %s", message.Attributes)
	log.Printf("Received a pubsub message: m.Data = %s", message.Data)

	err := PublishTwitterFollowings(projectId, &message)
	if err != nil {
		log.Fatalf("Failed to publish %v\n", err)
	}
	log.Printf("published json")

	return nil
}
