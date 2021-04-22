package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
)

func callTwitterAPI(userId, bearerToken string) ([]byte, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("https://api.twitter.com/2/users/%s/following", userId), nil)
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

	byte, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return byte, nil
}

func ConstructSaveGcsMessage(userId string, bytes []byte) *pubsub.Message {
	attributes := map[string]string{
		"userId": userId,
	}
	return &pubsub.Message{Attributes: attributes, Data: bytes}

}

func PublishTwitterFollowings(projectId string, message *pubsub.Message) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	topic := client.Topic("twitter-followings")

	result := topic.Publish(ctx, message)
	_, err = result.Get(ctx)
	if err != nil {
		return err
	}
	return nil
}

func RetrieveTwitterFollowings(ctx context.Context, m pubsub.Message) error {
	projectId := os.Getenv("GCP_PROJECT")
	bearerToken := os.Getenv("BEARER_TOKEN")

	var twReq TwitterRequest
	err := json.Unmarshal([]byte(m.Data), &twReq)
	if err != nil {
		log.Fatalf("error unmarshaling json %v", m.Data)
	}
	log.Printf("Received a pubsub message 1: m.Data = %s", m.Data)
	log.Printf("Received a pubsub message 2: twReq = %+v", twReq)

	//maybe not needed, and assume that the bucket is creaated beforehand?
	byte, err := callTwitterAPI(twReq.UserId, bearerToken)
	if err != nil {
		log.Fatalf("Failed to retrieve from twitter api %v\n", err)
	}
	log.Printf("Retreived a twitter responsee: %s", byte)

	message := ConstructSaveGcsMessage(twReq.UserId, byte)
	log.Printf("message.Attributes = %+v", message.Attributes)
	log.Printf("message.Data = %s", message.Data)

	err = PublishTwitterFollowings(projectId, message)
	if err != nil {
		log.Fatalf("Failed to publish %v\n", err)
	}
	log.Printf("published json")

	return nil
}
