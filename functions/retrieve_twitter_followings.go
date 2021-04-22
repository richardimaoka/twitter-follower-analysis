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

	byte, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return byte, nil
}

func PublishTwitterFollowings(projectId string, byte []byte) error {
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	topic := client.Topic("twitter-followings")

	result := topic.Publish(ctx, &pubsub.Message{Data: byte})
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
	log.Printf("Received a pubsub message: m.Data = %v", twReq)

	//maybe not needed, and assume that the bucket is creaated beforehand?
	byte, err := callTwitterAPI(twReq.UserId, bearerToken)
	if err != nil {
		log.Fatalf("Failed to retrieve from twitter api %v\n", err)
	}
	log.Printf("Retreived a twitter responsee: %v", byte)

	err = PublishTwitterFollowings(projectId, byte)
	if err != nil {
		log.Fatalf("Failed to publish %v\n", err)
	}
	log.Printf("published json")

	return nil
}
