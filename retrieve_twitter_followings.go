package p

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

func requestTwitter(userId, bearerToken, bucket, object string) ([]byte, error) {
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

	var data TwitterRequest
	err := json.Unmarshal([]byte(m.Data), &data)
	if err != nil {
		log.Fatalf("error unmarshaling json %v", m.Data)
	}

	bucket := "my-new-buckettttttttttt-francepddan"
	object := "twitttt.json"

	//maybe not needed, and assume that the bucket is creaated beforehand?
	if err := fetchAndSaveJson(user.UserId, bearerToken, bucket, object); err != nil {
		log.Fatalf("Failed to fetch and save json %v\n", err)
	}
	return nil
}
