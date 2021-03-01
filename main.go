package main

import (
	"bufio"
	"fmt"
	"net/http"
)

func main() {
	UserId := "aaa"
	resp, err := http.Get(fmt.Sprintf("https://api.twitter.com/2/users/%s/followers?max_results=1000&user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld", UserId))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	fmt.Println("Response status:", resp.Status)

	scanner := bufio.NewScanner(resp.Body)
	for i := 0; scanner.Scan() && i < 5; i++ {
		fmt.Println(scanner.Text())
	}
}
