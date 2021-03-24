BQ users

SELECT * FROM users

pub userId
sub userId
  -> twitter api, save to bq
    -> pub next page
    -> sub next page
     -> twitter api, save to bq
     -> pub next page
     ...
  -> pub next user cursor

  
cloud watch to function successful
```go
// PubSubMessage is the payload of a Pub/Sub event. Please refer to the docs for
// additional information regarding Pub/Sub events.
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// HelloPubSub consumes a Pub/Sub message.
func HelloPubSub(ctx context.Context, m PubSubMessage) error {
	log.Println(string(m.Data))
	return nil
}
```
