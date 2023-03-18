package pubsubjobs

import (
	"context"

	"cloud.google.com/go/pubsub"
)

func (d *Driver) listener() {
	s, err := d.client.Topic("").Subscriptions().Next()
	s.Receive(context.Background(), func(ctx context.Context, message *pubsub.Message) {
	})
}
