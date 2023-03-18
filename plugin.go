package google_pub_sub

import (
	"context"

	"cloud.google.com/go/pubsub"
)

const pluginName string = "google-pub-sub"

type Plugin struct {
}

func (p *Plugin) Init() error {
	client, err := pubsub.NewClient(context.Background(), "")
	if err != nil {
		return err
	}

	return nil
}
