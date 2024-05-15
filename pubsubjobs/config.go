package pubsubjobs

import (
	"errors"
)

// pipeline rabbitmq info
const (
	skipTopicKey string = "skip_topic_declaration"
	projectIDKey string = "project_id"
	topicKey     string = "topic"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Endpoint string `mapstructure:"endpoint"`
	Insecure bool   `mapstructure:"insecure"`

	// local
	SkipTopicDeclaration bool   `mapstructure:"skip_topic_declaration"`
	ProjectID            string `mapstructure:"project_id"`
	Priority             int64  `mapstructure:"priority"`
	Topic                string `mapstructure:"topic"`
}

func (c *config) InitDefaults() error {
	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.ProjectID == "" {
		return errors.New("project_id is required")
	}

	if c.Topic == "" {
		return errors.New("topic is required")
	}

	if c.Endpoint == "" {
		c.Endpoint = "127.0.0.1:8085"
	}

	return nil
}
