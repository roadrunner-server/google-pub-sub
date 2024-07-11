package pubsubjobs

import (
	"errors"
)

// pipeline rabbitmq info
const (
	projectIDKey        string = "project_id"
	deadLetterTopic     string = "dead_letter_topic"
	topicKey            string = "topic"
	maxDeliveryAttempts string = "max_delivery_attempts"
	priorityKey         string = "priority"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	Endpoint string `mapstructure:"endpoint"`
	Insecure bool   `mapstructure:"insecure"`

	// local
	ProjectID           string `mapstructure:"project_id"`
	DeadLetterTopic     string `mapstructure:"dead_letter_topic"`
	Topic               string `mapstructure:"topic"`
	MaxDeliveryAttempts int    `mapstructure:"max_delivery_attempts"`
	Priority            int    `mapstructure:"priority"`
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

	if c.DeadLetterTopic != "" {
		if c.MaxDeliveryAttempts == 0 {
			c.MaxDeliveryAttempts = 10
		}
	}

	return nil
}
