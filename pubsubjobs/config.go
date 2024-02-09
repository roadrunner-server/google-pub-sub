package pubsubjobs

import "os"

// pipeline rabbitmq info
const (
	exchangeKey string = "exchange"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	ProjectID string `mapstructure:"project_id"`
	Topic string `mapstructure:"topic"`
	SkipTopicDeclaration bool `mapstructure:"skip_topic_declaration"`

	// local
	Prefetch int    `mapstructure:"prefetch"`
	Priority int64  `mapstructure:"priority"`
	Host     string `mapstructure:"host"`
}

func (c *config) InitDefault() {
	if c.Prefetch == 0 {
		c.Prefetch = 10
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.Host != "" {
		// No possibility to set up emulator from client init
		os.Setenv("PUBSUB_EMULATOR_HOST", c.Host)
	}
}
