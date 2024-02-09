package pubsubjobs

import "os"

// pipeline rabbitmq info
const (
	pref                 string = "prefetch"
	skipTopicDeclaration string = "skip_topic_declaration"
	topic                string = "topic"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	ProjectID            string `mapstructure:"project_id"`
	Topic                string `mapstructure:"topic"`
	SkipTopicDeclaration bool   `mapstructure:"skip_topic_declaration"`

	// local
	Prefetch int32  `mapstructure:"prefetch"`
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
