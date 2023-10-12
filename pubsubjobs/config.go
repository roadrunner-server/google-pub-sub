package pubsubjobs

// pipeline rabbitmq info
const (
	exchangeKey string = "exchange"
)

// config is used to parse pipeline configuration
type config struct {
	// global
	ProjectID string `mapstructure:"project_id"`

	// local
	Prefetch int    `mapstructure:"prefetch"`
	Queue    string `mapstructure:"queue"`
	Priority int64  `mapstructure:"priority"`
}

func (c *config) InitDefault() {
	if c.Prefetch == 0 {
		c.Prefetch = 10
	}

	if c.Priority == 0 {
		c.Priority = 10
	}

	if c.Addr == "" {
		c.Addr = "amqp://guest:guest@127.0.0.1:5672/"
	}
}
