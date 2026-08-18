package pubsubjobs

import (
	"testing"

	pubsubpb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/stretchr/testify/require"
)

func TestConfigRequiresProjectID(t *testing.T) {
	c := &config{Topic: "topic"}

	require.EqualError(t, c.InitDefaults(), "project_id is required")
}

func TestConfigRequiresTopic(t *testing.T) {
	c := &config{ProjectID: "test"}

	require.EqualError(t, c.InitDefaults(), "topic is required")
}

// TestConfigDefaults covers the values filled in when the pipeline leaves them
// out. max_delivery_attempts only gets a default once a dead letter topic is
// set, since the subscription has no dead letter policy without one.
func TestConfigDefaults(t *testing.T) {
	c := &config{ProjectID: "test", Topic: "topic"}

	require.NoError(t, c.InitDefaults())
	require.Equal(t, 10, c.Priority)
	require.Equal(t, "127.0.0.1:8085", c.Endpoint)
	require.Zero(t, c.MaxDeliveryAttempts)
}

func TestConfigDeadLetterTopicDefaultsAttempts(t *testing.T) {
	c := &config{ProjectID: "test", Topic: "topic", DeadLetterTopic: "dlt"}

	require.NoError(t, c.InitDefaults())
	require.Equal(t, 10, c.MaxDeliveryAttempts)
}

func TestConfigKeepsExplicitValues(t *testing.T) {
	c := &config{
		ProjectID:           "test",
		Topic:               "topic",
		DeadLetterTopic:     "dlt",
		Endpoint:            "127.0.0.1:9999",
		Priority:            3,
		MaxDeliveryAttempts: 5,
	}

	require.NoError(t, c.InitDefaults())
	require.Equal(t, "127.0.0.1:9999", c.Endpoint)
	require.Equal(t, 3, c.Priority)
	require.Equal(t, 5, c.MaxDeliveryAttempts)
}

// TestInitOrNil covers the dead letter policy builder. A nil or unnamed topic
// has to yield a nil policy, since the subscription rejects an empty one.
func TestInitOrNil(t *testing.T) {
	require.Nil(t, initOrNil(nil, 10))
	require.Nil(t, initOrNil(&pubsubpb.Topic{}, 10))

	policy := initOrNil(&pubsubpb.Topic{Name: "projects/test/topics/dlt"}, 7)

	require.NotNil(t, policy)
	require.Equal(t, "projects/test/topics/dlt", policy.GetDeadLetterTopic())
	require.Equal(t, int32(7), policy.GetMaxDeliveryAttempts())
}
