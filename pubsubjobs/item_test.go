package pubsubjobs

import (
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/roadrunner-server/api-plugins/v6/jobs"
	"github.com/stretchr/testify/require"
)

// testMessage is the jobs.Message the jobs plugin hands to Push.
type testMessage struct {
	name     string
	id       string
	payload  []byte
	headers  map[string][]string
	priority int64
	groupID  string
	delay    int64
	autoAck  bool
}

func (m *testMessage) ID() string                   { return m.id }
func (m *testMessage) GroupID() string              { return m.groupID }
func (m *testMessage) Priority() int64              { return m.priority }
func (m *testMessage) Name() string                 { return m.name }
func (m *testMessage) Payload() []byte              { return m.payload }
func (m *testMessage) Delay() int64                 { return m.delay }
func (m *testMessage) AutoAck() bool                { return m.autoAck }
func (m *testMessage) Headers() map[string][]string { return m.headers }
func (m *testMessage) UpdatePriority(p int64)       { m.priority = p }
func (*testMessage) Offset() int64                  { return 0 }
func (*testMessage) Partition() int32               { return 0 }
func (*testMessage) Topic() string                  { return "" }
func (*testMessage) Metadata() string               { return "" }

var _ jobs.Message = (*testMessage)(nil)

func TestFromJob(t *testing.T) {
	msg := &testMessage{
		name:     "some/php/namespace",
		id:       "job-id",
		payload:  []byte(`{"hello":"world"}`),
		headers:  map[string][]string{"test": {"test2"}},
		priority: 3,
		groupID:  "test-1",
		delay:    5,
		autoAck:  true,
	}

	item := fromJob(msg)

	require.Equal(t, "job-id", item.ID())
	require.Equal(t, "test-1", item.GroupID())
	require.Equal(t, int64(3), item.Priority())
	require.Equal(t, []byte(`{"hello":"world"}`), item.Body())
	require.Equal(t, map[string][]string{"test": {"test2"}}, item.Headers())
	require.Equal(t, int64(5), item.Options.Delay)
	require.True(t, item.Options.AutoAck)
}

func TestDelayDuration(t *testing.T) {
	require.Equal(t, 5*time.Second, (&Options{Delay: 5}).DelayDuration())
	require.Zero(t, (&Options{}).DelayDuration())
}

// TestItemContext covers the metadata handed to the worker. The driver name is
// baked in, so a rename here has to stay in step with the PHP side.
func TestItemContext(t *testing.T) {
	item := &Item{
		Job:     "some/php/namespace",
		Ident:   "job-id",
		headers: map[string][]string{"test": {"test2"}},
		Options: &Options{Pipeline: "test-1", Queue: "rrTopic1"},
	}

	data, err := item.Context()
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, "job-id", got["id"])
	require.Equal(t, "some/php/namespace", got["job"])
	require.Equal(t, "google_pub_sub", got["driver"])
	require.Equal(t, "rrTopic1", got["queue"])
	require.Equal(t, "test-1", got["pipeline"])
}

func TestItemRespondIsNoop(t *testing.T) {
	require.NoError(t, (&Item{}).Respond(nil, ""))
}

// newStoppedItem returns an item whose pipeline has already been stopped.
func newStoppedItem() *Item {
	stopped := &atomic.Uint64{}
	stopped.Store(1)

	return &Item{Options: &Options{stopped: stopped}}
}

// TestStoppedPipelineRejectsAck covers the guard that keeps a late worker reply
// from touching a subscription the driver has already closed.
func TestStoppedPipelineRejectsAck(t *testing.T) {
	const want = "failed to acknowledge the JOB, the pipeline is probably stopped"

	require.EqualError(t, newStoppedItem().Ack(), want)
	require.EqualError(t, newStoppedItem().Nack(), want)
	require.EqualError(t, newStoppedItem().NackWithOptions(true, 0), want)
	require.EqualError(t, newStoppedItem().Requeue(nil, 0), want)
}

// newAutoAckItem returns a running item the listener has already acknowledged.
func newAutoAckItem() *Item {
	return &Item{Options: &Options{AutoAck: true, stopped: &atomic.Uint64{}}}
}

// TestAutoAckItemSkipsBroker checks the worker reply is a no-op once the
// listener acknowledged the message, so none of these reach the nil message.
func TestAutoAckItemSkipsBroker(t *testing.T) {
	require.NoError(t, newAutoAckItem().Ack())
	require.NoError(t, newAutoAckItem().Nack())
	require.NoError(t, newAutoAckItem().NackWithOptions(true, 0))
	require.NoError(t, newAutoAckItem().Requeue(map[string][]string{"a": {"b"}}, 0))
}

func TestHandleResult(t *testing.T) {
	cause := errors.New("boom")

	require.NoError(t, handleResult(cause, pubsub.AcknowledgeStatusSuccess))

	for _, tc := range []struct {
		status pubsub.AcknowledgeStatus
		want   string
	}{
		{pubsub.AcknowledgeStatusPermissionDenied, "acknowledge status: PermissionDenied, err: boom"},
		{pubsub.AcknowledgeStatusFailedPrecondition, "acknowledge status: FailedPrecondition, err: boom"},
		{pubsub.AcknowledgeStatusInvalidAckID, "acknowledge status: InvalidAckID, err: boom"},
		{pubsub.AcknowledgeStatusOther, "acknowledge status: Other, err: boom"},
	} {
		err := handleResult(cause, tc.status)

		require.EqualError(t, err, tc.want)
		require.ErrorIs(t, err, cause)
	}
}
