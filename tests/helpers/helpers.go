package helpers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/rpc"
	"slices"
	"testing"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/google/uuid"
	jobsProto "github.com/roadrunner-server/api-go/v6/jobs/v1"
	goridgeRpc "github.com/roadrunner-server/goridge/v4/pkg/rpc"
	"github.com/stretchr/testify/require"
	otherit "google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// emulatorAddr is the address the compose file publishes the emulator on, and
// the endpoint every test config points the driver at.
const emulatorAddr = "127.0.0.1:8085"

// emulatorProject is the project the emulator is started with.
const emulatorProject = "test"

func NewJobsClient(t *testing.T, address string) *rpc.Client {
	t.Helper()

	conn, err := new(net.Dialer).DialContext(t.Context(), "tcp", address)
	require.NoError(t, err)

	client := rpc.NewClientWithCodec(goridgeRpc.NewClientCodec(conn))
	t.Cleanup(func() { _ = client.Close() })

	return client
}

func ResumePipes(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Resume",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Empty{}))
	}
}

func PausePipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Pause",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Empty{}))
	}
}

func DestroyPipelines(address string, pipes ...string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Destroy",
			&jobsProto.Pipelines{Pipelines: slices.Clone(pipes)},
			&jobsProto.Pipelines{}))
	}
}

func PushToPipe(pipeline string, autoAck bool, address string) func(t *testing.T) {
	return func(t *testing.T) {
		client := NewJobsClient(t, address)
		require.NoError(t, client.Call("jobs.Push",
			&jobsProto.PushRequest{Job: createDummyJob(pipeline, autoAck)},
			&jobsProto.Empty{}))
	}
}

func createDummyJob(pipeline string, autoAck bool) *jobsProto.Job {
	return &jobsProto.Job{
		Job:     "some/php/namespace",
		Id:      uuid.NewString(),
		Payload: []byte(`{"hello":"world"}`),
		Headers: map[string]*jobsProto.HeaderValue{"test": {Value: []string{"test2"}}},
		Options: &jobsProto.Options{
			AutoAck:  autoAck,
			Priority: 1,
			Pipeline: pipeline,
			Topic:    pipeline,
		},
	}
}

// DeclarePipe declares a pipeline over rpc and requires the call to succeed.
func DeclarePipe(topic string, address string, pipeline string) func(t *testing.T) {
	return func(t *testing.T) {
		require.NoError(t, Declare(t, address, map[string]string{
			"driver":            "google_pub_sub",
			"name":              pipeline,
			"priority":          "3",
			"topic":             topic,
			"dead_letter_topic": "dead_letter_topic",
			"project_id":        emulatorProject,
		}))
	}
}

// Declare issues a raw declare call and returns its error, so negative tests
// can assert on a rejected pipeline configuration.
func Declare(t *testing.T, address string, pipeline map[string]string) error {
	t.Helper()

	client := NewJobsClient(t, address)

	return client.Call("jobs.Declare",
		&jobsProto.DeclareRequest{Pipeline: pipeline},
		&jobsProto.Empty{})
}

// Stats returns the per-pipeline stats the jobs plugin reports.
func Stats(t *testing.T, address string) []*jobsProto.Stat {
	t.Helper()

	resp := &jobsProto.Stats{}
	require.NoError(t, NewJobsClient(t, address).Call("jobs.Stat", &jobsProto.Empty{}, resp))

	return resp.GetStats()
}

// newEmulatorClient dials the emulator directly, bypassing RoadRunner.
func newEmulatorClient(ctx context.Context) (*pubsub.Client, error) {
	return pubsub.NewClient(ctx, emulatorProject,
		option.WithoutAuthentication(),
		option.WithTelemetryDisabled(),
		option.WithEndpoint(emulatorAddr),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
}

// PublishRaw puts a message on the topic without going through the jobs plugin,
// so a test can hand the listener attributes RoadRunner would never produce.
func PublishRaw(t *testing.T, topic string, data []byte, attributes map[string]string) {
	t.Helper()

	client, err := newEmulatorClient(t.Context())
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.Publisher(topic).Publish(t.Context(), &pubsub.Message{
		Data:       data,
		Attributes: attributes,
	}).Get(t.Context())
	require.NoError(t, err)
}

// CleanEmulator drops every subscription and topic, so each test starts against
// an empty broker and cannot inherit messages from the previous one.
func CleanEmulator() error {
	ctx := context.Background()

	client, err := newEmulatorClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	project := fmt.Sprintf("projects/%s", client.Project())

	subiter := client.SubscriptionAdminClient.ListSubscriptions(ctx, &pubsubpb.ListSubscriptionsRequest{Project: project})
	for {
		sub, err := subiter.Next()
		if err != nil {
			if errors.Is(err, otherit.Done) {
				break
			}
			return err
		}

		if err := client.SubscriptionAdminClient.DeleteSubscription(ctx, &pubsubpb.DeleteSubscriptionRequest{Subscription: sub.GetName()}); err != nil {
			return err
		}
	}

	titer := client.TopicAdminClient.ListTopics(ctx, &pubsubpb.ListTopicsRequest{Project: project})
	for {
		topic, err := titer.Next()
		if err != nil {
			if errors.Is(err, otherit.Done) {
				break
			}
			return err
		}

		if err := client.TopicAdminClient.DeleteTopic(ctx, &pubsubpb.DeleteTopicRequest{Topic: topic.GetName()}); err != nil {
			return err
		}
	}

	return nil
}
