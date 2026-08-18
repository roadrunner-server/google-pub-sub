package pubsubjobs

import (
	"testing"

	"tests/helpers"

	googlePubSub "github.com/roadrunner-server/google-pub-sub/v6"
	"github.com/roadrunner-server/informer/v6"
	"github.com/roadrunner-server/jobs/v6"
	"github.com/roadrunner-server/resetter/v6"
	rpcPlugin "github.com/roadrunner-server/rpc/v6"
	"github.com/roadrunner-server/server/v6"
	"github.com/stretchr/testify/require"
)

const (
	rpcAddr = "127.0.0.1:6001"
	pqAddr  = "127.0.0.1:6601"
	// declared is the pipeline the declare configs create over rpc.
	declared = "test-3"
)

func jobsPlugins() []any {
	return []any{
		&server.Plugin{},
		&rpcPlugin.Plugin{},
		&jobs.Plugin{},
		&resetter.Plugin{},
		&informer.Plugin{},
		&googlePubSub.Plugin{},
	}
}

// boot starts the container with the observed logger and waits for the rpc
// listener, which is the readiness signal the fixed sleeps used to stand in for.
func boot(t *testing.T, cfgPath string, addr string) (*helpers.RR, func()) {
	t.Helper()

	return helpers.Start(t, cfgPath, jobsPlugins(),
		helpers.WithObservedLogger(),
		helpers.WithTCPProbe(addr),
	)
}

// TestBoots covers the config-declared pipelines. Both subscriptions are
// created up front, and only the pipeline carrying dead_letter_topic asks the
// emulator for a second topic.
func TestBoots(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-init.yaml", rpcAddr)

	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "created subscription, not listening", 2)
	rr.RequireLogCount(t, "created/used dead letter topic", 1)
}

// TestPushAndProcess follows two jobs from the rpc call to the worker ack.
func TestPushAndProcess(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-init.yaml", rpcAddr)

	helpers.PushToPipe("test-1", false, rpcAddr)(t)
	helpers.PushToPipe("test-2", false, rpcAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(rpcAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "job was pushed successfully", 2)
	rr.RequireLogCount(t, "job was processed successfully", 2)
}

// TestAutoAck checks the listener acknowledges the message itself, before the
// worker ever sees it, when the job carries the auto ack option.
func TestAutoAck(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-init.yaml", rpcAddr)

	helpers.PushToPipe("test-1", true, rpcAddr)(t)
	helpers.PushToPipe("test-2", true, rpcAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 2)

	helpers.DestroyPipelines(rpcAddr, "test-1", "test-2")(t)

	rr.RequireLogCount(t, "auto ack is turned on, message acknowledged", 2)
}

// TestDeclareAndConsume declares a pipeline over rpc, runs a job through it and
// pauses it again.
func TestDeclareAndConsume(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-declare.yaml", rpcAddr)

	helpers.DeclarePipe("rr1", rpcAddr, declared)(t)
	helpers.ResumePipes(rpcAddr, declared)(t)
	rr.WaitLog(t, "pipeline was resumed", 1)

	helpers.PushToPipe(declared, false, rpcAddr)(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(rpcAddr, declared)(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.DestroyPipelines(rpcAddr, declared)(t)

	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestDeclareRejectsIncompleteConfig covers the pipeline options the driver
// cannot fill in for the caller.
func TestDeclareRejectsIncompleteConfig(t *testing.T) {
	boot(t, "configs/.rr-declare.yaml", rpcAddr)

	for name, tc := range map[string]struct {
		pipeline map[string]string
		want     string
	}{
		"no project id": {
			pipeline: map[string]string{"driver": "google_pub_sub", "name": "no-project", "topic": "rr1"},
			want:     "project_id is required",
		},
		"no topic": {
			pipeline: map[string]string{"driver": "google_pub_sub", "name": "no-topic", "project_id": "test"},
			want:     "topic is required",
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorContains(t, helpers.Declare(t, rpcAddr, tc.pipeline), tc.want)
		})
	}
}

// TestRequeueRetriesUntilAck covers the worker that requeues a job with a
// growing attempts header and only acks on the fourth delivery. The old test
// slept out the three five second delays instead of following the records.
func TestRequeueRetriesUntilAck(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-jobs-err.yaml", rpcAddr)

	helpers.DeclarePipe("rr2", rpcAddr, declared)(t)
	helpers.ResumePipes(rpcAddr, declared)(t)
	helpers.PushToPipe(declared, false, rpcAddr)(t)

	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.PausePipelines(rpcAddr, declared)(t)
	helpers.DestroyPipelines(rpcAddr, declared)(t)

	// one original delivery plus the three the worker requeued
	rr.RequireLogCount(t, "job processing was started", 4)
	rr.RequireLogCount(t, "job was pushed successfully", 1)
	rr.RequireLogCount(t, "job was processed successfully", 1)
	rr.RequireLogCount(t, "pipeline was paused", 1)
	rr.RequireLogCount(t, "pipeline was resumed", 1)
	rr.RequireLogCount(t, "pipeline was stopped", 1)
}

// TestDestroyDropsUnprocessedJobs pushes far more jobs than the two slow
// workers can finish and destroys both pipelines while they are still busy.
// Nothing may be reported as processed, and both listeners have to come down.
func TestDestroyDropsUnprocessedJobs(t *testing.T) {
	const rounds = 10

	rr, _ := boot(t, "configs/.rr-pq.yaml", pqAddr)

	for range rounds {
		helpers.PushToPipe("test-3", false, pqAddr)(t)
		helpers.PushToPipe("test-4", false, pqAddr)(t)
	}

	rr.RequireLogCount(t, "job was pushed successfully", 2*rounds)

	// both workers have to be busy before the destroy, otherwise the test would
	// pass without ever exercising the in-flight path
	rr.WaitLog(t, "job processing was started", 2)

	helpers.DestroyPipelines(pqAddr, "test-3", "test-4")(t)

	rr.RequireLogCount(t, "pipeline was started", 2)
	rr.RequireLogCount(t, "pipeline was stopped", 2)
	rr.RequireLogCount(t, "listener was stopped", 2)

	// the workers sleep far longer than the destroy takes, so no job can have
	// reached an ack
	require.Zero(t, rr.CountLog("job was processed successfully"))
}

// TestMalformedAttributesFallBack publishes straight to the topic with values
// RoadRunner would never write, so the listener has to fall back rather than
// drop the message.
func TestMalformedAttributesFallBack(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-init.yaml", rpcAddr)

	helpers.PublishRaw(t, "rrTopic1", []byte(`{"hello":"world"}`), map[string]string{
		"rr_id":       "raw-id",
		"rr_job":      "some/php/namespace",
		"rr_headers":  "not-json",
		"rr_delay":    "soon",
		"rr_priority": "high",
	})

	rr.WaitLog(t, "job was processed successfully", 1)

	rr.RequireLogCount(t, "failed to unpack the headers, not a JSON", 1)
	rr.RequireLogCount(t, "failed to unpack the delay, not a number", 1)
	rr.RequireLogCount(t, "failed to unpack the priority; inheriting the pipeline's default priority", 1)

	helpers.DestroyPipelines(rpcAddr, "test-1", "test-2")(t)
}

// TestPauseStopsConsuming checks a paused pipeline still accepts pushes but
// leaves them on the topic until it is resumed.
func TestPauseStopsConsuming(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-init.yaml", rpcAddr)

	helpers.PausePipelines(rpcAddr, "test-1")(t)
	rr.WaitLog(t, "pipeline was paused", 1)

	helpers.PushToPipe("test-1", false, rpcAddr)(t)
	rr.WaitLog(t, "job was pushed successfully", 1)

	rr.NeverLog(t, "job was processed successfully")

	helpers.ResumePipes(rpcAddr, "test-1")(t)
	rr.WaitLog(t, "job was processed successfully", 1)

	helpers.DestroyPipelines(rpcAddr, "test-1", "test-2")(t)
}

// TestStatsReportPipelineIdentity covers the state the driver reports. Pub/Sub
// keeps the counters on the Google side, so only the identity is filled in.
func TestStatsReportPipelineIdentity(t *testing.T) {
	rr, _ := boot(t, "configs/.rr-init.yaml", rpcAddr)

	stats := helpers.Stats(t, rpcAddr)
	require.Len(t, stats, 2)

	byPipeline := make(map[string]string, len(stats))
	for _, s := range stats {
		byPipeline[s.GetPipeline()] = s.GetQueue()
		require.Equal(t, "google_pub_sub", s.GetDriver())
		require.Zero(t, s.GetActive())
		require.Zero(t, s.GetDelayed())
		require.Zero(t, s.GetReserved())
	}

	require.Equal(t, map[string]string{"test-1": "rrTopic1", "test-2": "rrTopic2"}, byPipeline)

	rr.WaitLog(t, "State method is not implemented", 2)
}
