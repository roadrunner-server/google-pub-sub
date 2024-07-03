package pubsubjobs

import (
	"context"
	stderr "errors"
	"fmt"
	"maps"
	"strconv"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/goccy/go-json"
	"go.uber.org/zap"

	"github.com/roadrunner-server/api/v4/plugins/v4/jobs"
	"github.com/roadrunner-server/errors"
)

var _ jobs.Job = (*Item)(nil)

type Item struct {
	// Job contains the pluginName of job broker (usually PHP class).
	Job string `json:"job"`
	// Ident is a unique identifier of the job, should be provided from outside
	Ident string `json:"id"`
	// Payload is string data (usually JSON) passed to Job broker.
	Payload []byte `json:"payload"`
	// Headers with key-values pairs
	headers map[string][]string
	// Options contain a set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle a given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as a priority not set
	Priority int64 `json:"priority"`
	// Pipeline manually specified pipeline.
	Pipeline string `json:"pipeline,omitempty"`
	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int64 `json:"delay,omitempty"`
	// AutoAck option
	AutoAck bool `json:"auto_ack"`
	// AMQP Queue
	Queue string `json:"queue,omitempty"`
	// Private ================
	requeueFn func(ctx context.Context, job *Item) error
	message   *pubsub.Message
	stopped   *uint64
}

// DelayDuration returns delay duration in the form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

func (i *Item) ID() string {
	return i.Ident
}

func (i *Item) Priority() int64 {
	return i.Options.Priority
}

func (i *Item) GroupID() string {
	return i.Options.Pipeline
}

// Body packs job payload into binary payload.
func (i *Item) Body() []byte {
	return i.Payload
}

func (i *Item) Headers() map[string][]string {
	return i.headers
}

// Context packs job context (job, id) into binary payload.
// Not used in the amqp, amqp.Table used instead
func (i *Item) Context() ([]byte, error) {
	ctx, err := json.Marshal(
		struct {
			ID       string              `json:"id"`
			Job      string              `json:"job"`
			Driver   string              `json:"driver"`
			Queue    string              `json:"queue"`
			Headers  map[string][]string `json:"headers"`
			Pipeline string              `json:"pipeline"`
		}{
			ID:       i.Ident,
			Job:      i.Job,
			Driver:   pluginName,
			Headers:  i.headers,
			Queue:    i.Options.Queue,
			Pipeline: i.Options.Pipeline,
		},
	)

	if err != nil {
		return nil, err
	}

	return ctx, nil
}

func (i *Item) Ack() error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}
	// return in case of auto-ack
	if i.Options.AutoAck {
		return nil
	}

	i.Options.message.Ack()
	return nil
}

func (i *Item) Nack() error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}

	// message already deleted
	if i.Options.AutoAck {
		return nil
	}

	i.Options.message.Nack()

	return nil
}

func (i *Item) NackWithOptions(requeue bool, _ int) error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}

	// message already deleted
	if i.Options.AutoAck {
		return nil
	}

	if requeue {
		err := i.Requeue(nil, 0)
		if err != nil {
			return err
		}

		// ack the previous message
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		ar, err := i.Options.message.AckWithResult().Get(ctx)
		cancel()
		if err != nil {
			return handleResult(err, ar)
		}

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	nr, err := i.Options.message.NackWithResult().Get(ctx)
	cancel()
	if err != nil {
		return handleResult(err, nr)
	}

	return nil
}

// Requeue is a non-native method, it's used to requeue the message back to the queue but with new headers
func (i *Item) Requeue(headers map[string][]string, _ int) error {
	if atomic.LoadUint64(i.Options.stopped) == 1 {
		return errors.Str("failed to acknowledge the JOB, the pipeline is probably stopped")
	}

	// message already deleted
	if i.Options.AutoAck {
		return nil
	}

	if i.headers == nil {
		i.headers = make(map[string][]string, 2)
	}

	if len(headers) > 0 {
		maps.Copy(i.headers, headers)
	}

	// requeue the message
	err := i.Options.requeueFn(context.Background(), i)
	if err != nil {
		// Nack on fail
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		nr, err2 := i.Options.message.NackWithResult().Get(ctx)
		cancel()
		return handleResult(stderr.Join(err, err2), nr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	ar, err := i.Options.message.AckWithResult().Get(ctx)
	cancel()
	if err != nil {
		return handleResult(err, ar)
	}

	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: job.Payload(),
		headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),
		},
	}
}

func (d *Driver) unpack(message *pubsub.Message) *Item {
	attributes := message.Attributes

	var rrid string
	if val, ok := attributes[jobs.RRID]; ok {
		rrid = val
	}

	var rrj string
	if val, ok := attributes[jobs.RRJob]; ok {
		rrj = val
	}

	h := make(map[string][]string)
	if val, ok := attributes[jobs.RRHeaders]; ok {
		err := json.Unmarshal([]byte(val), &h)
		if err != nil {
			d.log.Debug("failed to unpack the headers, not a JSON", zap.Error(err))
		}
	}

	var autoAck bool
	if val, ok := attributes[jobs.RRAutoAck]; ok {
		autoAck = stob(val)
	}

	var dl int
	var err error
	if val, ok := attributes[jobs.RRDelay]; ok {
		dl, err = strconv.Atoi(val)
		if err != nil {
			d.log.Debug("failed to unpack the delay, not a number", zap.Error(err))
		}
	}

	var priority int
	if val, ok := attributes[jobs.RRPriority]; ok {
		priority, err = strconv.Atoi(val)
		if err != nil {
			priority = int((*d.pipeline.Load()).Priority())
			d.log.Debug("failed to unpack the priority; inheriting the pipeline's default priority", zap.Error(err))
		}
	}

	return &Item{
		Job:     rrj,
		Ident:   rrid,
		Payload: message.Data,
		headers: h,
		Options: &Options{
			AutoAck:  autoAck,
			Delay:    int64(dl),
			Priority: int64(priority),
			Pipeline: (*d.pipeline.Load()).Name(),
			// private
			message:   message,
			stopped:   &d.stopped,
			requeueFn: d.handlePush,
		},
	}
}

func btos(b bool) string {
	if b {
		return "true"
	}

	return "false"
}

func stob(s string) bool {
	if s != "" {
		return s == "true"
	}

	return false
}

func handleResult(err error, ar pubsub.AcknowledgeStatus) error {
	switch ar {
	case pubsub.AcknowledgeStatusSuccess:
		// no error
		return nil
	case pubsub.AcknowledgeStatusPermissionDenied:
		return fmt.Errorf("acknowledge status: PermissionDenied, err: %w", err)
	case pubsub.AcknowledgeStatusFailedPrecondition:
		return fmt.Errorf("acknowledge status: FailedPrecondition, err: %w", err)
	case pubsub.AcknowledgeStatusInvalidAckID:
		return fmt.Errorf("acknowledge status: InvalidAckID, err: %w", err)
	case pubsub.AcknowledgeStatusOther:
		return fmt.Errorf("acknowledge status: Other, err: %w", err)
	default:
		return err
	}
}
