package pubsubjobs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"cloud.google.com/go/pubsub"
	"github.com/goccy/go-json"
	"go.uber.org/zap"

	"github.com/roadrunner-server/api/v4/plugins/v3/jobs"
	"github.com/roadrunner-server/errors"
)

type Item struct {
	// Job contains pluginName of job broker (usually PHP class).
	Job string `json:"job"`
	// Ident is unique identifier of the job, should be provided from outside
	Ident string `json:"id"`
	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`
	// Headers with key-values pairs
	headers map[string][]string `json:"headers"`
	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Options carry information about how to handle given job.
type Options struct {
	// Priority is job priority, default - 10
	// pointer to distinguish 0 as a priority and nil as priority not set
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
	cond        *sync.Cond
	message     *pubsub.Message
	msgInFlight *int64
	stopped     *uint64
}

// DelayDuration returns delay duration in a form of time.Duration.
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
	return strToBytes(i.Payload)
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
	defer func() {
		i.Options.cond.Signal()
		atomic.AddInt64(i.Options.msgInFlight, ^int64(0))
	}()
	// just return in case of auto-ack
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
	defer func() {
		i.Options.cond.Signal()
		atomic.AddInt64(i.Options.msgInFlight, ^int64(0))
	}()
	
	// message already deleted
	if i.Options.AutoAck {
		return nil
	}

	i.Options.message.Nack()

	return nil
}

// Requeue with the provided delay, handled by the Nack
func (i *Item) Requeue(headers map[string][]string, delay int64) error {
	return nil
}

func (i *Item) Respond(_ []byte, _ string) error {
	return nil
}

func fromJob(job jobs.Message) *Item {
	return &Item{
		Job:     job.Name(),
		Ident:   job.ID(),
		Payload: string(job.Payload()),
		headers: job.Headers(),
		Options: &Options{
			Priority: job.Priority(),
			Pipeline: job.GroupID(),
			Delay:    job.Delay(),
			AutoAck:  job.AutoAck(),
		},
	}
}
func bytesToStr(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	return unsafe.String(unsafe.SliceData(data), len(data))
}

func strToBytes(data string) []byte {
	if data == "" {
		return nil
	}

	return unsafe.Slice(unsafe.StringData(data), len(data))
}

func (c *Driver) unpack(message *pubsub.Message) *Item {
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
			c.log.Debug("failed to unpack the headers, not a JSON", zap.Error(err))
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
			c.log.Debug("failed to unpack the delay, not a number", zap.Error(err))
		}
	}

	var priority int
	if val, ok := attributes[jobs.RRPriority]; ok {
		priority, err = strconv.Atoi(val)
		if err != nil {
			priority = int((*c.pipeline.Load()).Priority())
			c.log.Debug("failed to unpack the priority; inheriting the pipeline's default priority", zap.Error(err))
		}
	}

	return &Item{
		Job:     rrj,
		Ident:   rrid,
		Payload: string(message.Data),
		headers: h,
		Options: &Options{
			AutoAck:  autoAck,
			Delay:    int64(dl),
			Priority: int64(priority),
			Pipeline: (*c.pipeline.Load()).Name(),
			// private
			message:     message,
			msgInFlight: c.msgInFlight,
			cond:        &c.cond,
			stopped:     &c.stopped,
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
