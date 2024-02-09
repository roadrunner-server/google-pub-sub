package pubsubjobs

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/roadrunner-server/api/v4/plugins/v3/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

const (
	pluginName string = "google-pub-sub"
	tracerName string = "jobs"
)

var _ jobs.Driver = (*Driver)(nil)

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshal it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Driver struct {
	mu   sync.Mutex
	cond sync.Cond

	log         *zap.Logger
	pq          jobs.Queue
	pipeline    atomic.Pointer[jobs.Pipeline]
	tracer      *sdktrace.TracerProvider
	prop        propagation.TextMapPropagator
	consumeAll  bool
	skipDeclare bool
	topic       string

	// if user invoke several resume operations
	listeners uint32

	// func to cancel listener
	cancel context.CancelFunc

	client *pubsub.Client

	stopped uint64
	pauseCh chan struct{}
}

// FromConfig initializes google_pub_sub_driver_ pipeline
func FromConfig(tracer *sdktrace.TracerProvider, configKey string, pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_google_pub_sub_consumer")

	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	if !cfg.Has(configKey) {
		return nil, errors.E(op, errors.Errorf("no configuration by provided key: %s", configKey))
	}

	// if no global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global google-pub-sub configuration, global configuration should contain google-pub-sub addrs"))
	}

	// PARSE CONFIGURATION START -------
	var conf config
	err := cfg.UnmarshalKey(configKey, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	err = cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}

	conf.InitDefault()
	// PARSE CONFIGURATION END -------

	jb := &Driver{
		tracer:      tracer,
		prop:        prop,
		log:         log,
		skipDeclare: conf.SkipTopicDeclaration,
		topic:       conf.Topic,
		pq:          pq,
		pauseCh:     make(chan struct{}, 1),
		cond:        sync.Cond{L: &sync.Mutex{}},
	}

	ctx := context.Background()
	jb.client, err = pubsub.NewClient(ctx, conf.ProjectID)
	if err != nil {
		return nil, err
	}

	err = jb.manageTopic(ctx)
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb.pipeline.Store(&pipe)
	time.Sleep(time.Second)

	return jb, nil
}

// FromPipeline initializes consumer from pipeline
func FromPipeline(tracer *sdktrace.TracerProvider, pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("new_google_pub_sub_consumer_from_pipeline")
	if tracer == nil {
		tracer = sdktrace.NewTracerProvider()
	}

	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}, jprop.Jaeger{})
	otel.SetTextMapPropagator(prop)

	// only global section
	if !cfg.Has(pluginName) {
		return nil, errors.E(op, errors.Str("no global google-pub-sub configuration, global configuration should contain google-pub-sub addrs"))
	}

	// PARSE CONFIGURATION -------
	var conf config
	err := cfg.UnmarshalKey(pluginName, &conf)
	if err != nil {
		return nil, errors.E(op, err)
	}
	conf.InitDefault()
	// PARSE CONFIGURATION -------

	jb := &Driver{
		prop:        prop,
		tracer:      tracer,
		log:         log,
		pq:          pq,
		pauseCh:     make(chan struct{}, 1),
		skipDeclare: conf.SkipTopicDeclaration,
		topic:       conf.Topic,
		cond:        sync.Cond{L: &sync.Mutex{}},
	}

	err = jb.manageTopic(context.Background())
	if err != nil {
		return nil, errors.E(op, err)
	}

	// register the pipeline
	jb.pipeline.Store(&pipe)
	time.Sleep(time.Second)

	return jb, nil
}

func (d *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("google_pub_sub_driver_push")
	// check if the pipeline registered

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_push")
	defer span.End()

	result := d.client.Topic(d.topic).Publish(ctx, &pubsub.Message{Data: jb.Payload()})
	id, err := result.Get(ctx)
	if err != nil {
		return err
	}

	d.log.Debug("Message published", zap.String("messageId", id))

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	start := time.Now().UTC()
	const op = errors.Op("google_pub_sub_driver_run")

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_run")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	atomic.AddUint32(&d.listeners, 1)

	// start listener
	var ctxCancel context.Context
	ctxCancel, d.cancel = context.WithCancel(context.Background())
	d.listen(ctxCancel)

	d.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	const op = errors.Op("google_pub_sub_driver_state")
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_state")
	defer span.End()

	return nil, nil
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_resume")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 0 {
		return errors.Str("no active listeners, nothing to pause")
	}

	atomic.AddUint32(&d.listeners, ^uint32(0))

	if d.cancel != nil {
		d.cancel()
	}

	// stop consume
	d.pauseCh <- struct{}{}
	// if blocked, let 1 item to pass to unblock the listener and close the pipe
	d.cond.Signal()

	d.log.Debug("pipeline was paused", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", time.Now().UTC()), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_resume")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	l := atomic.LoadUint32(&d.listeners)
	// no active listeners
	if l == 1 {
		return errors.Str("listener is already in the active state")
	}

	// start listener
	var ctxCancel context.Context
	ctxCancel, d.cancel = context.WithCancel(context.Background())
	d.listen(ctxCancel)

	// increase num of listeners
	atomic.AddUint32(&d.listeners, 1)
	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", time.Now().UTC()), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_stop")
	defer span.End()

	atomic.StoreUint64(&d.stopped, 1)
	pipe := *d.pipeline.Load()
	_ = d.pq.Remove(pipe.Name())

	if atomic.LoadUint32(&d.listeners) > 0 {
		if d.cancel != nil {
			d.cancel()
		}
		// if blocked, let 1 item to pass to unblock the listener and close the pipe
		d.cond.Signal()

		d.pauseCh <- struct{}{}
	}

	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", time.Now().UTC()), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (d *Driver) manageTopic(ctx context.Context) error {
	if d.skipDeclare {
		return nil
	}

	_, err := d.client.CreateTopic(ctx, d.topic)
	if err != nil {
		if strings.Contains(err.Error(), "Topic already exists") {
			return nil
		}
		return err
	}

	return nil
}
