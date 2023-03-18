package pubsubjobs

import (
	"context"
	"sync"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"github.com/roadrunner-server/api/v4/plugins/v1/jobs"
	pq "github.com/roadrunner-server/api/v4/plugins/v1/priority_queue"
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
	mu         sync.Mutex
	log        *zap.Logger
	pq         pq.Queue
	pipeline   atomic.Pointer[jobs.Pipeline]
	tracer     *sdktrace.TracerProvider
	prop       propagation.TextMapPropagator
	consumeAll bool

	client *pubsub.Client
}

// FromConfig initializes rabbitmq pipeline
func FromConfig(tracer *sdktrace.TracerProvider, configKey string, log *zap.Logger, cfg Configurer, pipeline jobs.Pipeline, pq pq.Queue) (*Driver, error) {
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
		tracer: tracer,
		prop:   prop,
		log:    log,
		pq:     pq,
	}

	jb.client, err = pubsub.NewClient(context.Background(), "")
	if err != nil {
		return nil, err
	}

	jb.pipeline.Store(&pipeline)

	return jb, nil
}

// FromPipeline initializes consumer from pipeline
func FromPipeline(tracer *sdktrace.TracerProvider, pipeline jobs.Pipeline, log *zap.Logger, cfg Configurer, pq pq.Queue) (*Driver, error) {
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
		prop:   prop,
		tracer: tracer,
		log:    log,
		pq:     pq,
	}

	// register the pipeline
	jb.pipeline.Store(&pipeline)

	return jb, nil
}

func (d *Driver) Push(ctx context.Context, job jobs.Job) error {
	const op = errors.Op("rabbitmq_push")
	// check if the pipeline registered

	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_push")
	defer span.End()

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	//start := time.Now().UTC()
	const op = errors.Op("rabbit_run")

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_run")
	defer span.End()

	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
	const op = errors.Op("google_pub_sub_driver_state")
	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_state")
	defer span.End()

	return nil, nil
}

func (d *Driver) Pause(ctx context.Context, p string) error {
	//start := time.Now().UTC()
	pipe := *d.pipeline.Load()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_resume")
	defer span.End()

	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	return nil
}

func (d *Driver) Resume(ctx context.Context, p string) error {
	//start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_resume")
	defer span.End()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p {
		return errors.Errorf("no such pipeline: %s", pipe.Name())
	}

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	//start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_stop")
	defer span.End()

	return nil
}
