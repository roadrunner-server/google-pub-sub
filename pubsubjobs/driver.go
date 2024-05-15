package pubsubjobs

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/goccy/go-json"
	"github.com/roadrunner-server/api/v4/plugins/v3/jobs"
	"github.com/roadrunner-server/errors"
	jprop "go.opentelemetry.io/contrib/propagators/jaeger"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	pluginName string = "google_pub_sub"
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
	mu sync.Mutex

	log         *zap.Logger
	pq          jobs.Queue
	pipeline    atomic.Pointer[jobs.Pipeline]
	tracer      *sdktrace.TracerProvider
	prop        propagation.TextMapPropagator
	skipDeclare bool
	topic       string
	sub         string
	// pubsub specific
	gsub    *pubsub.Subscription
	gtopic  *pubsub.Topic
	gclient *pubsub.Client

	// context cancel func used to cancel the pubsub subscription
	receiveCtxCancel context.CancelFunc
	rctx             context.Context

	// if user invoke several resume operations
	listeners uint32
	stopped   uint64
}

// FromConfig initializes google_pub_sub_driver_ pipeline
func FromConfig(tracer *sdktrace.TracerProvider, configKey string, pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("google_pub_sub_consumer")

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

	err = conf.InitDefaults()
	if err != nil {
		return nil, errors.E(op, err)
	}
	// PARSE CONFIGURATION END -------

	var opts []option.ClientOption
	if conf.Insecure {
		opts = append(opts, option.WithoutAuthentication())
		opts = append(opts, option.WithTelemetryDisabled())
		opts = append(opts, option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}

	opts = append(opts, option.WithEndpoint(conf.Endpoint))

	gclient, err := pubsub.NewClient(context.Background(), conf.ProjectID, opts...)
	if err != nil {
		return nil, err
	}

	jb := &Driver{
		tracer:      tracer,
		prop:        prop,
		log:         log,
		skipDeclare: conf.SkipTopicDeclaration,
		topic:       conf.Topic,
		pq:          pq,
		sub:         pipe.Name(),
		gclient:     gclient,
	}

	err = jb.manageTopic()
	if err != nil {
		return nil, errors.E(op, err)
	}

	jb.pipeline.Store(&pipe)

	return jb, nil
}

// FromPipeline initializes consumer from pipeline
func FromPipeline(tracer *sdktrace.TracerProvider, pipe jobs.Pipeline, log *zap.Logger, cfg Configurer, pq jobs.Queue) (*Driver, error) {
	const op = errors.Op("google_pub_sub_consumer_from_pipeline")
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

	conf.ProjectID = pipe.String(projectIDKey, "")
	conf.Topic = pipe.String(topicKey, "")
	conf.SkipTopicDeclaration = pipe.Bool(skipTopicKey, false)

	err = conf.InitDefaults()
	if err != nil {
		return nil, err
	}

	var opts []option.ClientOption
	if conf.Insecure {
		opts = append(opts, option.WithoutAuthentication())
		opts = append(opts, option.WithTelemetryDisabled())
		opts = append(opts, option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	}

	opts = append(opts, option.WithEndpoint(conf.Endpoint))

	// PARSE CONFIGURATION END -------
	gclient, err := pubsub.NewClient(context.Background(), conf.ProjectID, opts...)
	if err != nil {
		return nil, err
	}

	jb := &Driver{
		prop:        prop,
		tracer:      tracer,
		log:         log,
		pq:          pq,
		skipDeclare: conf.SkipTopicDeclaration,
		topic:       conf.Topic,
		sub:         pipe.Name(),
		gclient:     gclient,
	}

	err = jb.manageTopic()
	if err != nil {
		return nil, errors.E(op, err)
	}

	// register the pipeline
	jb.pipeline.Store(&pipe)

	return jb, nil
}

func (d *Driver) Push(ctx context.Context, jb jobs.Message) error {
	const op = errors.Op("google_pub_sub_push")
	// check if the pipeline registered
	ctx, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_push")
	defer span.End()

	// load atomic value
	pipe := *d.pipeline.Load()
	if pipe.Name() != jb.GroupID() {
		return errors.E(op, errors.Errorf("no such pipeline: %s, actual: %s", jb.GroupID(), pipe.Name()))
	}

	job := fromJob(jb)

	data, err := json.Marshal(job.Metadata)
	if err != nil {
		return err
	}

	result := d.gtopic.Publish(ctx, &pubsub.Message{
		Data: jb.Payload(),
		Attributes: map[string]string{
			jobs.RRID:       job.Ident,
			jobs.RRJob:      job.Job,
			jobs.RRDelay:    strconv.Itoa(int(job.Options.Delay)),
			jobs.RRHeaders:  string(data),
			jobs.RRPriority: strconv.Itoa(int(job.Options.Priority)),
			jobs.RRAutoAck:  btos(job.Options.AutoAck),
		},
		PublishTime: time.Now().UTC(),
	})

	id, err := result.Get(ctx)
	if err != nil {
		return err
	}

	d.log.Debug("Message published", zap.String("messageId", id))

	return nil
}

func (d *Driver) Run(ctx context.Context, p jobs.Pipeline) error {
	const op = errors.Op("google_pub_sub_driver_run")
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_run")
	defer span.End()

	d.mu.Lock()
	defer d.mu.Unlock()

	pipe := *d.pipeline.Load()
	if pipe.Name() != p.Name() {
		return errors.E(op, errors.Errorf("no such pipeline registered: %s", pipe.Name()))
	}

	atomic.AddUint32(&d.listeners, 1)

	d.listen()

	d.log.Debug("pipeline was started", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", start), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (d *Driver) State(ctx context.Context) (*jobs.State, error) {
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

	// stop the listener
	d.gtopic.Stop()

	atomic.AddUint32(&d.listeners, ^uint32(0))

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
	// we have an active listener
	if l == 1 {
		return errors.Str("listener is already in the active state")
	}

	d.listen()

	// increase num of listeners
	atomic.AddUint32(&d.listeners, 1)
	d.log.Debug("pipeline was resumed", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", time.Now().UTC()), zap.Duration("elapsed", time.Since(start)))

	return nil
}

func (d *Driver) Stop(ctx context.Context) error {
	start := time.Now().UTC()

	_, span := trace.SpanFromContext(ctx).TracerProvider().Tracer(tracerName).Start(ctx, "google_pub_sub_stop")
	defer span.End()

	pipe := *d.pipeline.Load()
	_ = d.pq.Remove(pipe.Name())

	d.checkCtxAndCancel()

	atomic.StoreUint64(&d.stopped, 1)
	d.gtopic.Stop()

	err := d.gclient.Close()
	if err != nil {
		d.log.Error("failed to close the client", zap.Error(err))
	}

	d.log.Debug("pipeline was stopped", zap.String("driver", pipe.Driver()), zap.String("pipeline", pipe.Name()), zap.Time("start", time.Now().UTC()), zap.Duration("elapsed", time.Since(start)))
	return nil
}

func (d *Driver) manageTopic() error {
	if d.skipDeclare {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	var err error
	d.gtopic, err = d.gclient.CreateTopic(ctx, d.topic)
	if err != nil {
		if !strings.Contains(err.Error(), "Topic already exists") {
			return err
		}

		// topic would be nil if it already exists
		d.gtopic = d.gclient.Topic(d.topic)
	}

	d.log.Debug("created topic", zap.String("topic", d.gtopic.String()))

	d.gsub, err = d.gclient.CreateSubscription(ctx, d.sub, pubsub.SubscriptionConfig{
		Topic:       d.gtopic,
		AckDeadline: 10 * time.Minute,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "Subscription already exists") {
			return err
		}
	}

	d.log.Debug("created subscription", zap.String("topic", d.topic), zap.String("subscription", d.sub))

	return nil
}
