package googlepubsub

import (
	"github.com/roadrunner-server/api/v4/plugins/v3/jobs"
	"github.com/roadrunner-server/endure/v2/dep"
	"github.com/roadrunner-server/errors"
	"github.com/roadrunner-server/google-pub-sub/v4/pubsubjobs"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
)

const (
	pluginName       string = "google-pub-sub"
	masterPluginName string = "jobs"
)

type Plugin struct {
	log    *zap.Logger
	cfg    Configurer
	tracer *sdktrace.TracerProvider
}

type Configurer interface {
	// UnmarshalKey takes a single key and unmarshals it into a Struct.
	UnmarshalKey(name string, out any) error
	// Has checks if config section exists.
	Has(name string) bool
}

type Logger interface {
	NamedLogger(name string) *zap.Logger
}

type Tracer interface {
	Tracer() *sdktrace.TracerProvider
}

func (p *Plugin) Init(log Logger, cfg Configurer) error {
	if !cfg.Has(pluginName) && !cfg.Has(masterPluginName) {
		return errors.E(errors.Disabled)
	}

	p.log = log.NamedLogger(pluginName)
	p.cfg = cfg

	return nil
}

func (p *Plugin) Name() string {
	return pluginName
}

func (p *Plugin) Collects() []*dep.In {
	return []*dep.In{
		dep.Fits(func(pp any) {
			p.tracer = pp.(Tracer).Tracer()
		}, (*Tracer)(nil)),
	}
}

func (p *Plugin) DriverFromConfig(configKey string, pq jobs.Queue, pipeline jobs.Pipeline, _ chan<- jobs.Commander) (jobs.Driver, error) {
	return pubsubjobs.FromConfig(p.tracer, configKey, pipeline, p.log, p.cfg, pq)
}

func (p *Plugin) DriverFromPipeline(pipe jobs.Pipeline, pq jobs.Queue, _ chan<- jobs.Commander) (jobs.Driver, error) {
	return pubsubjobs.FromPipeline(p.tracer, pipe, p.log, p.cfg, pq)
}
