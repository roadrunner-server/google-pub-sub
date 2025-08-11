package pubsubjobs

import (
	"context"
	"errors"
	"sync/atomic"

	"cloud.google.com/go/pubsub/v2"
	"github.com/roadrunner-server/events"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

const (
	restartStr string = "restart"
)

func (d *Driver) listen() {
	// context used to stop the listener
	d.atomicCtx()
	go func() {
		err := d.gclient.Subscriber(d.subStr).Receive(d.rctx, func(ctx context.Context, message *pubsub.Message) {
			if message == nil {
				d.log.Warn("received nil message, skipping processing the message")
				return
			}

			d.log.Debug("receive message", zap.Stringp("ID", &message.ID))

			if message.DeliveryAttempt != nil {
				d.log.Info("message delivery attempts", zap.Int("attempts", *message.DeliveryAttempt))
			}

			item := d.unpack(message)

			ctxspan, span := d.tracer.Tracer(tracerName).Start(d.prop.Extract(ctx, propagation.HeaderCarrier(item.headers)), "google_pub_sub_listener")
			if item.Options.AutoAck {
				message.Ack()
				d.log.Debug("auto ack is turned on, message acknowledged")
			}

			if item.headers == nil {
				item.headers = make(map[string][]string, 2)
			}

			d.prop.Inject(ctxspan, propagation.HeaderCarrier(item.headers))

			d.pq.Insert(item)
			d.log.Debug("message pushed to the priority queue", zap.Uint64("queue size", d.pq.Len()))

			span.End()
		})
		if err != nil {
			if errors.Is(err, context.Canceled) {
				atomic.StoreUint32(&d.listeners, 0)
				return
			}
			st := status.Convert(err)
			if st != nil && st.Message() == "grpc: the client connection is closing" {
				// reduce the number of listeners
				if atomic.LoadUint32(&d.listeners) > 0 {
					atomic.AddUint32(&d.listeners, ^uint32(0))
				}

				d.log.Debug("listener was stopped")
				return
			}

			atomic.StoreUint32(&d.listeners, 0)
			// the pipeline was stopped
			if atomic.LoadUint64(&d.stopped) == 1 {
				return
			}

			// recreate pipeline on fail
			pipe := (*d.pipeline.Load()).Name()
			d.eventsCh <- events.NewEvent(events.EventJOBSDriverCommand, pipe, restartStr)
			d.log.Error("subscribing error, restarting the pipeline", zap.Error(err), zap.String("pipeline", pipe))
		}
	}()
}

func (d *Driver) atomicCtx() {
	d.rctx, d.receiveCtxCancel = context.WithCancel(context.Background())
}

func (d *Driver) checkCtxAndCancel() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if atomic.LoadUint32(&d.listeners) == 0 {
		if d.receiveCtxCancel != nil {
			d.receiveCtxCancel()
		}
	}
}
