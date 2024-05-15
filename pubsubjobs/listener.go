package pubsubjobs

import (
	"context"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"
)

func (d *Driver) listen() {
	// context used to stop the listener
	d.atomicCtx()
	go func() {
		err := d.gsub.Receive(d.rctx, func(ctx context.Context, message *pubsub.Message) {
			d.log.Debug("receive message", zap.Stringp("ID", &message.ID))
			item := d.unpack(message)

			ctxspan, span := d.tracer.Tracer(tracerName).Start(d.prop.Extract(ctx, propagation.HeaderCarrier(item.Metadata)), "google_pub_sub_listener")
			if item.Options.AutoAck {
				message.Ack()
				d.log.Debug("auto ack is turned on, message acknowledged")
			}

			if item.Metadata == nil {
				item.Metadata = make(map[string][]string, 2)
			}

			d.prop.Inject(ctxspan, propagation.HeaderCarrier(item.Metadata))

			d.pq.Insert(item)
			d.log.Debug("message pushed to the priority queue", zap.Uint64("queue size", d.pq.Len()))

			span.End()
		})
		if err != nil {
			st := status.Convert(err)
			if st != nil && st.Message() == "grpc: the client connection is closing" {
				// reduce number of listeners
				if atomic.LoadUint32(&d.listeners) > 0 {
					atomic.AddUint32(&d.listeners, ^uint32(0))
				}

				d.log.Debug("listener was stopped")
				return
			}

			d.log.Error("subscribing error", zap.Error(err))
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
