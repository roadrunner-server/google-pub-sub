package pubsubjobs

import (
	"context"
	"sync/atomic"

	"cloud.google.com/go/pubsub"
	"go.opentelemetry.io/otel/propagation"
	"go.uber.org/zap"
)

func (d *Driver) listen(ctx context.Context) {
	go func() {
		for {
			select {
			case <-d.stopCh:
				d.log.Debug("listener was stopped")
				return
			default:
				err := d.client.Subscription(d.sub).Receive(ctx, func(ctx context.Context, message *pubsub.Message) {
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
					// increase the current number of messages
					atomic.AddInt64(d.msgInFlight, 1)
					d.log.Debug("message pushed to the priority queue", zap.Int64("current", atomic.LoadInt64(d.msgInFlight)), zap.Int32("limit", atomic.LoadInt32(d.msgInFlightLimit)))

					span.End()
				})

				if err != nil {
					d.log.Error("subscribing error", zap.Error(err))
				}
			}
		}
	}()
}
