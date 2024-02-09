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
			case <-d.pauseCh:
				d.log.Debug("listener was stopped")
				return
			default:
				s, err := d.client.Topic(d.topic).Subscriptions(ctx).Next()
				if err != nil {
					d.log.Error("subscription iteration", zap.Error(err))
					continue
				}
				
				s.Receive(context.Background(), func(ctx context.Context, message *pubsub.Message) {
					d.cond.L.Lock()
					// lock when we hit the limit
					for atomic.LoadInt64(d.msgInFlight) >= int64(atomic.LoadInt32(d.msgInFlightLimit)) {
						d.log.Debug("prefetch limit was reached, waiting for the jobs to be processed", zap.Int64("current", atomic.LoadInt64(d.msgInFlight)), zap.Int32("limit", atomic.LoadInt32(d.msgInFlightLimit)))
						d.cond.Wait()
					}

					d.log.Debug("receive message", zap.Stringp("ID", &message.ID))
					item := d.unpack(message)

					ctxspan, span := d.tracer.Tracer(tracerName).Start(d.prop.Extract(context.Background(), propagation.HeaderCarrier(item.headers)), "google_pub_sub_listener")
					if item.Options.AutoAck {
						item.Ack()
						d.log.Debug("auto ack is turned on, message acknowledged")
						span.End()
					}

					if item.headers == nil {
						item.headers = make(map[string][]string, 2)
					}

					d.prop.Inject(ctxspan, propagation.HeaderCarrier(item.headers))

					d.pq.Insert(item)
					// increase the current number of messages
					atomic.AddInt64(d.msgInFlight, 1)
					d.log.Debug("message pushed to the priority queue", zap.Int64("current", atomic.LoadInt64(d.msgInFlight)), zap.Int32("limit", atomic.LoadInt32(d.msgInFlightLimit)))
					d.cond.L.Unlock()
					span.End()
				})
			}
		}
	}()
}
