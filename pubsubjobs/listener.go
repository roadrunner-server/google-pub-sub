package pubsubjobs

import "context"

func (d *Driver) listen(ctx context.Context) {
	go func() {
		for {
			select {
			case <-d.pauseCh:
				d.log.Debug("listener was stopped")
				return
			default:
			}
		}
	}()
}
