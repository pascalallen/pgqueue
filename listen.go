package pgqueue

import (
	"context"
	"fmt"

	"github.com/lib/pq"
)

// listen issues LISTEN for each channel while honoring ctx. pq.Listener.Listen
// blocks until a connection exists — indefinitely if Postgres is unreachable —
// so it runs on its own goroutine and ctx cancellation closes the listener,
// which unblocks it. On error the listener is closed and the caller must not
// use it.
func listen(ctx context.Context, listener *pq.Listener, channels ...string) error {
	errCh := make(chan error, 1)
	go func() {
		for _, channel := range channels {
			if err := listener.Listen(channel); err != nil {
				errCh <- fmt.Errorf("pgqueue: LISTEN %s: %w", channel, err)
				return
			}
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			listener.Close()
		}
		return err
	case <-ctx.Done():
		listener.Close()
		<-errCh
		return fmt.Errorf("pgqueue: LISTEN aborted before it connected: %w", ctx.Err())
	}
}
