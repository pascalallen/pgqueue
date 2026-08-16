package pgqueue

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
)

// Publish sends a fire-and-forget notification to every process currently
// LISTENing on channel. db may be a *sql.DB or, to defer delivery until
// commit, a *sql.Tx. Postgres caps NOTIFY payloads at ~8000 bytes.
func Publish(ctx context.Context, db DBTX, channel string, payload []byte) error {
	if _, err := db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, channel, string(payload)); err != nil {
		return fmt.Errorf("pgqueue: publish to %s: %w", channel, err)
	}

	return nil
}

// Subscriber delivers NOTIFY payloads to registered callbacks over a single
// dedicated LISTEN connection that reconnects automatically.
type Subscriber struct {
	dsn      string
	logger   Logger
	handlers map[string]func(payload []byte)
	started  atomic.Bool
}

// NewSubscriber takes a lib/pq connection string for the dedicated LISTEN
// connection.
func NewSubscriber(dsn string, logger Logger) *Subscriber {
	if logger == nil {
		logger = nopLogger{}
	}

	return &Subscriber{
		dsn:      dsn,
		logger:   logger,
		handlers: make(map[string]func(payload []byte)),
	}
}

// Handle registers a callback for one channel. It panics if called after
// Start. Callbacks run sequentially on the subscriber's goroutine — keep them
// fast or hand off internally.
func (s *Subscriber) Handle(channel string, fn func(payload []byte)) {
	if s.started.Load() {
		panic("pgqueue: Handle called after Start")
	}
	s.handlers[channel] = fn
}

// Start listens until ctx is canceled. Notifications that fire while the
// connection is re-establishing are lost — Subscriber is a real-time signal,
// not a durable stream.
func (s *Subscriber) Start(ctx context.Context) error {
	if !s.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	channels := make([]string, 0, len(s.handlers))
	for channel := range s.handlers {
		channels = append(channels, channel)
	}

	listener := pq.NewListener(s.dsn, time.Second, time.Minute, nil)
	if err := listen(ctx, listener, channels...); err != nil {
		return err
	}
	defer listener.Close()

	for {
		select {
		case <-ctx.Done():
			return nil
		case n, ok := <-listener.Notify:
			if !ok {
				return nil
			}
			if n == nil {
				// The connection was re-established; notifications may have
				// been missed in the gap.
				s.logger.Warn("pgqueue: subscriber reconnected; notifications may have been missed")
				continue
			}
			if fn, ok := s.handlers[n.Channel]; ok {
				fn([]byte(n.Extra))
			}
		}
	}
}
