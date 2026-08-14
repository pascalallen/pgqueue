package pgqueue

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLogger is a distinct Logger implementation so tests can tell an
// explicitly supplied logger apart from the nopLogger default.
type testLogger struct{ nopLogger }

func TestNewWorker_Defaults(t *testing.T) {
	t.Run("a zero config is normalized to the documented defaults", func(t *testing.T) {
		w := NewWorker(nil, WorkerConfig{})
		require.NotNil(t, w)

		assert.Equal(t, "default", w.cfg.QueueName)
		assert.Equal(t, 2, w.cfg.Concurrency)
		assert.Equal(t, 5*time.Second, w.cfg.PollInterval)
		assert.Equal(t, 5*time.Minute, w.cfg.RescueAfter)
		assert.IsType(t, nopLogger{}, w.cfg.Logger)

		require.NotNil(t, w.cfg.Backoff)
		got := w.cfg.Backoff(1)
		assert.GreaterOrEqual(t, got, 2*time.Second, "default backoff behaves like DefaultBackoff")
		assert.LessOrEqual(t, got, 2500*time.Millisecond, "default backoff behaves like DefaultBackoff")
	})

	t.Run("explicit values are preserved", func(t *testing.T) {
		w := NewWorker(nil, WorkerConfig{
			QueueName:    "emails",
			Concurrency:  8,
			PollInterval: time.Second,
			ListenDSN:    "postgres://example.invalid/db",
			RescueAfter:  30 * time.Second,
			Backoff:      func(attempt int) time.Duration { return 42 * time.Millisecond },
			Logger:       testLogger{},
		})

		assert.Equal(t, "emails", w.cfg.QueueName)
		assert.Equal(t, 8, w.cfg.Concurrency)
		assert.Equal(t, time.Second, w.cfg.PollInterval)
		assert.Equal(t, "postgres://example.invalid/db", w.cfg.ListenDSN)
		assert.Equal(t, 30*time.Second, w.cfg.RescueAfter)
		assert.Equal(t, 42*time.Millisecond, w.cfg.Backoff(1))
		assert.IsType(t, testLogger{}, w.cfg.Logger)
	})

	t.Run("non-positive values fall back to the defaults", func(t *testing.T) {
		w := NewWorker(nil, WorkerConfig{
			Concurrency:  -1,
			PollInterval: -time.Second,
			RescueAfter:  -time.Minute,
		})

		assert.Equal(t, 2, w.cfg.Concurrency)
		assert.Equal(t, 5*time.Second, w.cfg.PollInterval)
		assert.Equal(t, 5*time.Minute, w.cfg.RescueAfter)
	})
}

func TestNew_Defaults(t *testing.T) {
	t.Run("a bare queue uses the default name, five max attempts, and a nop logger", func(t *testing.T) {
		q := New(nil)
		require.NotNil(t, q)

		assert.Equal(t, "default", q.name)
		assert.Equal(t, 5, q.maxAttempts)
		assert.IsType(t, nopLogger{}, q.logger)
	})

	t.Run("WithQueueName overrides the queue name", func(t *testing.T) {
		assert.Equal(t, "emails", New(nil, WithQueueName("emails")).name)
	})

	t.Run("WithQueueName ignores an empty name", func(t *testing.T) {
		assert.Equal(t, "default", New(nil, WithQueueName("")).name)
	})

	t.Run("WithDefaultMaxAttempts overrides the default", func(t *testing.T) {
		assert.Equal(t, 10, New(nil, WithDefaultMaxAttempts(10)).maxAttempts)
	})

	t.Run("WithDefaultMaxAttempts ignores zero and negative values", func(t *testing.T) {
		assert.Equal(t, 5, New(nil, WithDefaultMaxAttempts(0)).maxAttempts)
		assert.Equal(t, 5, New(nil, WithDefaultMaxAttempts(-3)).maxAttempts)
	})

	t.Run("WithLogger overrides the logger", func(t *testing.T) {
		assert.IsType(t, testLogger{}, New(nil, WithLogger(testLogger{})).logger)
	})

	t.Run("WithLogger ignores nil", func(t *testing.T) {
		assert.IsType(t, nopLogger{}, New(nil, WithLogger(nil)).logger)
	})
}
