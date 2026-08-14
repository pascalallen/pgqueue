package pgqueue_test

import (
	"testing"
	"time"

	"github.com/pascalallen/pgqueue"
	"github.com/stretchr/testify/assert"
)

// backoffBase mirrors the deterministic part of DefaultBackoff: one second
// doubled per attempt with the shift clamped at 8 (256s), which stays under
// the 5m cap, so the cap branch never engages. Jitter adds up to 25% on top.
func backoffBase(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 8 {
		attempt = 8
	}

	return time.Second << uint(attempt)
}

func TestDefaultBackoff(t *testing.T) {
	t.Run("each attempt stays within its base plus twenty-five percent jitter", func(t *testing.T) {
		for attempt := 1; attempt <= 12; attempt++ {
			base := backoffBase(attempt)
			for i := 0; i < 25; i++ {
				got := pgqueue.DefaultBackoff(attempt)
				assert.GreaterOrEqual(t, got, base, "attempt %d", attempt)
				assert.LessOrEqual(t, got, base+base/4, "attempt %d", attempt)
			}
		}
	})

	t.Run("large attempts are clamped to the attempt-eight base of 256 seconds", func(t *testing.T) {
		for _, attempt := range []int{9, 50, 100, 1 << 20} {
			for i := 0; i < 25; i++ {
				got := pgqueue.DefaultBackoff(attempt)
				assert.GreaterOrEqual(t, got, 256*time.Second, "attempt %d", attempt)
				assert.LessOrEqual(t, got, 320*time.Second, "attempt %d", attempt)
			}
		}
	})

	t.Run("non-positive attempts are treated as attempt one", func(t *testing.T) {
		for _, attempt := range []int{0, -1, -100} {
			for i := 0; i < 25; i++ {
				got := pgqueue.DefaultBackoff(attempt)
				assert.GreaterOrEqual(t, got, 2*time.Second, "attempt %d", attempt)
				assert.LessOrEqual(t, got, 2500*time.Millisecond, "attempt %d", attempt)
			}
		}
	})
}
