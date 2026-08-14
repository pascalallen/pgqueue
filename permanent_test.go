package pgqueue_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/pascalallen/pgqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type typedError struct{ code int }

func (e *typedError) Error() string { return fmt.Sprintf("typed error %d", e.code) }

func TestPermanent(t *testing.T) {
	t.Run("a wrapped error is reported as permanent", func(t *testing.T) {
		assert.True(t, pgqueue.IsPermanent(pgqueue.Permanent(errors.New("boom"))))
	})

	t.Run("a plain error is not permanent", func(t *testing.T) {
		assert.False(t, pgqueue.IsPermanent(errors.New("boom")))
	})

	t.Run("nil is not permanent", func(t *testing.T) {
		assert.False(t, pgqueue.IsPermanent(nil))
	})

	t.Run("wrapping nil returns nil", func(t *testing.T) {
		assert.NoError(t, pgqueue.Permanent(nil))
	})

	t.Run("errors.Is unwraps through the permanent wrapper", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		assert.ErrorIs(t, pgqueue.Permanent(sentinel), sentinel)
	})

	t.Run("errors.As unwraps through the permanent wrapper", func(t *testing.T) {
		var target *typedError
		require.ErrorAs(t, pgqueue.Permanent(&typedError{code: 42}), &target)
		assert.Equal(t, 42, target.code)
	})

	t.Run("a permanent error wrapped again in fmt.Errorf is still detected", func(t *testing.T) {
		sentinel := errors.New("sentinel")
		wrapped := fmt.Errorf("handler failed: %w", pgqueue.Permanent(sentinel))
		assert.True(t, pgqueue.IsPermanent(wrapped))
		assert.ErrorIs(t, wrapped, sentinel)
	})

	t.Run("the message prefixes the cause with permanent", func(t *testing.T) {
		assert.EqualError(t, pgqueue.Permanent(errors.New("boom")), "permanent: boom")
	})
}
