package pgqueue_test

import (
	"context"
	"testing"
	"time"

	"github.com/pascalallen/pgqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPublishSubscribe_FansOutToAllSubscribers(t *testing.T) {
	db := testDB(t)
	dsn := testDSN(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	received := make([]chan []byte, 2)
	for i := range received {
		payloads := make(chan []byte, 1)
		received[i] = payloads

		sub := pgqueue.NewSubscriber(dsn, nil)
		sub.Handle("pgqueue_test_channel", func(payload []byte) {
			select {
			case payloads <- payload:
			default:
			}
		})
		go func() { _ = sub.Start(ctx) }()
	}

	// Publish repeatedly: LISTEN on the subscriber connections may not be
	// established yet, and NOTIFY is fire-and-forget.
	deadline := time.Now().Add(5 * time.Second)
	got := make([][]byte, 2)
	for time.Now().Before(deadline) {
		require.NoError(t, pgqueue.Publish(context.Background(), db, "pgqueue_test_channel", []byte(`{"group_id":"01ABC"}`)))

		pending := false
		for i := range received {
			if got[i] == nil {
				select {
				case got[i] = <-received[i]:
				case <-time.After(100 * time.Millisecond):
					pending = true
				}
			}
		}
		if !pending {
			break
		}
	}

	for i := range got {
		require.NotNil(t, got[i], "subscriber %d never received the notification", i)
		assert.JSONEq(t, `{"group_id":"01ABC"}`, string(got[i]))
	}
}

func TestSubscriber_StartHonorsContextWhileListenCannotConnect(t *testing.T) {
	sub := pgqueue.NewSubscriber("host=127.0.0.1 port=1 user=postgres sslmode=disable connect_timeout=1", nil)
	sub.Handle("pgqueue_test_channel", func(payload []byte) {})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- sub.Start(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after its context was canceled")
	}
}

func TestSubscriber_HandleAfterStartPanics(t *testing.T) {
	sub := pgqueue.NewSubscriber(testDSN(t), nil)
	sub.Handle("pgqueue_test_channel", func(payload []byte) {})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = sub.Start(ctx) }()
	time.Sleep(200 * time.Millisecond)

	assert.Panics(t, func() { sub.Handle("late", func(payload []byte) {}) })
}
