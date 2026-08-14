package pgqueue_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/pascalallen/pgqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fastPoll is a test worker config: tight polling, instant retries.
func fastPoll() pgqueue.WorkerConfig {
	return pgqueue.WorkerConfig{
		PollInterval: 25 * time.Millisecond,
		Backoff:      func(attempt int) time.Duration { return 10 * time.Millisecond },
	}
}

func TestWorker_ProcessesJob(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	var mu sync.Mutex
	var got pgqueue.Job
	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		mu.Lock()
		defer mu.Unlock()
		got = job

		return nil
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", []byte(`{"user_id":"01ABC"}`))
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "job completed", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})
	row := getJob(t, db, id)
	assert.True(t, row.CompletedAt.Valid)
	assert.Equal(t, 1, row.Attempts)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, id, got.ID)
	assert.Equal(t, "SendWelcomeEmail", got.Type)
	assert.Equal(t, 1, got.Attempts)
	assert.JSONEq(t, `{"user_id":"01ABC"}`, string(got.Payload))
}

func TestWorker_DoesNotClaimFutureJobs(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error { return nil })
	startWorker(t, w)

	nowId, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)
	futureId, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil, pgqueue.WithRunAt(time.Now().Add(time.Hour)))
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "due job completed", func() bool {
		return getJob(t, db, nowId).Status == string(pgqueue.StatusCompleted)
	})
	assert.Equal(t, string(pgqueue.StatusPending), getJob(t, db, futureId).Status)
	assert.Equal(t, 0, getJob(t, db, futureId).Attempts)
}

func TestWorker_ConcurrentWorkersProcessEachJobExactlyOnce(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)
	const jobCount = 20

	var mu sync.Mutex
	seen := make(map[int64]int)
	handler := func(ctx context.Context, job pgqueue.Job) error {
		mu.Lock()
		seen[job.ID]++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond) // hold the slot so claims overlap

		return nil
	}

	for range 2 {
		cfg := fastPoll()
		cfg.Concurrency = 4
		w := pgqueue.NewWorker(db, cfg)
		w.Register("SendWelcomeEmail", handler)
		startWorker(t, w)
	}

	ids := make([]int64, 0, jobCount)
	for range jobCount {
		id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
		require.NoError(t, err)
		ids = append(ids, id)
	}

	waitFor(t, 10*time.Second, "all jobs completed", func() bool {
		var n int
		require.NoError(t, db.QueryRow(`SELECT count(*) FROM pgqueue_jobs WHERE status = 'completed'`).Scan(&n))

		return n == jobCount
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, seen, jobCount)
	for _, id := range ids {
		assert.Equal(t, 1, seen[id], "job %d must be executed exactly once", id)
	}
}

func TestWorker_RetriesUntilDead(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		return errors.New("smtp is down")
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil, pgqueue.WithMaxAttempts(2))
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "job dead after retries", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})
	row := getJob(t, db, id)
	assert.Equal(t, 2, row.Attempts)
	assert.Contains(t, row.LastError.String, "smtp is down")
}

func TestWorker_PermanentErrorSkipsRetries(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		return pgqueue.Permanent(errors.New("payload is garbage"))
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "job dead without retries", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})
	row := getJob(t, db, id)
	assert.Equal(t, 1, row.Attempts, "permanent errors must not retry")
	assert.Contains(t, row.LastError.String, "payload is garbage")
}

func TestWorker_UnregisteredJobTypeGoesDead(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SomethingElse", func(ctx context.Context, job pgqueue.Job) error { return nil })
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "NoSuchType", nil)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "unhandled job dead", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})
	assert.Contains(t, getJob(t, db, id).LastError.String, "no handler registered")
}

func TestWorker_HandlerPanicIsRetried(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	var mu sync.Mutex
	calls := 0
	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			panic("template blew up")
		}

		return nil
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "job recovered from panic and completed", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})
	assert.Equal(t, 2, getJob(t, db, id).Attempts)
}

func TestWorker_NotifyWakesIdleWorker(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	// A one-minute poll guarantees that prompt processing proves the LISTEN
	// wakeup path, not polling.
	cfg := pgqueue.WorkerConfig{PollInterval: time.Minute, ListenDSN: testDSN(t)}
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error { return nil })
	startWorker(t, w)
	time.Sleep(500 * time.Millisecond) // allow the LISTEN connection to establish

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "job completed via LISTEN wakeup", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})
}

func TestWorker_GracefulStopDrainsInFlightJob(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	started := make(chan struct{})
	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		close(started)
		time.Sleep(500 * time.Millisecond)

		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = w.Start(ctx) }()

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)

	<-started
	cancel() // shutdown begins while the job is mid-flight

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	assert.NoError(t, w.Stop(stopCtx))
	assert.Equal(t, string(pgqueue.StatusCompleted), getJob(t, db, id).Status, "in-flight job must finish during drain")
}

func TestWorker_RescuesJobsStuckInRunning(t *testing.T) {
	db := testDB(t)

	// A row a crashed worker left behind: running, claimed once, stale.
	var id int64
	require.NoError(t, db.QueryRow(
		`INSERT INTO pgqueue_jobs (queue, job_type, status, attempts, updated_at)
		 VALUES ('default', 'SendWelcomeEmail', 'running', 1, now() - interval '1 hour')
		 RETURNING id`,
	).Scan(&id))

	cfg := fastPoll()
	cfg.RescueAfter = 100 * time.Millisecond
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error { return nil })
	startWorker(t, w)

	waitFor(t, 5*time.Second, "stuck job rescued and completed", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})
	assert.Equal(t, 2, getJob(t, db, id).Attempts)
}
