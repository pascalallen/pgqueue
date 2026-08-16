package pgqueue_test

import (
	"context"
	"errors"
	"fmt"
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

func TestWorker_StaleRunCannotOverwriteASupersededJob(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	// Attempt 1 hangs past RescueAfter so the sweep re-queues the job and a
	// second claim (attempt 2) completes it. When attempt 1 finally returns an
	// error, its retry write must be a no-op: the job was superseded.
	release := make(chan struct{})
	secondDone := make(chan struct{})
	cfg := fastPoll()
	cfg.RescueAfter = 100 * time.Millisecond
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		switch job.Attempts {
		case 1:
			<-release
			return errors.New("slow run finally failed")
		case 2:
			close(secondDone)
			return nil
		}
		return nil
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)

	select {
	case <-secondDone:
	case <-time.After(5 * time.Second):
		t.Fatal("rescued job was never re-run")
	}
	waitFor(t, 5*time.Second, "second run completed", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})

	close(release)
	// Give the stale run time to issue its (ignored) retry write.
	time.Sleep(300 * time.Millisecond)

	row := getJob(t, db, id)
	assert.Equal(t, string(pgqueue.StatusCompleted), row.Status, "a stale run must not overwrite the superseding run's result")
	assert.Equal(t, 2, row.Attempts)
	assert.False(t, row.LastError.Valid, "the stale run's error must not be recorded")
}

func TestWorker_RescueDeadLettersJobsThatExhaustedAttempts(t *testing.T) {
	db := testDB(t)

	// A poison pill: every attempt crashed the process before it could report
	// back, so the row is stuck running with attempts already at the maximum.
	var id int64
	require.NoError(t, db.QueryRow(
		`INSERT INTO pgqueue_jobs (queue, job_type, status, attempts, max_attempts, updated_at)
		 VALUES ('default', 'SendWelcomeEmail', 'running', 3, 3, now() - interval '1 hour')
		 RETURNING id`,
	).Scan(&id))

	calls := make(chan struct{}, 8)
	cfg := fastPoll()
	cfg.RescueAfter = 100 * time.Millisecond
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		calls <- struct{}{}
		return nil
	})
	startWorker(t, w)

	waitFor(t, 5*time.Second, "exhausted job dead-lettered by rescue", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})
	row := getJob(t, db, id)
	assert.Equal(t, 3, row.Attempts, "rescue must not grant another attempt")
	assert.Contains(t, row.LastError.String, "max attempts")
	assert.Empty(t, calls, "an exhausted job must not run again")
}

func TestWorker_StartHonorsContextWhileListenCannotConnect(t *testing.T) {
	db := testDB(t)

	// Nothing listens on port 1: the LISTEN connection can never be
	// established, and lib/pq's Listen blocks until it is.
	cfg := fastPoll()
	cfg.ListenDSN = "host=127.0.0.1 port=1 user=postgres sslmode=disable connect_timeout=1"
	w := pgqueue.NewWorker(db, cfg)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- w.Start(ctx) }()

	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("Start did not return after its context was canceled")
	}
}

func TestWorker_SecondStartReturnsErrAlreadyStarted(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)
	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error { return nil })
	startWorker(t, w)

	// A completed job proves the first Start owns the worker before we race it.
	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)
	waitFor(t, 5*time.Second, "worker running", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})

	assert.ErrorIs(t, w.Start(context.Background()), pgqueue.ErrAlreadyStarted)
}

func TestWorker_RegisterAfterStartPanics(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)
	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error { return nil })
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)
	waitFor(t, 5*time.Second, "worker running", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusCompleted)
	})

	assert.Panics(t, func() {
		w.Register("Late", func(ctx context.Context, job pgqueue.Job) error { return nil })
	})
}

func TestWorker_JobCarriesCreatedAt(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	got := make(chan pgqueue.Job, 1)
	w := pgqueue.NewWorker(db, fastPoll())
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		got <- job
		return nil
	})
	startWorker(t, w)

	before := time.Now()
	_, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)

	select {
	case job := <-got:
		assert.WithinDuration(t, before, job.CreatedAt, 5*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("job was not delivered")
	}
}

func TestWorker_JobTimeoutCancelsHandlerContext(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	cfg := fastPoll()
	cfg.JobTimeout = 100 * time.Millisecond
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		<-ctx.Done() // a handler that only stops when told to
		return ctx.Err()
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil, pgqueue.WithMaxAttempts(1))
	require.NoError(t, err)

	waitFor(t, 5*time.Second, "timed-out job dead-lettered", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})
	assert.Contains(t, getJob(t, db, id).LastError.String, context.DeadlineExceeded.Error())
}

// recordingLogger captures log calls so tests can assert on structured fields.
type recordingLogger struct {
	mu    sync.Mutex
	calls []logCall
}

type logCall struct {
	level   string
	msg     string
	keyVals []any
}

func (l *recordingLogger) record(level, msg string, kv []any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, logCall{level, msg, kv})
}
func (l *recordingLogger) Debug(msg string, kv ...any) { l.record("debug", msg, kv) }
func (l *recordingLogger) Info(msg string, kv ...any)  { l.record("info", msg, kv) }
func (l *recordingLogger) Warn(msg string, kv ...any)  { l.record("warn", msg, kv) }
func (l *recordingLogger) Error(msg string, kv ...any) { l.record("error", msg, kv) }

// value returns the value logged under key for the first call with msg.
func (l *recordingLogger) value(msg, key string) (any, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, c := range l.calls {
		if c.msg != msg {
			continue
		}
		for i := 0; i+1 < len(c.keyVals); i += 2 {
			if c.keyVals[i] == key {
				return c.keyVals[i+1], true
			}
		}
	}
	return nil, false
}

func TestWorker_RetainPrunesFinishedJobsOlderThanRetain(t *testing.T) {
	db := testDB(t)

	var oldCompleted, recentCompleted int64
	require.NoError(t, db.QueryRow(
		`INSERT INTO pgqueue_jobs (queue, job_type, status, updated_at)
		 VALUES ('default', 'SendWelcomeEmail', 'completed', now() - interval '1 hour') RETURNING id`).Scan(&oldCompleted))
	require.NoError(t, db.QueryRow(
		`INSERT INTO pgqueue_jobs (queue, job_type, status, updated_at)
		 VALUES ('default', 'SendWelcomeEmail', 'completed', now() + interval '1 hour') RETURNING id`).Scan(&recentCompleted))

	cfg := fastPoll()
	cfg.Retain = 30 * time.Minute
	w := pgqueue.NewWorker(db, cfg)
	startWorker(t, w)

	waitFor(t, 5*time.Second, "old completed job pruned", func() bool {
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pgqueue_jobs WHERE id = $1)`, oldCompleted).Scan(&exists))
		return !exists
	})
	var exists bool
	require.NoError(t, db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pgqueue_jobs WHERE id = $1)`, recentCompleted).Scan(&exists))
	assert.True(t, exists, "jobs younger than Retain must survive")
}

func TestWorker_HandlerPanicIsLoggedWithStack(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	logger := &recordingLogger{}
	cfg := fastPoll()
	cfg.Logger = logger
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		panic("template blew up")
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil, pgqueue.WithMaxAttempts(1))
	require.NoError(t, err)
	waitFor(t, 5*time.Second, "panicking job dead", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})

	stack, ok := logger.value("pgqueue: handler panicked", "stack")
	require.True(t, ok, "a panic must be logged with its stack")
	assert.Contains(t, stack.(string), "goroutine")
	panicked, _ := logger.value("pgqueue: handler panicked", "panic")
	assert.Equal(t, "template blew up", panicked)
}

func TestWorker_MiddlewareWrapsHandlers(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	var mu sync.Mutex
	var trace []string
	mw := func(name string) pgqueue.Middleware {
		return func(next pgqueue.Handler) pgqueue.Handler {
			return func(ctx context.Context, job pgqueue.Job) error {
				mu.Lock()
				trace = append(trace, name+":before:"+job.Type)
				mu.Unlock()
				err := next(ctx, job)
				mu.Lock()
				trace = append(trace, name+":after:"+fmt.Sprint(err))
				mu.Unlock()
				return err
			}
		}
	}

	cfg := fastPoll()
	cfg.Middleware = []pgqueue.Middleware{mw("outer"), mw("inner")}
	w := pgqueue.NewWorker(db, cfg)
	w.Register("SendWelcomeEmail", func(ctx context.Context, job pgqueue.Job) error {
		mu.Lock()
		trace = append(trace, "handler")
		mu.Unlock()
		return pgqueue.Permanent(errors.New("boom"))
	})
	startWorker(t, w)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil)
	require.NoError(t, err)
	waitFor(t, 5*time.Second, "job dead", func() bool {
		return getJob(t, db, id).Status == string(pgqueue.StatusDead)
	})

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []string{
		"outer:before:SendWelcomeEmail",
		"inner:before:SendWelcomeEmail",
		"handler",
		"inner:after:permanent: boom",
		"outer:after:permanent: boom",
	}, trace, "the first middleware is outermost and errors flow back through the chain")
}
