package pgqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
)

// Handler executes a claimed job. A nil return completes the job; an error
// schedules a retry (or moves the job to 'dead' once MaxAttempts is reached,
// immediately if the error is wrapped with Permanent). Delivery is
// at-least-once: handlers must tolerate re-execution of the same job.
type Handler func(ctx context.Context, job Job) error

type WorkerConfig struct {
	// QueueName selects which queue to consume. Default "default".
	QueueName string
	// Concurrency is the number of jobs executed simultaneously. Default 2.
	Concurrency int
	// PollInterval is the safety-net poll cadence; LISTEN provides prompt
	// wakeups when ListenDSN is set. Default 5s.
	PollInterval time.Duration
	// ListenDSN is a lib/pq connection string for the dedicated LISTEN
	// connection that wakes the worker as soon as a job is enqueued. Empty
	// means poll-only.
	ListenDSN string
	// Backoff maps the just-failed attempt number (1-based) to the delay
	// before the next attempt. Default DefaultBackoff.
	Backoff func(attempt int) time.Duration
	// RescueAfter re-queues jobs stuck in 'running' longer than this —
	// orphans left by a crashed process. It must comfortably exceed the
	// longest handler runtime. Default 5m.
	RescueAfter time.Duration
	Logger      Logger
}

// Worker claims and executes jobs from one queue. Register handlers, then
// run Start in a goroutine; cancel its context to begin a graceful drain and
// use Stop to wait for the drain to finish.
type Worker struct {
	db       *sql.DB
	cfg      WorkerConfig
	handlers map[string]Handler
	wake     chan struct{}
	running  atomic.Int32
	wg       sync.WaitGroup
	done     chan struct{}
	started  atomic.Bool
}

func NewWorker(db *sql.DB, cfg WorkerConfig) *Worker {
	if cfg.QueueName == "" {
		cfg.QueueName = "default"
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 2
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 5 * time.Second
	}
	if cfg.RescueAfter <= 0 {
		cfg.RescueAfter = 5 * time.Minute
	}
	if cfg.Backoff == nil {
		cfg.Backoff = DefaultBackoff
	}
	if cfg.Logger == nil {
		cfg.Logger = nopLogger{}
	}

	return &Worker{
		db:       db,
		cfg:      cfg,
		handlers: make(map[string]Handler),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
}

// DefaultBackoff doubles from 2s (attempt 1) to a ceiling of 256s (attempt 8
// and beyond), plus up to 25% jitter — so the longest possible delay is 320s.
func DefaultBackoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Second << uint(min(attempt, 8))

	return delay + rand.N(delay/4+1)
}

// ErrAlreadyStarted is returned by Worker.Start and Subscriber.Start when the
// receiver has already been started.
var ErrAlreadyStarted = errors.New("pgqueue: worker already started")

// Register maps a job type to its handler. It panics if called after Start:
// the handler map is read without locking by running jobs.
func (w *Worker) Register(jobType string, h Handler) {
	if w.started.Load() {
		panic("pgqueue: Register called after Start")
	}
	w.handlers[jobType] = h
}

// Start runs the claim loop until ctx is canceled, then waits for in-flight
// jobs to finish before returning. Handlers receive a context that survives
// the cancellation so drains complete cleanly.
func (w *Worker) Start(ctx context.Context) error {
	if !w.started.CompareAndSwap(false, true) {
		return ErrAlreadyStarted
	}
	defer close(w.done)

	if w.cfg.ListenDSN != "" {
		listener := pq.NewListener(w.cfg.ListenDSN, time.Second, time.Minute, nil)
		if err := listen(ctx, listener, jobsChannel); err != nil {
			return err
		}
		defer listener.Close()
		go w.forwardNotifications(ctx, listener)
	}

	execCtx := context.WithoutCancel(ctx)

	ticker := time.NewTicker(w.cfg.PollInterval)
	defer ticker.Stop()

	for {
		w.rescue(ctx)
		w.claimAndRun(ctx, execCtx)

		select {
		case <-ctx.Done():
			w.wg.Wait()
			return nil
		case <-ticker.C:
		case <-w.wake:
		}
	}
}

// Stop blocks until Start has fully drained and returned, or until ctx
// expires. It does not itself trigger the shutdown — cancel Start's context
// for that.
func (w *Worker) Stop(ctx context.Context) error {
	select {
	case <-w.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pgqueue: worker did not drain before deadline: %w", ctx.Err())
	}
}

func (w *Worker) forwardNotifications(ctx context.Context, listener *pq.Listener) {
	for {
		select {
		case <-ctx.Done():
			return
		case n, ok := <-listener.Notify:
			if !ok {
				return
			}
			// A nil notification signals a reconnect during which enqueues may
			// have been missed, so wake unconditionally in that case too.
			if n == nil || n.Extra == w.cfg.QueueName {
				w.poke()
			}
		}
	}
}

func (w *Worker) poke() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// rescueQuery re-queues orphaned jobs that still have attempts left and
// dead-letters the rest: a job that crashed the process on every attempt
// never reaches execute, so this is the only place it can be retired.
const rescueQuery = `
UPDATE pgqueue_jobs
SET status     = CASE WHEN attempts < max_attempts THEN 'pending' ELSE 'dead' END,
    last_error = CASE WHEN attempts < max_attempts THEN last_error ELSE 'rescued after exceeding max attempts' END,
    updated_at = now()
WHERE queue = $1 AND status = 'running' AND updated_at < now() - make_interval(secs => $2)
RETURNING status`

func (w *Worker) rescue(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx, rescueQuery, w.cfg.QueueName, w.cfg.RescueAfter.Seconds())
	if err != nil {
		if ctx.Err() == nil {
			w.cfg.Logger.Error("pgqueue: rescue sweep failed", "error", err, "queue", w.cfg.QueueName)
		}
		return
	}
	defer rows.Close()

	var requeued, dead int
	for rows.Next() {
		var status Status
		if err := rows.Scan(&status); err != nil {
			w.cfg.Logger.Error("pgqueue: rescue sweep failed", "error", err, "queue", w.cfg.QueueName)
			return
		}
		if status == StatusDead {
			dead++
		} else {
			requeued++
		}
	}
	if err := rows.Err(); err != nil {
		w.cfg.Logger.Error("pgqueue: rescue sweep failed", "error", err, "queue", w.cfg.QueueName)
		return
	}
	if requeued > 0 {
		w.cfg.Logger.Warn("pgqueue: rescued stuck jobs", "count", requeued, "queue", w.cfg.QueueName)
	}
	if dead > 0 {
		w.cfg.Logger.Error("pgqueue: dead-lettered stuck jobs that exhausted their attempts", "count", dead, "queue", w.cfg.QueueName)
	}
}

const claimQuery = `
UPDATE pgqueue_jobs
SET status = 'running', attempts = attempts + 1, updated_at = now()
WHERE id IN (
    SELECT id
    FROM pgqueue_jobs
    WHERE queue = $1 AND status = 'pending' AND run_at <= now()
    ORDER BY run_at, id
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
RETURNING id, queue, job_type, payload, attempts, max_attempts, run_at, COALESCE(last_error, '')`

func (w *Worker) claimAndRun(ctx context.Context, execCtx context.Context) {
	for {
		free := w.cfg.Concurrency - int(w.running.Load())
		if free <= 0 {
			return
		}

		jobs, err := w.claim(ctx, free)
		if err != nil {
			if ctx.Err() == nil {
				w.cfg.Logger.Error("pgqueue: claim failed", "error", err, "queue", w.cfg.QueueName)
			}
			return
		}
		if len(jobs) == 0 {
			return
		}

		for _, job := range jobs {
			w.running.Add(1)
			w.wg.Add(1)
			go func(job Job) {
				defer w.wg.Done()
				defer func() {
					w.running.Add(-1)
					w.poke()
				}()
				w.execute(execCtx, job)
			}(job)
		}

		if len(jobs) < free {
			return
		}
	}
}

func (w *Worker) claim(ctx context.Context, limit int) ([]Job, error) {
	rows, err := w.db.QueryContext(ctx, claimQuery, w.cfg.QueueName, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		var job Job
		if err := rows.Scan(&job.ID, &job.Queue, &job.Type, &job.Payload, &job.Attempts, &job.MaxAttempts, &job.RunAt, &job.LastError); err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}

	return jobs, rows.Err()
}

// Terminal writes are fenced on (status = 'running', attempts = the claimed
// attempt): if the rescue sweep re-queued the job and another claim superseded
// this run, the stale run's result must not overwrite the newer one.
const (
	completeQuery = `UPDATE pgqueue_jobs SET status = 'completed', completed_at = now(), updated_at = now() WHERE id = $1 AND status = 'running' AND attempts = $2`
	retryQuery    = `UPDATE pgqueue_jobs SET status = 'pending', run_at = $3, last_error = $4, updated_at = now() WHERE id = $1 AND status = 'running' AND attempts = $2`
	deadQuery     = `UPDATE pgqueue_jobs SET status = 'dead', last_error = $3, updated_at = now() WHERE id = $1 AND status = 'running' AND attempts = $2`
)

func (w *Worker) execute(ctx context.Context, job Job) {
	var err error
	if handler, ok := w.handlers[job.Type]; ok {
		err = w.runHandler(ctx, handler, job)
	} else {
		err = Permanent(fmt.Errorf("no handler registered for job type %q", job.Type))
	}

	switch {
	case err == nil:
		if !w.finish(ctx, job, "completed", completeQuery, job.ID, job.Attempts) {
			return
		}
		w.cfg.Logger.Debug("pgqueue: job completed", "job_id", job.ID, "job_type", job.Type, "attempt", job.Attempts)
	case IsPermanent(err) || job.Attempts >= job.MaxAttempts:
		if !w.finish(ctx, job, "dead", deadQuery, job.ID, job.Attempts, err.Error()) {
			return
		}
		w.cfg.Logger.Error("pgqueue: job dead", "error", err, "job_id", job.ID, "job_type", job.Type, "attempts", job.Attempts)
	default:
		runAt := time.Now().Add(w.cfg.Backoff(job.Attempts))
		if !w.finish(ctx, job, "retry", retryQuery, job.ID, job.Attempts, runAt, err.Error()) {
			return
		}
		w.cfg.Logger.Warn("pgqueue: job failed; will retry", "error", err, "job_id", job.ID, "job_type", job.Type, "attempt", job.Attempts, "next_run_at", runAt)
	}
}

// finish issues a fenced terminal write and reports whether it took effect.
// A write that matched no row means the job was rescued and superseded while
// this run was in flight; the newer run owns the outcome, so the stale result
// is dropped and logged.
func (w *Worker) finish(ctx context.Context, job Job, outcome string, query string, args ...any) bool {
	res, err := w.db.ExecContext(ctx, query, args...)
	if err != nil {
		w.cfg.Logger.Error("pgqueue: failed to mark job "+outcome, "error", err, "job_id", job.ID)
		return false
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		w.cfg.Logger.Warn("pgqueue: stale run superseded by rescue; result dropped", "outcome", outcome, "job_id", job.ID, "job_type", job.Type, "attempt", job.Attempts)
		return false
	}

	return true
}

func (w *Worker) runHandler(ctx context.Context, handler Handler, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("pgqueue: handler panicked: %v", r)
		}
	}()

	return handler(ctx, job)
}
