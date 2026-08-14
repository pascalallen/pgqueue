package pgqueue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Status is the lifecycle state of a job row.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusDead      Status = "dead"
)

// jobsChannel is the NOTIFY channel that wakes workers promptly after an
// enqueue; the notification payload is the queue name.
const jobsChannel = "pgqueue_jobs"

// Job is a claimed job as handed to a Handler. Attempts includes the attempt
// currently executing (it is 1 on the first delivery).
type Job struct {
	ID          int64
	Queue       string
	Type        string
	Payload     json.RawMessage
	Attempts    int
	MaxAttempts int
	RunAt       time.Time
	LastError   string
}

// DBTX is the subset of database/sql operations this package needs. It is
// satisfied by both *sql.DB and *sql.Tx.
type DBTX interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Queue enqueues jobs onto a single named queue.
type Queue struct {
	db          *sql.DB
	name        string
	maxAttempts int
	logger      Logger
}

type Option func(*Queue)

// WithQueueName routes jobs to a queue other than "default".
func WithQueueName(name string) Option {
	return func(q *Queue) {
		if name != "" {
			q.name = name
		}
	}
}

func WithLogger(l Logger) Option {
	return func(q *Queue) {
		if l != nil {
			q.logger = l
		}
	}
}

// WithDefaultMaxAttempts changes the per-queue default (5) applied to jobs
// enqueued without WithMaxAttempts.
func WithDefaultMaxAttempts(n int) Option {
	return func(q *Queue) {
		if n > 0 {
			q.maxAttempts = n
		}
	}
}

func New(db *sql.DB, opts ...Option) *Queue {
	q := &Queue{
		db:          db,
		name:        "default",
		maxAttempts: 5,
		logger:      nopLogger{},
	}
	for _, opt := range opts {
		opt(q)
	}

	return q
}

type enqueueSettings struct {
	runAt       time.Time
	maxAttempts int
}

type EnqueueOption func(*enqueueSettings)

// WithRunAt schedules the job for a future time instead of immediately.
func WithRunAt(t time.Time) EnqueueOption {
	return func(s *enqueueSettings) { s.runAt = t }
}

func WithMaxAttempts(n int) EnqueueOption {
	return func(s *enqueueSettings) {
		if n > 0 {
			s.maxAttempts = n
		}
	}
}

// Enqueue inserts a job and notifies workers. payload must be valid JSON
// (nil is stored as {}).
func (q *Queue) Enqueue(ctx context.Context, jobType string, payload []byte, opts ...EnqueueOption) (int64, error) {
	return q.enqueue(ctx, q.db, jobType, payload, opts...)
}

// EnqueueTx is Enqueue inside the caller's transaction: the job row and its
// wakeup NOTIFY are only visible/delivered if the transaction commits.
func (q *Queue) EnqueueTx(ctx context.Context, tx *sql.Tx, jobType string, payload []byte, opts ...EnqueueOption) (int64, error) {
	return q.enqueue(ctx, tx, jobType, payload, opts...)
}

const enqueueQuery = `
INSERT INTO pgqueue_jobs (queue, job_type, payload, max_attempts, run_at)
VALUES ($1, $2, $3, $4, $5)
RETURNING id`

func (q *Queue) enqueue(ctx context.Context, db DBTX, jobType string, payload []byte, opts ...EnqueueOption) (int64, error) {
	settings := enqueueSettings{
		runAt:       time.Now(),
		maxAttempts: q.maxAttempts,
	}
	for _, opt := range opts {
		opt(&settings)
	}
	if len(payload) == 0 {
		payload = []byte("{}")
	}

	var id int64
	err := db.QueryRowContext(ctx, enqueueQuery, q.name, jobType, payload, settings.maxAttempts, settings.runAt).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("pgqueue: enqueue %s: %w", jobType, err)
	}

	if _, err := db.ExecContext(ctx, `SELECT pg_notify($1, $2)`, jobsChannel, q.name); err != nil {
		return 0, fmt.Errorf("pgqueue: notify after enqueue %s: %w", jobType, err)
	}

	q.logger.Debug("pgqueue: job enqueued", "job_id", id, "queue", q.name, "job_type", jobType)

	return id, nil
}

type permanentError struct {
	cause error
}

// Permanent wraps an error to tell the worker that retrying is pointless
// (e.g. an unparseable payload): the job goes straight to 'dead'.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return &permanentError{cause: err}
}

func (e *permanentError) Error() string { return "permanent: " + e.cause.Error() }
func (e *permanentError) Unwrap() error { return e.cause }

// IsPermanent reports whether err is (or wraps) a Permanent error.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
