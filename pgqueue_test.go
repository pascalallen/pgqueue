package pgqueue_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/pascalallen/pgqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers ---

func testDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("PGQUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("PGQUEUE_TEST_DSN not set; skipping pgqueue integration tests")
	}

	return dsn
}

// testDB opens the integration database and resets the pgqueue schema.
func testDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("postgres", testDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, db.PingContext(ctx))

	_, err = db.ExecContext(ctx, pgqueue.SchemaDown)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, pgqueue.Schema)
	require.NoError(t, err)

	return db
}

type jobRow struct {
	Queue       string
	Type        string
	Payload     string
	Status      string
	Attempts    int
	MaxAttempts int
	RunAt       time.Time
	LastError   sql.NullString
	CompletedAt sql.NullTime
}

func getJob(t *testing.T, db *sql.DB, id int64) jobRow {
	t.Helper()

	var row jobRow
	err := db.QueryRow(
		`SELECT queue, job_type, payload, status, attempts, max_attempts, run_at, last_error, completed_at
		 FROM pgqueue_jobs WHERE id = $1`, id,
	).Scan(&row.Queue, &row.Type, &row.Payload, &row.Status, &row.Attempts, &row.MaxAttempts, &row.RunAt, &row.LastError, &row.CompletedAt)
	require.NoError(t, err)

	return row
}

func countJobs(t *testing.T, db *sql.DB) int {
	t.Helper()

	var n int
	require.NoError(t, db.QueryRow(`SELECT count(*) FROM pgqueue_jobs`).Scan(&n))

	return n
}

// waitFor polls cond until it holds or the timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s: %s", timeout, msg)
}

// startWorker runs w in a goroutine and guarantees a drained shutdown at
// test cleanup.
func startWorker(t *testing.T, w *pgqueue.Worker) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = w.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		assert.NoError(t, w.Stop(stopCtx))
	})
}

// --- enqueue tests ---

func TestEnqueue_AppliesDefaults(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", []byte(`{"user_id":"01ABC"}`))

	assert.NoError(t, err)
	row := getJob(t, db, id)
	assert.Equal(t, "default", row.Queue)
	assert.Equal(t, "SendWelcomeEmail", row.Type)
	assert.JSONEq(t, `{"user_id":"01ABC"}`, row.Payload)
	assert.Equal(t, string(pgqueue.StatusPending), row.Status)
	assert.Equal(t, 0, row.Attempts)
	assert.Equal(t, 5, row.MaxAttempts)
	assert.WithinDuration(t, time.Now(), row.RunAt, 5*time.Second)
	assert.False(t, row.CompletedAt.Valid)
}

func TestEnqueue_AppliesOptions(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db, pgqueue.WithQueueName("mail"), pgqueue.WithDefaultMaxAttempts(7))
	runAt := time.Now().Add(time.Hour)

	id, err := q.Enqueue(context.Background(), "SendWelcomeEmail", nil, pgqueue.WithRunAt(runAt), pgqueue.WithMaxAttempts(2))

	assert.NoError(t, err)
	row := getJob(t, db, id)
	assert.Equal(t, "mail", row.Queue)
	assert.JSONEq(t, `{}`, row.Payload)
	assert.Equal(t, 2, row.MaxAttempts)
	assert.WithinDuration(t, runAt, row.RunAt, time.Second)
}

func TestEnqueueTx_HonorsTransactionOutcome(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)
	ctx := context.Background()

	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = q.EnqueueTx(ctx, tx, "SendWelcomeEmail", nil)
	assert.NoError(t, err)
	require.NoError(t, tx.Rollback())
	assert.Equal(t, 0, countJobs(t, db), "rolled-back enqueue must leave no row")

	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	id, err := q.EnqueueTx(ctx, tx, "SendWelcomeEmail", nil)
	assert.NoError(t, err)
	require.NoError(t, tx.Commit())
	assert.Equal(t, string(pgqueue.StatusPending), getJob(t, db, id).Status)
}

// --- prune tests ---

func TestPrune_RemovesOnlyOldFinishedJobsOfTheQueue(t *testing.T) {
	db := testDB(t)
	q := pgqueue.New(db)

	insert := func(queue, status string, age string) int64 {
		t.Helper()
		var id int64
		require.NoError(t, db.QueryRow(
			`INSERT INTO pgqueue_jobs (queue, job_type, status, updated_at)
			 VALUES ($1, 'SendWelcomeEmail', $2, now() - $3::interval) RETURNING id`, queue, status, age,
		).Scan(&id))
		return id
	}
	oldCompleted := insert("default", "completed", "2 hours")
	oldDead := insert("default", "dead", "2 hours")
	recentCompleted := insert("default", "completed", "1 minute")
	oldPending := insert("default", "pending", "2 hours")
	oldRunning := insert("default", "running", "2 hours")
	otherQueueOldCompleted := insert("mail", "completed", "2 hours")

	n, err := q.Prune(context.Background(), time.Hour)

	require.NoError(t, err)
	assert.Equal(t, int64(2), n)
	assert.Equal(t, 4, countJobs(t, db))
	for _, id := range []int64{recentCompleted, oldPending, oldRunning, otherQueueOldCompleted} {
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pgqueue_jobs WHERE id = $1)`, id).Scan(&exists))
		assert.True(t, exists, "job %d must survive the prune", id)
	}
	for _, id := range []int64{oldCompleted, oldDead} {
		var exists bool
		require.NoError(t, db.QueryRow(`SELECT EXISTS (SELECT 1 FROM pgqueue_jobs WHERE id = $1)`, id).Scan(&exists))
		assert.False(t, exists, "job %d must be pruned", id)
	}
}
