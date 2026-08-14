# pgqueue

[![Go Reference](https://pkg.go.dev/badge/github.com/pascalallen/pgqueue.svg)](https://pkg.go.dev/github.com/pascalallen/pgqueue)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/pascalallen/pgqueue)
[![Go Report Card](https://goreportcard.com/badge/github.com/pascalallen/pgqueue)](https://goreportcard.com/report/github.com/pascalallen/pgqueue)
![GitHub Workflow Status (with branch)](https://img.shields.io/github/actions/workflow/status/pascalallen/pgqueue/go.yml?branch=main)
![GitHub](https://img.shields.io/github/license/pascalallen/pgqueue)
![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/pascalallen/pgqueue)

pgqueue is a Postgres-backed durable job queue and NOTIFY/LISTEN pub/sub module for Go, built on `database/sql` and `github.com/lib/pq` — jobs are claimed with `FOR UPDATE SKIP LOCKED`, failures retry with exponential backoff until they land in a dead-letter state, orphaned jobs are rescued after a crash, and delivery is at-least-once.

## Installation

Use the Go CLI tool [go](https://go.dev/dl/) to install pgqueue.

```bash
go get github.com/pascalallen/pgqueue
```

## Usage

Apply the schema and enqueue jobs:

```go
...

import (
    "github.com/pascalallen/pgqueue"
)

...

// pgqueue.Schema creates the pgqueue_jobs table and its indexes. Copy it
// into your application's migrations; for a quick start, execute it directly.
if _, err := db.ExecContext(ctx, pgqueue.Schema); err != nil {
    // Schema not applied; nothing below will work.
}

q := pgqueue.New(db)

id, err := q.Enqueue(ctx, "send_welcome_email", []byte(`{"user_id":"01J5..."}`))
if err != nil {
    // The job row was not inserted; nothing will run.
}

// EnqueueTx makes the job part of a business transaction: the job row and
// its wakeup NOTIFY are only visible/delivered if tx commits.
id, err = q.EnqueueTx(ctx, tx, "send_welcome_email", payload)
if err != nil {
    // Roll back tx; the job rides the transaction.
}

...
```

Run a worker:

```go
...

w := pgqueue.NewWorker(db, pgqueue.WorkerConfig{
    ListenDSN: dsn, // dedicated LISTEN connection for prompt wakeups; empty means poll-only
})

w.Register("send_welcome_email", func(ctx context.Context, job pgqueue.Job) error {
    var p struct {
        UserID string `json:"user_id"`
    }
    if err := json.Unmarshal(job.Payload, &p); err != nil {
        // Retrying an unparseable payload is pointless: go straight to 'dead'.
        return pgqueue.Permanent(err)
    }

    // A plain error schedules a retry with backoff; nil completes the job.
    return sendWelcomeEmail(ctx, p.UserID)
})

go func() {
    if err := w.Start(ctx); err != nil {
        // Start only fails before the claim loop begins (already started, LISTEN error).
    }
}()

...

// Graceful shutdown: cancel Start's context to stop claiming, then wait for
// in-flight jobs to drain.
cancel()
if err := w.Stop(shutdownCtx); err != nil {
    // The worker did not drain before the deadline.
}

...
```

Fire-and-forget pub/sub over NOTIFY/LISTEN:

```go
...

// Publish sends to every process currently LISTENing on the channel. Pass a
// *sql.Tx instead of db to defer delivery until commit.
if err := pgqueue.Publish(ctx, db, "orders_updated", []byte(`{"order_id":42}`)); err != nil {
    // The NOTIFY was not issued.
}

s := pgqueue.NewSubscriber(dsn, nil)
s.Handle("orders_updated", func(payload []byte) {
    // Callbacks run sequentially on the subscriber's goroutine — keep them
    // fast or hand off internally.
})

if err := s.Start(ctx); err != nil {
    // LISTEN failed; Start returns nil once ctx is canceled.
}

...
```

### At-least-once delivery

- Workers claim batches with `FOR UPDATE SKIP LOCKED`, so any number of processes can consume the same queue without double-processing a claimed job.
- A failed job retries with exponential backoff (default: doubling from 2s to a ceiling of ~4m16s, plus up to 25% jitter) until `MaxAttempts` (default 5), then remains in the table with `status = 'dead'` for inspection. Wrap an error with `pgqueue.Permanent` to skip retries and dead-letter immediately.
- A rescue sweep re-queues jobs stuck in `running` longer than `RescueAfter` (default 5m) — orphans left by a crashed worker. This makes delivery at-least-once: handlers must tolerate being invoked more than once for the same job.

### Schema ownership

pgqueue never runs migrations. The `pgqueue.Schema` constant creates the `pgqueue_jobs` table and its indexes, and `pgqueue.SchemaDown` drops everything it creates — copy them into your application's own migration files. If a release ever changes `Schema`, add a follow-up migration in the host application.

### Pub/sub is not durable

`Publish`/`Subscriber` wrap Postgres NOTIFY/LISTEN, which is fire-and-forget: a subscriber that is disconnected (or mid-reconnect) when a notification fires never sees it, and Postgres caps NOTIFY payloads at ~8000 bytes. Use it as a real-time signal; when you need durability, use the job queue.

## Testing

Run the test suite with the race detector and coverage:

```bash
go test -race -cover ./...
```

Integration tests need a Postgres database and are skipped when `PGQUEUE_TEST_DSN` is unset. To run them against a scratch Postgres:

```bash
docker run -d --rm --name pg -e POSTGRES_HOST_AUTH_METHOD=trust -p 5544:5432 postgres:16-alpine
PGQUEUE_TEST_DSN="host=localhost port=5544 user=postgres dbname=postgres sslmode=disable" go test -race -cover ./...
```

Create and view a coverage profile:

```bash
go test -covermode=count -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first
to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

[MIT](LICENSE)
