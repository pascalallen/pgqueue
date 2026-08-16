package pgqueue_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"log/slog"
	"time"

	"github.com/pascalallen/pgqueue"
)

// These examples have no "// Output:" line, so they are compiled but not run:
// they need a live Postgres.

func ExampleQueue_Enqueue() {
	db, err := sql.Open("postgres", "postgres://localhost/app?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()

	// pgqueue.Schema creates the pgqueue_jobs table; copy it into your own
	// migrations. Executing it directly is fine for a quick start.
	if _, err := db.ExecContext(ctx, pgqueue.Schema); err != nil {
		log.Fatal(err)
	}

	q := pgqueue.New(db, pgqueue.WithLogger(slog.Default()))

	id, err := q.Enqueue(ctx, "send_welcome_email", []byte(`{"user_id":"01J5..."}`),
		pgqueue.WithMaxAttempts(3),
		pgqueue.WithRunAt(time.Now().Add(time.Minute)),
	)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("enqueued job", id)
}

func ExampleQueue_EnqueueTx() {
	db, err := sql.Open("postgres", "postgres://localhost/app?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	q := pgqueue.New(db)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()

	// ... business writes on tx ...

	// The job row and its wakeup NOTIFY become visible only if tx commits.
	if _, err := q.EnqueueTx(ctx, tx, "send_welcome_email", []byte(`{"user_id":"01J5..."}`)); err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
}

func ExampleWorker() {
	const dsn = "postgres://localhost/app?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}

	w := pgqueue.NewWorker(db, pgqueue.WorkerConfig{
		Concurrency: 4,
		ListenDSN:   dsn, // dedicated LISTEN connection; empty means poll-only
		Logger:      slog.Default(),
	})

	w.Register("send_welcome_email", func(ctx context.Context, job pgqueue.Job) error {
		var p struct {
			UserID string `json:"user_id"`
		}
		if err := json.Unmarshal(job.Payload, &p); err != nil {
			// Retrying an unparseable payload is pointless: dead-letter now.
			return pgqueue.Permanent(err)
		}
		// A plain error schedules a retry with backoff; nil completes the job.
		return sendWelcomeEmail(ctx, p.UserID)
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		if err := w.Start(ctx); err != nil {
			log.Println("worker stopped:", err)
		}
	}()

	// ... on shutdown: stop claiming, then wait for in-flight jobs to drain.
	cancel()
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer stopCancel()
	if err := w.Stop(stopCtx); err != nil {
		log.Println("worker did not drain in time:", err)
	}
}

func ExamplePublish() {
	const dsn = "postgres://localhost/app?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub := pgqueue.NewSubscriber(dsn, slog.Default())
	sub.Handle("orders_updated", func(payload []byte) {
		// Runs on the subscriber goroutine: keep it fast or hand off.
		log.Printf("order update: %s", payload)
	})
	go func() {
		if err := sub.Start(ctx); err != nil {
			log.Println("subscriber stopped:", err)
		}
	}()

	// Every process currently LISTENing on the channel receives this; pass a
	// *sql.Tx instead of db to defer delivery until commit.
	if err := pgqueue.Publish(ctx, db, "orders_updated", []byte(`{"order_id":42}`)); err != nil {
		log.Fatal(err)
	}
}

func sendWelcomeEmail(context.Context, string) error { return nil }
