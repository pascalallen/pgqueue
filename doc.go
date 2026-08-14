// Package pgqueue is a small Postgres-backed durable job queue and pub/sub
// layer built on database/sql and lib/pq.
//
// Jobs are rows in the pgqueue_jobs table (see Schema). Producers enqueue
// with Queue.Enqueue or, to make a job part of a business transaction,
// Queue.EnqueueTx — the wakeup NOTIFY rides the caller's transaction, so
// workers are only woken for jobs that actually committed. Workers claim
// batches with FOR UPDATE SKIP LOCKED, so any number of processes can
// consume the same queue without double-processing. Failed jobs retry with
// backoff until MaxAttempts, then remain in the table with status 'dead'
// for inspection. A rescue sweep re-queues jobs orphaned by a crashed
// worker, which makes delivery at-least-once: handlers must tolerate being
// invoked more than once for the same job.
//
// Publish and Subscriber wrap Postgres NOTIFY/LISTEN for fire-and-forget
// fan-out to every listening process. Notifications carry no durability:
// a subscriber that is disconnected when one fires never sees it.
//
// Dependency policy: this package imports only the standard library and
// github.com/lib/pq, so it stays trivial to vendor or embed in any
// database/sql-based application.
package pgqueue
