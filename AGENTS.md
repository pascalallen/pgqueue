# pgqueue — notes for coding agents

Small, single-package Go library: a Postgres-backed durable job queue plus a
thin NOTIFY/LISTEN pub/sub. `doc.go` and `README.md` are the authoritative
description of behavior; this file only records what is not derivable from
the code.

## Commands

```bash
gofmt -l . && go vet ./... && staticcheck ./...
go test -race -cover ./...                       # unit tests only; integration tests skip
docker run -d --rm --name pg -e POSTGRES_HOST_AUTH_METHOD=trust -p 5544:5432 postgres:16-alpine
PGQUEUE_TEST_DSN="host=localhost port=5544 user=postgres dbname=postgres sslmode=disable" go test -race -cover ./...
```

Integration tests drop and recreate `pgqueue_jobs` in the target database, so
never point `PGQUEUE_TEST_DSN` at anything but a scratch instance, and never run
two suites against the same instance at once.

## Invariants — keep the tests that pin them

- Delivery is at-least-once. Every terminal write (`completeQuery`, `retryQuery`,
  `deadQuery`) must stay fenced on `status = 'running' AND attempts = $n`;
  `TestWorker_StaleRunCannotOverwriteASupersededJob` proves it.
- The rescue sweep must dead-letter rows whose `attempts >= max_attempts`
  (`TestWorker_RescueDeadLettersJobsThatExhaustedAttempts`) — it is the only
  path that can retire a job that crashes the process every time.
- `Worker.Start` / `Subscriber.Start` must return when their context is canceled
  even while Postgres is unreachable; `lib/pq`'s `Listener.Listen` blocks until
  connected, which is why `listen.go` exists.
- Dependencies are the standard library and `github.com/lib/pq` only.

## Schema is owned by host applications

`Schema` / `SchemaDown` are constants that hosts copy into their own migrations
(carline: migration `000011`). Changing `Schema` is a breaking change for hosts:
call it out in `CHANGELOG.md` and describe the follow-up migration.

## Conventions

- `go.mod`'s `go` directive is the lowest version the code needs (currently 1.24),
  not the newest release — it is a viral floor for every importer.
- Tests use testify with sentence-style names and `t.Run` subtests. New behavior
  lands test-first.
- Releases are semver git tags (`vX.Y.Z`) on `main`; the maintainer merges every
  PR and cuts every tag. Update `CHANGELOG.md` in the PR that makes the change.
- Workflow: GitHub issue → `feature/<issue#>-<slug>` branch → PR.
