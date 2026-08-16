# Changelog

All notable changes to this module are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the module follows
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- Terminal job writes (complete / retry / dead) are fenced on `status = 'running'`
  and the claimed attempt, so a run that was rescued and superseded while in
  flight can no longer overwrite the newer run's outcome. (#7)
- The rescue sweep dead-letters stuck jobs that have already used all of their
  attempts instead of re-queuing them forever (process-crash poison pills). (#7)
- `Worker.Start` and `Subscriber.Start` honor context cancellation while the
  LISTEN connection cannot be established; previously they blocked inside
  lib/pq indefinitely if Postgres was unreachable at startup. (#7)
- `Queue.Enqueue` inserts the job and issues its wakeup NOTIFY in a single
  statement, so on a plain `*sql.DB` the two commit or fail together and the
  call costs one round trip. (#7)
- `DefaultBackoff` documentation now states the real ceiling (256s before
  jitter, 320s after); the unreachable 5m cap branch was removed. (#7)

### Added
- `WorkerConfig.JobTimeout` bounds each handler invocation; the handler's
  context is canceled and the attempt fails with the context error. (#8)
- `Queue.Prune(ctx, olderThan)` deletes a queue's completed and dead jobs, and
  `WorkerConfig.Retain` has the worker do so on its maintenance sweep. (#8)
- `WorkerConfig.Middleware` (`[]Middleware`, `func(Handler) Handler`) wraps
  every registered handler — the hook for metrics and tracing. (#8)
- `Job.CreatedAt`, so handlers and middleware can measure queue lag. (#8)
- Recovered handler panics are logged with the panic value and stack. (#8)
- `ErrAlreadyStarted`, returned by a second `Worker.Start` / `Subscriber.Start`.
- Runnable examples on pkg.go.dev.
- `CHANGELOG.md`, `AGENTS.md`, Dependabot, and a staticcheck step plus a Go
  version matrix (declared floor + stable) in CI.

### Changed
- The rescue sweep (and retention) runs once at startup and then on its own
  ticker (`max(PollInterval, RescueAfter/2)`) instead of on every wakeup. (#8)
- `Worker.Register` and `Subscriber.Handle` panic when called after `Start`
  instead of racing the running goroutine.
- The `go` directive is now `1.24`, the floor the code actually needs, instead
  of the newest release; `github.com/lib/pq` bumped to v1.12.3.

## [1.0.0] - 2026-08-14

Initial release, extracted from `pascalallen/carline`.
