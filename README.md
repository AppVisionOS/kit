# kit

Small, dependency-light Go packages shared by AppVision backends.

```
go get github.com/AppVisionOS/kit@latest
```

| Package | What it does |
|---|---|
| [`jobs`](jobs) | Runs scheduled jobs under a per-job Postgres advisory lock and records every run, so a job never runs twice at once and a job that stops succeeding shows up in a health check. |
| [`health`](health) | `Live()` (touches nothing) and `DB()` (`SELECT 1` with a timeout) HTTP handlers. |

Both work with plain `database/sql`. With GORM, pass `gormDB.DB()`. With Gin, wrap handlers in `gin.WrapH`.

## jobs

```go
sqlDB, _ := gormDB.DB()
guard, err := jobs.New(sqlDB, jobs.WithHooks(jobs.Hooks{
    // optional: forward to Sentry cron check-ins, metrics, logs...
    Finish: func(name, schedule string, took time.Duration, err error) { /* ... */ },
}))
if err != nil { log.Fatal(err) }
if err := guard.Migrate(ctx); err != nil { log.Fatal(err) } // CREATE TABLE IF NOT EXISTS job_runs

// with gocron, robfig/cron, a ticker, anything:
scheduler.NewJob(gocron.CronJob("*/5 * * * *", false), gocron.NewTask(func() {
    err := guard.Run(ctx, "renew-credits", "*/5 * * * *", renewCredits)
    if errors.Is(err, jobs.ErrSkipped) { return } // previous run still going
}))

router.GET("/health/jobs", gin.WrapH(guard.Handler()))
```

What a run does:

1. `pg_try_advisory_lock(namespace, hashtext(name))` on a dedicated connection. If another run of the same job holds it (in this process or any other), the body is not called and `Run` returns `ErrSkipped`.
2. Calls the job. A panic is recorded as a failure, the lock is released, and the panic continues.
3. Discards the connection, which ends the session and drops the lock even if the unlock never reached the server. A process that dies drops its locks with its connections, so a lock can never stay stuck.
4. Upserts the outcome into `job_runs`: last start, last success, last error, consecutive failures, duration, run and skip counts. Recording never fails the job.

Health rules (`WithRules` to change them):

- **stale**: no success for two intervals of its schedule plus 10 minutes
- **failing**: 3 failed runs in a row
- **retired**: not started for 30 days (removed from the code), left out of the summary

`Handler()` answers `200 {"status":"ok","jobs":N,...}` or `503 {"status":"degraded","stale":S,"failing":F}` and never exposes job names or errors, so it is safe on a public route. `Statuses()` returns the full detail for internal tools.

Schedules are standard 5-field cron expressions or descriptors (`@every 10m`, `@hourly`, `@daily`).

## health

```go
router.GET("/health", gin.WrapH(health.Live()))                     // for the host's restart probe
router.GET("/health/db", gin.WrapH(health.DB(sqlDB, 3*time.Second))) // for the external uptime monitor
```

Keep the two apart: restarting a service does not fix a database outage, so the probe that triggers restarts should not depend on the database.

## Development

Tests that need Postgres run when `KIT_PG_DSN` is set:

```
docker run -d --rm -e POSTGRES_PASSWORD=test -p 55433:5432 postgres:16
KIT_PG_DSN='postgres://postgres:test@localhost:55433/postgres?sslmode=disable' go test -race ./...
```
