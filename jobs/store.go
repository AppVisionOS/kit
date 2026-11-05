package jobs

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"
)

// lock takes a session-level advisory lock for the job on a connection of its
// own. The connection never goes back to the pool: release discards it, which
// ends the session and drops the lock even if no unlock reaches the server,
// and a process that dies drops its locks with its connections.
func (g *Guard) lock(ctx context.Context, name string) (release func(), acquired bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, err := g.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}
	var ok bool
	err = conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1::int, hashtext($2::text))", g.namespace, name).Scan(&ok)
	if err != nil {
		discard(conn)
		return nil, false, err
	}
	if !ok {
		// Holding no lock, so the connection can go back to the pool.
		conn.Close()
		return nil, false, nil
	}
	return func() { discard(conn) }, true, nil
}

func discard(conn *sql.Conn) {
	_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	_ = conn.Close()
}

// Migrate creates the runs table if it does not exist. It is safe to call
// from every process at startup.
func (g *Guard) Migrate(ctx context.Context) error {
	tx, err := g.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Two processes creating the same table at once can collide on the
	// catalog; serialize them.
	if _, err := tx.ExecContext(ctx, "SELECT pg_advisory_xact_lock($1::int, hashtext($2::text))", g.namespace, "migrate:"+g.table); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	name                 text        PRIMARY KEY,
	schedule             text        NOT NULL,
	last_started_at      timestamptz NOT NULL,
	last_finished_at     timestamptz NOT NULL,
	last_duration_ms     bigint      NOT NULL,
	last_success_at      timestamptz,
	last_error_at        timestamptz,
	last_error           text        NOT NULL DEFAULT '',
	consecutive_failures bigint      NOT NULL DEFAULT 0,
	runs                 bigint      NOT NULL DEFAULT 0,
	skipped_locked       bigint      NOT NULL DEFAULT 0
)`, g.table)); err != nil {
		return err
	}
	return tx.Commit()
}

// recordRun never fails the job: a missing table or a database hiccup is
// only logged.
func (g *Guard) recordRun(name, schedule string, started, finished time.Time, runErr error) {
	var successAt, errorAt *time.Time
	errText := ""
	if runErr == nil {
		successAt = &finished
	} else {
		errorAt = &finished
		errText = truncate(runErr.Error(), 500)
	}
	failures := 0
	if runErr != nil {
		failures = 1
	}
	q := fmt.Sprintf(`
INSERT INTO %[1]s (name, schedule, last_started_at, last_finished_at, last_duration_ms,
	last_success_at, last_error_at, last_error, consecutive_failures, runs)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 1)
ON CONFLICT (name) DO UPDATE SET
	schedule = EXCLUDED.schedule,
	last_started_at = EXCLUDED.last_started_at,
	last_finished_at = EXCLUDED.last_finished_at,
	last_duration_ms = EXCLUDED.last_duration_ms,
	last_success_at = COALESCE(EXCLUDED.last_success_at, %[1]s.last_success_at),
	last_error_at = COALESCE(EXCLUDED.last_error_at, %[1]s.last_error_at),
	last_error = CASE WHEN EXCLUDED.last_error_at IS NULL THEN %[1]s.last_error ELSE EXCLUDED.last_error END,
	consecutive_failures = CASE WHEN EXCLUDED.last_error_at IS NULL THEN 0 ELSE %[1]s.consecutive_failures + 1 END,
	runs = %[1]s.runs + 1`, g.table)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := g.db.ExecContext(ctx, q, name, schedule, started, finished,
		finished.Sub(started).Milliseconds(), successAt, errorAt, errText, failures); err != nil {
		g.logThrottled(name, "record run", err)
	}
}

func (g *Guard) recordSkip(name string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := fmt.Sprintf("UPDATE %s SET skipped_locked = skipped_locked + 1 WHERE name = $1", g.table)
	if _, err := g.db.ExecContext(ctx, q, name); err != nil {
		g.logThrottled(name, "record skip", err)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
