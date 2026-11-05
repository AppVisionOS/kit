// Package jobs makes scheduled jobs safe to run from several processes and
// visible when they stop working.
//
// A Guard wraps each run of a job. Before the job starts it takes a Postgres
// advisory lock named after the job, so a job never runs twice at the same
// time, whether the second run comes from another process or from the same
// scheduler firing again while the previous run is still going. After the
// run it records the outcome in a table (job_runs by default): last start,
// last success, last error, consecutive failures, duration and skips.
// Statuses and Handler turn that table into a health report, so a job that
// silently stopped succeeding shows up even when it never logs an error.
//
// The Guard is scheduler-agnostic: call Run from gocron, robfig/cron, a
// time.Ticker or anything else.
package jobs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"regexp"
	"sync"
	"time"
)

// ErrSkipped is returned by Run when another run of the same job holds the
// lock. The job body was not called.
var ErrSkipped = errors.New("jobs: another run of this job holds the lock")

// Guard runs jobs under a per-job lock and records every run.
type Guard struct {
	db        *sql.DB
	table     string
	namespace int32
	hooks     Hooks
	rules     Rules
	logf      func(format string, args ...any)

	mu      sync.Mutex
	lastLog map[string]time.Time
}

// Hooks let a service attach its own monitoring, such as cron check-ins in an
// error tracker. Every field is optional.
type Hooks struct {
	// Start is called after the lock is taken, right before the job body.
	Start func(name, schedule string)
	// Finish is called after the run has been recorded. err is the job's
	// error, or an error wrapping the panic value if the job panicked.
	Finish func(name, schedule string, took time.Duration, err error)
	// Skip is called when a run is skipped because the lock is held.
	Skip func(name string)
}

// Option configures a Guard.
type Option func(*Guard) error

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// WithTable sets the table runs are recorded in. Default "job_runs".
func WithTable(name string) Option {
	return func(g *Guard) error {
		if !identifier.MatchString(name) {
			return fmt.Errorf("jobs: invalid table name %q", name)
		}
		g.table = name
		return nil
	}
}

// WithLockNamespace sets the first key of every advisory lock the Guard
// takes, keeping its locks apart from other advisory lock users in the same
// database. Default 7275.
func WithLockNamespace(ns int32) Option {
	return func(g *Guard) error { g.namespace = ns; return nil }
}

// WithHooks attaches monitoring callbacks.
func WithHooks(h Hooks) Option {
	return func(g *Guard) error { g.hooks = h; return nil }
}

// WithRules overrides the thresholds used by Statuses and Handler.
func WithRules(r Rules) Option {
	return func(g *Guard) error { g.rules = r.withDefaults(); return nil }
}

// WithLogger sets where the Guard reports its own problems (a failed lock or
// record), at most once a minute per job and kind. Default log.Printf.
func WithLogger(logf func(format string, args ...any)) Option {
	return func(g *Guard) error {
		if logf == nil {
			return errors.New("jobs: nil logger")
		}
		g.logf = logf
		return nil
	}
}

// New returns a Guard over db, which must be a Postgres database.
func New(db *sql.DB, opts ...Option) (*Guard, error) {
	if db == nil {
		return nil, errors.New("jobs: nil db")
	}
	g := &Guard{
		db:        db,
		table:     "job_runs",
		namespace: 7275,
		rules:     Rules{}.withDefaults(),
		logf:      log.Printf,
		lastLog:   map[string]time.Time{},
	}
	for _, opt := range opts {
		if err := opt(g); err != nil {
			return nil, err
		}
	}
	return g, nil
}

// Run runs fn under the job's lock and records the outcome. It returns
// ErrSkipped without calling fn when another run holds the lock, the error
// from taking the lock if the database is unreachable, and otherwise fn's
// error. If fn panics, the run is recorded as failed, the lock is released
// and the panic continues.
//
// schedule is the job's cron expression ("*/5 * * * *", "@every 10m",
// "@daily", ...). It is stored with the run and used to decide when the job
// counts as stale.
func (g *Guard) Run(ctx context.Context, name, schedule string, fn func(context.Context) error) (err error) {
	release, acquired, err := g.lock(ctx, name)
	if err != nil {
		g.logThrottled(name, "lock", err)
		return fmt.Errorf("jobs: lock %s: %w", name, err)
	}
	if !acquired {
		g.recordSkip(name)
		if g.hooks.Skip != nil {
			g.hooks.Skip(name)
		}
		return ErrSkipped
	}
	if g.hooks.Start != nil {
		g.hooks.Start(name, schedule)
	}
	started := time.Now()
	finished := false
	defer func() {
		if finished {
			return
		}
		// fn panicked.
		p := recover()
		release()
		perr := fmt.Errorf("panic: %v", p)
		g.finish(name, schedule, started, perr)
		panic(p)
	}()
	err = fn(ctx)
	finished = true
	release()
	g.finish(name, schedule, started, err)
	return err
}

func (g *Guard) finish(name, schedule string, started time.Time, err error) {
	ended := time.Now()
	g.recordRun(name, schedule, started, ended, err)
	if g.hooks.Finish != nil {
		g.hooks.Finish(name, schedule, ended.Sub(started), err)
	}
}

func (g *Guard) logThrottled(name, what string, err error) {
	g.mu.Lock()
	key := name + "/" + what
	last, seen := g.lastLog[key]
	if seen && time.Since(last) < time.Minute {
		g.mu.Unlock()
		return
	}
	g.lastLog[key] = time.Now()
	g.mu.Unlock()
	g.logf("jobs: %s: %s failed: %v", name, what, err)
}
