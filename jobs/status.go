package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/robfig/cron/v3"
)

// State is a job's health as judged by the Guard's Rules.
type State string

const (
	StateOK      State = "ok"
	StateStale   State = "stale"   // no success for two intervals plus the margin
	StateFailing State = "failing" // failed FailingAfter times in a row
	StateRetired State = "retired" // not started for RetiredAfter; the job is gone
)

// Rules decide when a job is unhealthy. Zero fields take the defaults.
type Rules struct {
	// A job is stale once it has gone two of its intervals plus StaleMargin
	// without a successful run. Default 10 minutes.
	StaleMargin time.Duration
	// A job is failing after this many failed runs in a row. Default 3.
	FailingAfter int
	// A job that has not even started for this long was removed from the
	// code and is left out of the health summary. Default 30 days.
	RetiredAfter time.Duration
}

func (r Rules) withDefaults() Rules {
	if r.StaleMargin <= 0 {
		r.StaleMargin = 10 * time.Minute
	}
	if r.FailingAfter <= 0 {
		r.FailingAfter = 3
	}
	if r.RetiredAfter <= 0 {
		r.RetiredAfter = 30 * 24 * time.Hour
	}
	return r
}

// Status is the recorded state of one job.
type Status struct {
	Name                string        `json:"name"`
	Schedule            string        `json:"schedule"`
	State               State         `json:"state"`
	LastStartedAt       time.Time     `json:"last_started_at"`
	LastSuccessAt       *time.Time    `json:"last_success_at,omitempty"`
	LastErrorAt         *time.Time    `json:"last_error_at,omitempty"`
	LastError           string        `json:"last_error,omitempty"`
	LastDuration        time.Duration `json:"last_duration_ns"`
	ConsecutiveFailures int64         `json:"consecutive_failures"`
	Runs                int64         `json:"runs"`
	SkippedLocked       int64         `json:"skipped_locked"`
}

var scheduleParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// Interval is the time between two consecutive runs of schedule after now.
func Interval(schedule string, now time.Time) (time.Duration, error) {
	s, err := scheduleParser.Parse(schedule)
	if err != nil {
		return 0, err
	}
	first := s.Next(now)
	return s.Next(first).Sub(first), nil
}

// judge applies the rules to one recorded job.
func (r Rules) judge(s Status, now time.Time) State {
	if now.Sub(s.LastStartedAt) > r.RetiredAfter {
		return StateRetired
	}
	if s.ConsecutiveFailures >= int64(r.FailingAfter) {
		return StateFailing
	}
	if s.LastSuccessAt == nil {
		return StateOK
	}
	interval, err := Interval(s.Schedule, now)
	if err != nil {
		return StateOK
	}
	if now.Sub(*s.LastSuccessAt) > 2*interval+r.StaleMargin {
		return StateStale
	}
	return StateOK
}

// Statuses reads every recorded job and judges it at now. A missing table
// reads as no jobs.
func (g *Guard) Statuses(ctx context.Context, now time.Time) ([]Status, error) {
	var exists bool
	if err := g.db.QueryRowContext(ctx, "SELECT to_regclass($1) IS NOT NULL", g.table).Scan(&exists); err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	rows, err := g.db.QueryContext(ctx, fmt.Sprintf(`SELECT name, schedule, last_started_at, last_success_at,
	last_error_at, last_error, last_duration_ms, consecutive_failures, runs, skipped_locked
FROM %s ORDER BY name`, g.table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Status
	for rows.Next() {
		var s Status
		var ms int64
		if err := rows.Scan(&s.Name, &s.Schedule, &s.LastStartedAt, &s.LastSuccessAt,
			&s.LastErrorAt, &s.LastError, &ms, &s.ConsecutiveFailures, &s.Runs, &s.SkippedLocked); err != nil {
			return nil, err
		}
		s.LastDuration = time.Duration(ms) * time.Millisecond
		s.State = g.rules.judge(s, now)
		out = append(out, s)
	}
	return out, rows.Err()
}

// Summary counts jobs by state.
type Summary struct {
	Jobs    int `json:"jobs"`
	Stale   int `json:"stale"`
	Failing int `json:"failing"`
}

// Healthy is true when no job is stale or failing.
func (s Summary) Healthy() bool { return s.Stale == 0 && s.Failing == 0 }

// Summarize counts statuses, leaving retired jobs out.
func Summarize(statuses []Status) Summary {
	var s Summary
	for _, st := range statuses {
		switch st.State {
		case StateRetired:
			continue
		case StateStale:
			s.Stale++
		case StateFailing:
			s.Failing++
		}
		s.Jobs++
	}
	return s
}

// Handler serves the health summary: 200 {"status":"ok",...} when every job
// is healthy, 503 {"status":"degraded",...} otherwise, and 503
// {"status":"db_unavailable"} when the table cannot be read. It reports
// counts only, never job names or errors, so it is safe to expose publicly.
func (g *Guard) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		statuses, err := g.Statuses(ctx, time.Now())
		if err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "db_unavailable"})
			return
		}
		sum := Summarize(statuses)
		body := struct {
			Status string `json:"status"`
			Summary
		}{"ok", sum}
		if !sum.Healthy() {
			body.Status = "degraded"
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(body)
	})
}
