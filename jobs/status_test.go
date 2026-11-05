package jobs

import (
	"testing"
	"time"
)

func TestJudge(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)
	ago := func(d time.Duration) *time.Time { v := now.Add(-d); return &v }
	rules := Rules{}.withDefaults()
	cases := []struct {
		name string
		s    Status
		want State
	}{
		{"recent success", Status{Schedule: "* * * * *", LastSuccessAt: ago(time.Minute)}, StateOK},
		{"minute job silent for 13m", Status{Schedule: "* * * * *", LastSuccessAt: ago(13 * time.Minute)}, StateStale},
		{"weekly job six days ago", Status{Schedule: "0 4 * * 5", LastSuccessAt: ago(6 * 24 * time.Hour)}, StateOK},
		{"descriptor schedule", Status{Schedule: "@every 10m", LastSuccessAt: ago(31 * time.Minute)}, StateStale},
		{"three failures", Status{Schedule: "*/5 * * * *", LastSuccessAt: ago(time.Minute), ConsecutiveFailures: 3}, StateFailing},
		{"two failures", Status{Schedule: "*/5 * * * *", LastSuccessAt: ago(time.Minute), ConsecutiveFailures: 2}, StateOK},
		{"never succeeded yet", Status{Schedule: "* * * * *", ConsecutiveFailures: 1}, StateOK},
		{"unparsable schedule", Status{Schedule: "whenever", LastSuccessAt: ago(48 * time.Hour)}, StateOK},
	}
	for _, c := range cases {
		c.s.LastStartedAt = now.Add(-time.Minute)
		if got := rules.judge(c.s, now); got != c.want {
			t.Errorf("%s: got %s, want %s", c.name, got, c.want)
		}
	}
	retired := Status{Schedule: "* * * * *", LastStartedAt: now.Add(-31 * 24 * time.Hour), LastSuccessAt: ago(31 * 24 * time.Hour)}
	if got := rules.judge(retired, now); got != StateRetired {
		t.Errorf("retired: got %s", got)
	}
}

func TestSummarizeSkipsRetired(t *testing.T) {
	s := Summarize([]Status{{State: StateOK}, {State: StateStale}, {State: StateFailing}, {State: StateRetired}})
	if s.Jobs != 3 || s.Stale != 1 || s.Failing != 1 || s.Healthy() {
		t.Fatalf("got %+v", s)
	}
	if !Summarize([]Status{{State: StateOK}, {State: StateRetired}}).Healthy() {
		t.Fatal("ok + retired should be healthy")
	}
}

func TestWithTableRejectsInjection(t *testing.T) {
	if _, err := New(nil); err == nil {
		t.Fatal("nil db accepted")
	}
	g := &Guard{}
	if err := WithTable("job_runs; drop table users")(g); err == nil {
		t.Fatal("invalid table name accepted")
	}
	if err := WithTable("my_job_runs")(g); err != nil || g.table != "my_job_runs" {
		t.Fatalf("valid table name rejected: %v", err)
	}
}

func TestInterval(t *testing.T) {
	now := time.Date(2026, 11, 6, 12, 0, 30, 0, time.UTC)
	for schedule, want := range map[string]time.Duration{
		"* * * * *":    time.Minute,
		"*/15 * * * *": 15 * time.Minute,
		"0 4 * * 1":    7 * 24 * time.Hour,
		"@hourly":      time.Hour,
	} {
		got, err := Interval(schedule, now)
		if err != nil || got != want {
			t.Errorf("%s: got %v %v, want %v", schedule, got, err, want)
		}
	}
}
