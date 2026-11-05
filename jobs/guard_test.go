package jobs

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// These tests need a real Postgres, e.g.
// KIT_PG_DSN=postgres://postgres:test@localhost:55433/postgres?sslmode=disable
func testGuard(t *testing.T, opts ...Option) (*Guard, *sql.DB) {
	t.Helper()
	dsn := os.Getenv("KIT_PG_DSN")
	if dsn == "" {
		t.Skip("KIT_PG_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	table := "job_runs_" + strings.ToLower(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name()))
	if len(table) > 63 {
		table = table[:63]
	}
	if _, err := db.Exec("DROP TABLE IF EXISTS " + table); err != nil {
		t.Fatal(err)
	}
	g, err := New(db, append([]Option{WithTable(table)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP TABLE IF EXISTS " + table) })
	return g, db
}

func status(t *testing.T, g *Guard, name string) Status {
	t.Helper()
	all, err := g.Statuses(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range all {
		if s.Name == name {
			return s
		}
	}
	t.Fatalf("no status for %s", name)
	return Status{}
}

func TestLockIsExclusivePerJobAndReleased(t *testing.T) {
	g, _ := testGuard(t)
	ctx := context.Background()
	release, ok, err := g.lock(ctx, "a")
	if err != nil || !ok {
		t.Fatalf("first lock: %v %v", ok, err)
	}
	if _, ok, err := g.lock(ctx, "a"); err != nil || ok {
		t.Fatalf("second lock on a held job: ok=%v err=%v", ok, err)
	}
	if r, ok, err := g.lock(ctx, "b"); err != nil || !ok {
		t.Fatalf("other job blocked: ok=%v err=%v", ok, err)
	} else {
		r()
	}
	release()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r, ok, err := g.lock(ctx, "a")
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			r()
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("lock not released")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestOverlappingRunsAcrossGuardsRunOnce(t *testing.T) {
	g1, db := testGuard(t)
	g2, err := New(db, WithTable(g1.table))
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	started, finish := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		g1.Run(context.Background(), "nightly", "@daily", func(context.Context) error {
			calls.Add(1)
			close(started)
			<-finish
			return nil
		})
	}()
	<-started
	var skipped atomic.Int32
	g2.hooks.Skip = func(string) { skipped.Add(1) }
	err = g2.Run(context.Background(), "nightly", "@daily", func(context.Context) error {
		calls.Add(1)
		return nil
	})
	if !errors.Is(err, ErrSkipped) {
		t.Fatalf("second run: got %v, want ErrSkipped", err)
	}
	close(finish)
	wg.Wait()
	if calls.Load() != 1 || skipped.Load() != 1 {
		t.Fatalf("calls=%d skipped=%d", calls.Load(), skipped.Load())
	}
	// Once the row exists, skips are counted.
	release, _, _ := g1.lock(context.Background(), "nightly")
	g2.Run(context.Background(), "nightly", "@daily", func(context.Context) error { return nil })
	release()
	if s := status(t, g1, "nightly"); s.Runs != 1 || s.SkippedLocked != 1 || s.LastSuccessAt == nil {
		t.Fatalf("status %+v", s)
	}
}

func TestFailuresAndRecovery(t *testing.T) {
	var finished []error
	g, _ := testGuard(t, WithHooks(Hooks{Finish: func(_, _ string, _ time.Duration, err error) { finished = append(finished, err) }}))
	boom := errors.New("boom")
	for i := 0; i < 3; i++ {
		if err := g.Run(context.Background(), "sync", "*/5 * * * *", func(context.Context) error { return boom }); !errors.Is(err, boom) {
			t.Fatalf("run %d returned %v", i, err)
		}
	}
	s := status(t, g, "sync")
	if s.ConsecutiveFailures != 3 || s.State != StateFailing || s.LastError != "boom" || s.LastSuccessAt != nil {
		t.Fatalf("after failures: %+v", s)
	}
	if err := g.Run(context.Background(), "sync", "*/5 * * * *", func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	s = status(t, g, "sync")
	if s.ConsecutiveFailures != 0 || s.State != StateOK || s.Runs != 4 || s.LastSuccessAt == nil || s.LastError != "boom" {
		t.Fatalf("after recovery: %+v", s)
	}
	if len(finished) != 4 {
		t.Fatalf("finish hook calls = %d", len(finished))
	}
}

func TestPanicIsRecordedReleasedAndRethrown(t *testing.T) {
	g, _ := testGuard(t)
	func() {
		defer func() {
			if r := recover(); r != "kaboom" {
				t.Fatalf("recovered %v, want the original panic", r)
			}
		}()
		g.Run(context.Background(), "explode", "* * * * *", func(context.Context) error { panic("kaboom") })
	}()
	s := status(t, g, "explode")
	if s.ConsecutiveFailures != 1 || !strings.Contains(s.LastError, "kaboom") {
		t.Fatalf("status %+v", s)
	}
	ran := false
	deadline := time.Now().Add(5 * time.Second)
	for !ran && time.Now().Before(deadline) {
		err := g.Run(context.Background(), "explode", "* * * * *", func(context.Context) error { ran = true; return nil })
		if err != nil && !errors.Is(err, ErrSkipped) {
			t.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ran {
		t.Fatal("lock still held after the panic")
	}
}

func TestMigrateIsIdempotentAndConcurrent(t *testing.T) {
	g, _ := testGuard(t)
	if _, err := g.db.Exec("DROP TABLE " + g.table); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- g.Migrate(context.Background()) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestStatusesWithoutTable(t *testing.T) {
	g, _ := testGuard(t)
	g.db.Exec("DROP TABLE " + g.table)
	all, err := g.Statuses(context.Background(), time.Now())
	if err != nil || len(all) != 0 {
		t.Fatalf("got %v %v", all, err)
	}
}

func TestHandler(t *testing.T) {
	g, _ := testGuard(t)
	get := func() (int, map[string]any) {
		w := httptest.NewRecorder()
		g.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/jobs", nil))
		var body map[string]any
		json.Unmarshal(w.Body.Bytes(), &body)
		return w.Code, body
	}
	if code, body := get(); code != 200 || body["status"] != "ok" {
		t.Fatalf("empty: %d %v", code, body)
	}
	boom := errors.New("secret detail")
	for i := 0; i < 3; i++ {
		g.Run(context.Background(), "billing", "* * * * *", func(context.Context) error { return boom })
	}
	code, body := get()
	if code != 503 || body["status"] != "degraded" || body["failing"] != float64(1) {
		t.Fatalf("failing: %d %v", code, body)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), "billing") || strings.Contains(string(raw), "secret") {
		t.Fatalf("handler leaked job details: %s", raw)
	}
}
