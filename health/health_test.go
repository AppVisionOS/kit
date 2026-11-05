package health

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func code(h http.Handler) int {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
	return w.Code
}

func TestLive(t *testing.T) {
	if c := code(Live()); c != http.StatusOK {
		t.Fatalf("got %d", c)
	}
}

func TestDBUnreachable(t *testing.T) {
	db, err := sql.Open("pgx", "postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if c := code(DB(db, 2*time.Second)); c != http.StatusServiceUnavailable {
		t.Fatalf("got %d, want 503", c)
	}
}

func TestDBReachable(t *testing.T) {
	dsn := os.Getenv("KIT_PG_DSN")
	if dsn == "" {
		t.Skip("KIT_PG_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if c := code(DB(db, 3*time.Second)); c != http.StatusOK {
		t.Fatalf("got %d, want 200", c)
	}
}
