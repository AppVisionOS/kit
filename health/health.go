// Package health provides the two health endpoints every service needs: a
// liveness check that touches nothing and a database check for uptime
// monitors.
//
// Keep them separate. A host that restarts the service when the liveness
// check fails should not see a database outage there: restarting the service
// does not bring the database back. Point the external monitor at the
// database check instead.
package health

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

// Live answers 200 {"status":"ok"} without touching any dependency.
func Live() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		write(w, http.StatusOK, "ok")
	})
}

// DB answers 200 {"status":"ok"} when "SELECT 1" succeeds within timeout and
// 503 {"status":"db_unavailable"} otherwise.
func DB(db *sql.DB, timeout time.Duration) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		var one int
		if err := db.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil || one != 1 {
			write(w, http.StatusServiceUnavailable, "db_unavailable")
			return
		}
		write(w, http.StatusOK, "ok")
	})
}

func write(w http.ResponseWriter, code int, status string) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"status": status})
}
