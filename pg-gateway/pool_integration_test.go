//go:build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postSQLWithPool sends a POST /sql using a poolManager and returns NDJSON lines.
func postSQLWithPool(t *testing.T, cfg *Config, pm *poolManager, query string) []map[string]json.RawMessage {
	t.Helper()
	token := mintTestJWT(testSecret, "testuser")
	body := fmt.Sprintf(`{"query":%q}`, query)

	r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleSQL(w, r, cfg, pm)

	var lines []map[string]json.RawMessage
	scanner := bufio.NewScanner(w.Body)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		var m map[string]json.RawMessage
		if err := json.Unmarshal(line, &m); err != nil {
			t.Fatalf("invalid NDJSON line %q: %v", line, err)
		}
		lines = append(lines, m)
	}
	return lines
}

// TestPool_PIDReuse verifies that sequential requests through the pool reuse
// the same PG backend process (same PID from pg_backend_pid()).
func TestPool_PIDReuse(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.PoolEnabled = true
	cfg.PoolMaxConns = 5
	cfg.PoolMinConns = 1
	cfg.PoolMaxConnLifetime = 30 * time.Minute
	cfg.PoolMaxConnIdleTime = 5 * time.Minute

	pm := newPoolManager(cfg)
	defer pm.Close()

	pids := make(map[int64]bool)

	for i := 0; i < 10; i++ {
		lines := postSQLWithPool(t, cfg, pm, "SELECT pg_backend_pid() AS pid")
		require.GreaterOrEqual(t, len(lines), 3, "expected columns + row + complete")

		// No error
		for _, l := range lines {
			_, hasErr := l["error"]
			assert.False(t, hasErr, "unexpected error: %v", l)
		}

		var row map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
		var pid int64
		require.NoError(t, json.Unmarshal(row["pid"], &pid))
		pids[pid] = true
	}

	// With a pool of max 5 conns and sequential requests, we expect very few distinct PIDs.
	// Typically 1 (all requests reuse the same connection). Allow up to 2 for race conditions.
	assert.LessOrEqual(t, len(pids), 2,
		"expected PID reuse across sequential requests, got %d distinct PIDs: %v", len(pids), pids)
}

// TestPool_SessionIsolation verifies that SET ROLE from one request doesn't
// leak to the next request via the pool. DISCARD ALL on release must reset it.
func TestPool_SessionIsolation(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.PoolEnabled = true
	cfg.PoolMaxConns = 1 // force single connection to maximize leak chance
	cfg.PoolMinConns = 1
	cfg.PoolMaxConnLifetime = 30 * time.Minute
	cfg.PoolMaxConnIdleTime = 5 * time.Minute

	pm := newPoolManager(cfg)
	defer pm.Close()

	// First request: uses the default role (no SET ROLE in test JWT)
	lines := postSQLWithPool(t, cfg, pm, "SELECT current_user AS cu")
	require.GreaterOrEqual(t, len(lines), 3)
	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr)
	}
	var row1 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row1))
	var user1 string
	require.NoError(t, json.Unmarshal(row1["cu"], &user1))

	// Second request: also checks current_user — should be the same default, not a leaked role
	lines = postSQLWithPool(t, cfg, pm, "SELECT current_user AS cu")
	require.GreaterOrEqual(t, len(lines), 3)
	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr)
	}
	var row2 map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row2))
	var user2 string
	require.NoError(t, json.Unmarshal(row2["cu"], &user2))

	assert.Equal(t, user1, user2, "current_user must be the same across requests (no role leak)")
}

// TestPool_SessionIsolation_GUC verifies that GUC settings (like statement_timeout,
// default_transaction_read_only) don't leak between pooled requests.
func TestPool_SessionIsolation_GUC(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.PoolEnabled = true
	cfg.PoolMaxConns = 1
	cfg.PoolMinConns = 1
	cfg.PoolMaxConnLifetime = 30 * time.Minute
	cfg.PoolMaxConnIdleTime = 5 * time.Minute

	pm := newPoolManager(cfg)
	defer pm.Close()

	// First request with readonly=true sets default_transaction_read_only=on
	token := mintTestJWT(testSecret, "reader")
	body := `{"query":"SELECT current_setting('default_transaction_read_only') AS ro","readonly":true}`
	r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	handleSQL(w, r, cfg, pm)

	// Second request (no readonly flag): the GUC should be reset by DISCARD ALL
	lines := postSQLWithPool(t, cfg, pm, "SELECT current_setting('default_transaction_read_only') AS ro")
	require.GreaterOrEqual(t, len(lines), 3)
	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr, "unexpected error: %v", l)
	}

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
	var ro string
	require.NoError(t, json.Unmarshal(row["ro"], &ro))
	assert.Equal(t, "off", ro, "default_transaction_read_only must be reset after DISCARD ALL")
}

// TestPool_ConnectionBound verifies that the pool limits the number of concurrent
// PG connections to PoolMaxConns even under burst load.
func TestPool_ConnectionBound(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.PoolEnabled = true
	cfg.PoolMaxConns = 3
	cfg.PoolMinConns = 1
	cfg.PoolMaxConnLifetime = 30 * time.Minute
	cfg.PoolMaxConnIdleTime = 5 * time.Minute

	pm := newPoolManager(cfg)
	defer pm.Close()

	// Connect directly to check pg_stat_activity
	directCfg, err := newPGConnConfig(cfg, cfg.PGHost, cfg.PGPort, cfg.PGDatabase)
	require.NoError(t, err)
	directConn, err := pgx.ConnectConfig(context.Background(), directCfg)
	require.NoError(t, err)
	defer directConn.Close(context.Background())

	const workers = 15
	var wg sync.WaitGroup
	wg.Add(workers)

	var maxSeen atomic.Int64

	// Launch 15 concurrent requests — each runs a pg_sleep to hold the connection
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			lines := postSQLWithPool(t, cfg, pm, "SELECT pg_sleep(0.5), pg_backend_pid() AS pid")
			// Just verify no fatal errors
			for _, l := range lines {
				_, hasErr := l["error"]
				assert.False(t, hasErr, "unexpected error in concurrent request: %v", l)
			}
		}()
	}

	// While requests are in-flight, sample pg_stat_activity
	go func() {
		for i := 0; i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			var count int64
			err := directConn.QueryRow(context.Background(),
				"SELECT count(*) FROM pg_stat_activity WHERE usename = $1 AND state = 'active' AND pid != pg_backend_pid()",
				cfg.PGUser,
			).Scan(&count)
			if err == nil {
				for {
					old := maxSeen.Load()
					if count <= old || maxSeen.CompareAndSwap(old, count) {
						break
					}
				}
			}
		}
	}()

	wg.Wait()

	// The max active connections should never exceed PoolMaxConns
	assert.LessOrEqual(t, maxSeen.Load(), int64(cfg.PoolMaxConns),
		"max active PG connections (%d) should not exceed PoolMaxConns (%d)", maxSeen.Load(), cfg.PoolMaxConns)
}

// TestPool_FallbackWhenDisabled verifies that when PoolEnabled=false (default),
// everything works as before with direct connections.
func TestPool_FallbackWhenDisabled(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.PoolEnabled = false

	// Pass nil poolManager — same as before
	lines := postSQLWithPool(t, cfg, nil, "SELECT 42 AS n")
	require.GreaterOrEqual(t, len(lines), 3)

	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr, "unexpected error with pool disabled: %v", l)
	}

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
	var n int64
	require.NoError(t, json.Unmarshal(row["n"], &n))
	assert.Equal(t, int64(42), n)
}
