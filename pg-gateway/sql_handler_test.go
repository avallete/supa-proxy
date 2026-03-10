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
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// --- integration helpers ---

// startTestPG launches a PostgreSQL container and returns a connStr.
// Requires Docker. Gated by -tags integration.
func startTestPG(tb testing.TB) string {
	tb.Helper()

	ctx := context.Background()

	req := testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16-alpine",
			Env: map[string]string{
				"POSTGRES_PASSWORD": "testpass",
				"POSTGRES_USER":     "testuser",
				"POSTGRES_DB":       "testdb",
			},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor:   wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
		},
		Started: true,
	}
	pgC, err := testcontainers.GenericContainer(ctx, req)
	if err != nil {
		tb.Fatalf("start postgres container: %v", err)
	}
	tb.Cleanup(func() { pgC.Terminate(ctx) })

	host, err := pgC.Host(ctx)
	if err != nil {
		tb.Fatalf("container host: %v", err)
	}
	port, err := pgC.MappedPort(ctx, "5432/tcp")
	if err != nil {
		tb.Fatalf("container port: %v", err)
	}

	return fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
}

// cfgFromConnStr builds a *Config from a postgres:// connStr.
func cfgFromConnStr(tb testing.TB, connStr, secret string) *Config {
	tb.Helper()
	c, err := pgx.ParseConfig(connStr)
	if err != nil {
		tb.Fatalf("parse connStr: %v", err)
	}
	return &Config{
		JWTSecret:        secret,
		PGHost:           c.Host,
		PGPort:           int(c.Port),
		PGUser:           c.User,
		PGPassword:       c.Password,
		PGDatabase:       c.Database,
		StatementTimeout: 30 * 1e9, // 30s
		MaxFieldBytes:    1048576,
		MaxMemory:        10485760,
		CursorBatchSize:  100,
		AllowedOrigins:   "*",
	}
}

// postSQL sends a POST /sql request to the given handler and returns the NDJSON lines.
func postSQL(t *testing.T, cfg *Config, query string) []map[string]json.RawMessage {
	t.Helper()
	token := mintTestJWT(testSecret, "testuser")
	body := fmt.Sprintf(`{"query":%q}`, query)

	r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleSQL(w, r, cfg, nil)

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

// --- tests ---

func TestStreamSelect_RowByRow(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	lines := postSQL(t, cfg, "SELECT i FROM generate_series(1, 250) i")

	// First line: columns
	require.Greater(t, len(lines), 0)
	_, hasColumns := lines[0]["columns"]
	require.True(t, hasColumns, "first line must be a columns message")

	// Middle lines: rows
	rowLines := 0
	for _, l := range lines[1:] {
		if _, ok := l["row"]; ok {
			rowLines++
		}
	}
	assert.Equal(t, 250, rowLines)

	// Last line: complete
	last := lines[len(lines)-1]
	rawComplete, ok := last["complete"]
	require.True(t, ok, "last line must be a complete message")

	var info completeInfo
	require.NoError(t, json.Unmarshal(rawComplete, &info))
	assert.Equal(t, int64(250), info.RowCount)
}

func TestStreamSelect_LargeField(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxFieldBytes = 1024

	lines := postSQL(t, cfg, "SELECT repeat('x', 5*1024*1024) AS big_col")

	require.Len(t, lines, 3) // columns + row + complete

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))

	var val string
	require.NoError(t, json.Unmarshal(row["big_col"], &val))
	assert.True(t, strings.HasPrefix(val, "[truncated:"), "expected truncated prefix, got: %s", val[:min(50, len(val))])
}

func TestStreamSelect_LargeField_NoLimit(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxFieldBytes = 0 // unlimited

	lines := postSQL(t, cfg, "SELECT repeat('x', 1024) AS col")

	require.Len(t, lines, 3)

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))

	var val string
	require.NoError(t, json.Unmarshal(row["col"], &val))
	assert.Equal(t, 1024, len(val))
	assert.False(t, strings.HasPrefix(val, "[truncated:"))
}

func TestExecDML_WithReturning(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	// Create table
	postSQL(t, cfg, "CREATE TABLE IF NOT EXISTS _pgw_test_returning (id SERIAL PRIMARY KEY, v TEXT)")
	t.Cleanup(func() {
		postSQL(t, cfg, "DROP TABLE IF EXISTS _pgw_test_returning")
	})

	lines := postSQL(t, cfg, "INSERT INTO _pgw_test_returning (v) VALUES ('hello') RETURNING id, v")

	// columns + row + complete
	require.GreaterOrEqual(t, len(lines), 3)

	// First line: columns with "id" and "v"
	var cols columnMsg
	require.NoError(t, json.Unmarshal(lines[0]["columns"], &cols.Columns))
	names := make([]string, len(cols.Columns))
	for i, c := range cols.Columns {
		names[i] = c.Name
	}
	assert.Contains(t, names, "id")
	assert.Contains(t, names, "v")

	// Second line: row with id set
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
	_, hasID := row["id"]
	assert.True(t, hasID, "RETURNING row must contain 'id'")

	// Last line: complete
	last := lines[len(lines)-1]
	_, hasComplete := last["complete"]
	assert.True(t, hasComplete)
}

// TestExecDML_CTEWithReturning is a regression test for the bug where
// WITH cte AS (...) INSERT ... RETURNING was mis-classified as a SELECT
// (due to "WITH" prefix matching), causing PostgreSQL to reject a
// DECLARE CURSOR on an INSERT statement.
func TestExecDML_CTEWithReturning(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	postSQL(t, cfg, "CREATE TABLE IF NOT EXISTS _pgw_test_cte_ret (id SERIAL PRIMARY KEY)")
	t.Cleanup(func() {
		postSQL(t, cfg, "DROP TABLE IF EXISTS _pgw_test_cte_ret")
	})

	// CTE wrapping an INSERT with RETURNING — previously mis-routed to streamSelect.
	lines := postSQL(t, cfg, `WITH cte AS (SELECT 1) INSERT INTO _pgw_test_cte_ret DEFAULT VALUES RETURNING id`)

	// columns + row + complete
	require.GreaterOrEqual(t, len(lines), 3)

	// First line: columns with "id"
	var cols columnMsg
	require.NoError(t, json.Unmarshal(lines[0]["columns"], &cols.Columns))
	names := make([]string, len(cols.Columns))
	for i, c := range cols.Columns {
		names[i] = c.Name
	}
	assert.Contains(t, names, "id", "columns message must include 'id'")

	// Second line: row with id set
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
	_, hasID := row["id"]
	assert.True(t, hasID, "RETURNING row must contain 'id'")

	// No error line
	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr, "unexpected error line: %v", l)
	}
}

func TestExecDML_Plain(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	postSQL(t, cfg, "CREATE TABLE IF NOT EXISTS _pgw_test_plain (id SERIAL)")
	t.Cleanup(func() { postSQL(t, cfg, "DROP TABLE IF EXISTS _pgw_test_plain") })

	lines := postSQL(t, cfg, "DELETE FROM _pgw_test_plain")

	// columns (empty) + complete — no row lines
	require.Len(t, lines, 2)
	_, hasColumns := lines[0]["columns"]
	assert.True(t, hasColumns)
	_, hasComplete := lines[1]["complete"]
	assert.True(t, hasComplete)
}

func TestExecDML_Error(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	lines := postSQL(t, cfg, "SELECT * FROM this_table_does_not_exist_xyz_abc")

	require.GreaterOrEqual(t, len(lines), 1)
	_, hasError := lines[len(lines)-1]["error"]
	assert.True(t, hasError, "expected error message in last line")
}

func TestSQL_ReadOnly(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	// Request body with readonly flag
	token := mintTestJWT(testSecret, "reader")
	body := `{"query":"CREATE TABLE _pgw_test_ro (id INT)","readonly":true}`
	r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handleSQL(w, r, cfg, nil)

	scanner := bufio.NewScanner(w.Body)
	var lastLine map[string]json.RawMessage
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		json.Unmarshal(line, &lastLine)
	}

	_, hasError := lastLine["error"]
	assert.True(t, hasError, "write in readonly mode must return an error")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
