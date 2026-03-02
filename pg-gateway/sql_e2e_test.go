//go:build integration

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// postSQLBody sends a POST /sql with an arbitrary JSON body and returns NDJSON lines.
func postSQLBody(t *testing.T, cfg *Config, body string) []map[string]json.RawMessage {
	t.Helper()
	token := mintTestJWT(testSecret, "testuser")

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

func TestSQL_DDL_CreateAlterDrop(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	tbl := "_pgw_e2e_ddl"

	// CREATE
	lines := postSQL(t, cfg, "CREATE TABLE "+tbl+" (id SERIAL PRIMARY KEY, name TEXT)")
	require.GreaterOrEqual(t, len(lines), 1)
	_, hasErr := lines[len(lines)-1]["error"]
	assert.False(t, hasErr, "CREATE TABLE should not error")

	t.Cleanup(func() { postSQL(t, cfg, "DROP TABLE IF EXISTS "+tbl) })

	// ALTER: add a column
	lines = postSQL(t, cfg, "ALTER TABLE "+tbl+" ADD COLUMN score INT DEFAULT 0")
	_, hasErr = lines[len(lines)-1]["error"]
	assert.False(t, hasErr, "ALTER TABLE should not error")

	// INSERT
	lines = postSQL(t, cfg, "INSERT INTO "+tbl+" (name, score) VALUES ('alice', 42)")
	_, hasErr = lines[len(lines)-1]["error"]
	assert.False(t, hasErr, "INSERT should not error")

	// SELECT — verify the new column is present
	lines = postSQL(t, cfg, "SELECT id, name, score FROM "+tbl)
	require.GreaterOrEqual(t, len(lines), 3, "expected columns + row + complete")
	var cols columnMsg
	require.NoError(t, json.Unmarshal(lines[0]["columns"], &cols.Columns))
	colNames := make([]string, len(cols.Columns))
	for i, c := range cols.Columns {
		colNames[i] = c.Name
	}
	assert.Contains(t, colNames, "score", "score column must be present after ALTER")

	// DROP
	lines = postSQL(t, cfg, "DROP TABLE "+tbl)
	_, hasErr = lines[len(lines)-1]["error"]
	assert.False(t, hasErr, "DROP TABLE should not error")
}

func TestSQL_Transaction_Commit(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	tbl := "_pgw_e2e_txcommit"
	postSQL(t, cfg, "CREATE TABLE "+tbl+" (id INT)")
	t.Cleanup(func() { postSQL(t, cfg, "DROP TABLE IF EXISTS "+tbl) })

	// Single query: BEGIN + INSERT + COMMIT
	lines := postSQL(t, cfg, "BEGIN; INSERT INTO "+tbl+" VALUES (1); COMMIT")
	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr, "transaction commit should not produce error lines")
	}

	// Verify row is present
	lines = postSQL(t, cfg, "SELECT count(*) AS n FROM "+tbl)
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
	var n int64
	require.NoError(t, json.Unmarshal(row["n"], &n))
	assert.Equal(t, int64(1), n)
}

func TestSQL_Transaction_Rollback(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	tbl := "_pgw_e2e_txrollback"
	postSQL(t, cfg, "CREATE TABLE "+tbl+" (id INT)")
	t.Cleanup(func() { postSQL(t, cfg, "DROP TABLE IF EXISTS "+tbl) })

	// Single query: BEGIN + INSERT + ROLLBACK
	lines := postSQL(t, cfg, "BEGIN; INSERT INTO "+tbl+" VALUES (1); ROLLBACK")
	for _, l := range lines {
		_, hasErr := l["error"]
		assert.False(t, hasErr, "transaction rollback should not produce error lines")
	}

	// Verify row was NOT committed
	lines = postSQL(t, cfg, "SELECT count(*) AS n FROM "+tbl)
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(lines[1]["row"], &row))
	var n int64
	require.NoError(t, json.Unmarshal(row["n"], &n))
	assert.Equal(t, int64(0), n)
}

func TestSQL_MaxRows_Enforced(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxRows = 5

	lines := postSQL(t, cfg, "SELECT i FROM generate_series(1,20) i")

	rowCount := 0
	for _, l := range lines {
		if _, ok := l["row"]; ok {
			rowCount++
		}
	}
	assert.Equal(t, 5, rowCount, "MaxRows=5 must limit output to 5 row lines")

	last := lines[len(lines)-1]
	rawComplete, ok := last["complete"]
	require.True(t, ok, "last line must be complete message")
	var info completeInfo
	require.NoError(t, json.Unmarshal(rawComplete, &info))
	assert.Equal(t, int64(5), info.RowCount)
}

func TestSQL_StatementTimeout(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.StatementTimeout = 200 * time.Millisecond

	// Rebuild connCfg with the new timeout — postSQL builds it via handleSQL
	lines := postSQL(t, cfg, "SELECT pg_sleep(5)")

	last := lines[len(lines)-1]
	_, hasErr := last["error"]
	assert.True(t, hasErr, "pg_sleep(5) with 200ms timeout must produce an error line")
}

func TestSQL_CursorBatchSize_One(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.CursorBatchSize = 1

	lines := postSQL(t, cfg, "SELECT i FROM generate_series(1,10) i")

	rowCount := 0
	for _, l := range lines {
		if _, ok := l["row"]; ok {
			rowCount++
		}
	}
	assert.Equal(t, 10, rowCount, "CursorBatchSize=1 must still deliver all 10 rows")
}

func TestSQL_Parallel_Streams(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	const workers = 5
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			lines := postSQL(t, cfg, "SELECT i FROM generate_series(1,100) i")
			rowCount := 0
			for _, l := range lines {
				if _, ok := l["row"]; ok {
					rowCount++
				}
			}
			assert.Equal(t, 100, rowCount)
			for _, l := range lines {
				_, hasErr := l["error"]
				assert.False(t, hasErr, "parallel streams must not produce errors")
			}
		}()
	}
	wg.Wait()
}

func TestSQL_TruncationFields_Reported(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxFieldBytes = 64

	lines := postSQL(t, cfg, "SELECT repeat('x', 1024) AS big_col")

	// columns + row + complete
	require.GreaterOrEqual(t, len(lines), 3)

	rowLine := lines[1]
	rawTruncated, hasTruncated := rowLine["truncated_fields"]
	require.True(t, hasTruncated, "row line must contain 'truncated_fields' key when truncation occurred")

	var truncatedFields []string
	require.NoError(t, json.Unmarshal(rawTruncated, &truncatedFields))
	assert.Contains(t, truncatedFields, "big_col")
}

func TestSQL_PerQuery_MaxFieldBytes_Zero(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxFieldBytes = 64 // default small limit

	// Override per-query to 0 (unlimited)
	lines := postSQLBody(t, cfg, `{"query":"SELECT repeat('x', 1024) AS big_col","max_field_bytes":0}`)

	require.GreaterOrEqual(t, len(lines), 3)

	rowLine := lines[1]
	_, hasTruncated := rowLine["truncated_fields"]
	assert.False(t, hasTruncated, "max_field_bytes=0 must disable truncation; truncated_fields must be absent")

	// Also verify the value is the full 1024 chars
	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rowLine["row"], &row))
	var val string
	require.NoError(t, json.Unmarshal(row["big_col"], &val))
	assert.Equal(t, 1024, len(val))
}

func TestSQL_PerQuery_MaxFieldBytes_Override(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxFieldBytes = 64 // default truncates at 64

	// Override to 1024 — value is 512 bytes, so should NOT be truncated
	lines := postSQLBody(t, cfg, `{"query":"SELECT repeat('x', 512) AS col","max_field_bytes":1024}`)

	require.GreaterOrEqual(t, len(lines), 3)

	rowLine := lines[1]
	_, hasTruncated := rowLine["truncated_fields"]
	assert.False(t, hasTruncated, "max_field_bytes=1024 must not truncate a 512-byte value")

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rowLine["row"], &row))
	var val string
	require.NoError(t, json.Unmarshal(row["col"], &val))
	assert.Equal(t, 512, len(val))
}
