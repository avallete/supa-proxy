package main

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// BenchmarkNDJSONSerialization measures the cost of marshalling a rowMsg.
func BenchmarkNDJSONSerialization(b *testing.B) {
	msg := rowMsg{Row: map[string]interface{}{
		"id":    42,
		"name":  "alice",
		"score": 3.14,
		"tags":  []string{"foo", "bar"},
	}}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(msg); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWriteJSONRow measures the cost of writing a NDJSON row from raw PG wire columns.
func BenchmarkWriteJSONRow(b *testing.B) {
	fields := []fieldDesc{
		{Name: "id", DataTypeOID: oidInt4},
		{Name: "name", DataTypeOID: 25},
		{Name: "score", DataTypeOID: oidFloat8},
	}
	columns := []wireColumn{
		{Data: []byte("42"), OriginalLen: 2},
		{Data: []byte("alice"), OriginalLen: 5},
		{Data: []byte("3.14"), OriginalLen: 4},
	}
	var buf strings.Builder
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		writeJSONRow(&buf, fields, columns)
	}
}

// BenchmarkJWTValidation measures JWT parse+validate throughput.
func BenchmarkJWTValidation(b *testing.B) {
	token := mintTestJWT(testSecret, "bench-user")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := httptest.NewRequest("GET", "/", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		if _, err := validateJWT(r, testSecret); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkIsSelectLike measures the hot path in query routing.
func BenchmarkIsSelectLike(b *testing.B) {
	queries := []string{
		"SELECT id, name FROM users WHERE active = true",
		"INSERT INTO events (type) VALUES ('login')",
		"WITH cte AS (SELECT 1) SELECT * FROM cte",
		"UPDATE users SET last_seen = now() WHERE id = 1",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := strings.ToUpper(queries[i%len(queries)])
		isSelectLike(q)
	}
}

// BenchmarkHasDMLReturning measures RETURNING detection throughput.
func BenchmarkHasDMLReturning(b *testing.B) {
	queries := []string{
		"INSERT INTO t (v) VALUES ('x') RETURNING id",
		"UPDATE t SET v = 'y' WHERE id = 1 RETURNING id, v",
		"DELETE FROM t WHERE id = 1",
		"SELECT * FROM t",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		q := strings.ToUpper(queries[i%len(queries)])
		hasDMLReturning(q)
	}
}

// BenchmarkClassifyQuery_Parser measures classifyQuery() using the multigres parser.
func BenchmarkClassifyQuery_Parser(b *testing.B) {
	queries := []string{
		"SELECT id, name FROM users WHERE active = true",
		"INSERT INTO events (type) VALUES ('login') RETURNING id",
		"WITH cte AS (SELECT 1) INSERT INTO t DEFAULT VALUES RETURNING id",
		"UPDATE users SET last_seen = now() WHERE id = 1",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		classifyQuery(queries[i%len(queries)])
	}
}

// BenchmarkClassifyQuery_StringMatch measures the legacy string-matching path
// (isSelectLike + hasDMLReturning) for the same 4 query types.
func BenchmarkClassifyQuery_StringMatch(b *testing.B) {
	queries := []string{
		"SELECT id, name FROM users WHERE active = true",
		"INSERT INTO events (type) VALUES ('login') RETURNING id",
		"WITH cte AS (SELECT 1) INSERT INTO t DEFAULT VALUES RETURNING id",
		"UPDATE users SET last_seen = now() WHERE id = 1",
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		upper := strings.ToUpper(queries[i%len(queries)])
		if isSelectLike(upper) {
			continue
		}
		hasDMLReturning(upper)
	}
}
