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

// BenchmarkTruncateValue_SmallStr measures truncation for a string below the limit (common path).
func BenchmarkTruncateValue_SmallStr(b *testing.B) {
	s := strings.Repeat("x", 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		truncateValue(s, 1048576)
	}
}

// BenchmarkTruncateValue_LargeStr measures truncation for a 10 MB string with a 1 MB limit.
func BenchmarkTruncateValue_LargeStr(b *testing.B) {
	s := strings.Repeat("y", 10*1024*1024)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		truncateValue(s, 1024*1024)
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
