//go:build integration

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gorilla/websocket"
)

var wsDialer = websocket.DefaultDialer

// BenchmarkSQLHandler_10Rows benchmarks the full /sql handler with 10 rows (real PG).
func BenchmarkSQLHandler_10Rows(b *testing.B) {
	connStr := startTestPG(b)
	cfg := cfgFromConnStr(b, connStr, testSecret)
	token := mintTestJWT(testSecret, "bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := `{"query":"SELECT i FROM generate_series(1,10) i"}`
		r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handleSQL(w, r, cfg, nil)
		if w.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", w.Code)
		}
	}
}

// BenchmarkSQLHandler_1000Rows benchmarks 1000 rows through the streaming pipeline.
func BenchmarkSQLHandler_1000Rows(b *testing.B) {
	connStr := startTestPG(b)
	cfg := cfgFromConnStr(b, connStr, testSecret)
	token := mintTestJWT(testSecret, "bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := `{"query":"SELECT i, md5(i::text) AS h FROM generate_series(1,1000) i"}`
		r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handleSQL(w, r, cfg, nil)
	}
}

// BenchmarkSQLHandler_LargeField benchmarks a single row with a 5 MB text field.
func BenchmarkSQLHandler_LargeField(b *testing.B) {
	connStr := startTestPG(b)
	cfg := cfgFromConnStr(b, connStr, testSecret)
	cfg.MaxFieldBytes = 0 // no truncation
	token := mintTestJWT(testSecret, "bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := `{"query":"SELECT repeat('x', 5*1024*1024) AS big"}`
		r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handleSQL(w, r, cfg, nil)
	}
}

// BenchmarkSQLHandler_10000Rows benchmarks the full /sql handler with 10 000 rows (real PG).
func BenchmarkSQLHandler_10000Rows(b *testing.B) {
	connStr := startTestPG(b)
	cfg := cfgFromConnStr(b, connStr, testSecret)
	token := mintTestJWT(testSecret, "bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := `{"query":"SELECT i FROM generate_series(1,10000) i"}`
		r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handleSQL(w, r, cfg, nil)
		if w.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", w.Code)
		}
	}
}

// BenchmarkSQLHandler_1000Rows_LargeColumns benchmarks 1 000 rows × 10 KB columns (≈10 MB/iter, no truncation).
func BenchmarkSQLHandler_1000Rows_LargeColumns(b *testing.B) {
	connStr := startTestPG(b)
	cfg := cfgFromConnStr(b, connStr, testSecret)
	cfg.MaxFieldBytes = 0 // no truncation
	token := mintTestJWT(testSecret, "bench")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := `{"query":"SELECT i, repeat('x', 10240) AS data FROM generate_series(1,1000) i"}`
		r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		handleSQL(w, r, cfg, nil)
		if w.Code != http.StatusOK {
			b.Fatalf("unexpected status %d", w.Code)
		}
	}
}

// BenchmarkWSRelay_Handshake benchmarks WS connect+close with a mock TCP server.
func BenchmarkWSRelay_Handshake(b *testing.B) {
	mockAddr, _ := startMockPGServer(b)
	cfg := newTestConfig(addrHost(mockAddr), addrPort(mockAddr), testSecret)
	token := mintTestJWT(testSecret, "bench")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleWSRelay(w, r, cfg)
	}))
	b.Cleanup(ts.Close)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer "+token)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conn, _, err := wsDialer.Dial(wsURL, hdr)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		conn.Close()
	}
}
