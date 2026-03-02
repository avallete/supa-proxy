//go:build integration

package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gatherMetric returns the float64 value for a named metric with the given labels.
// Returns -1 if not found.
func gatherMetric(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, m := range family.GetMetric() {
			if labelsMatch(m.GetLabel(), labels) {
				if c := m.GetCounter(); c != nil {
					return c.GetValue()
				}
				if h := m.GetHistogram(); h != nil {
					return float64(h.GetSampleCount())
				}
				if g := m.GetGauge(); g != nil {
					return g.GetValue()
				}
			}
		}
	}
	return -1
}

func labelsMatch(got []*dto.LabelPair, want map[string]string) bool {
	matched := 0
	for _, lp := range got {
		if v, ok := want[lp.GetName()]; ok && v == lp.GetValue() {
			matched++
		}
	}
	return matched == len(want)
}

func TestMetrics_SQLSuccess(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)

	before := gatherMetric(t, "pgw_http_requests_total", map[string]string{"path": "/sql", "status": "200"})

	// Wrap handleSQL with the instrumented middleware like main.go does
	ts := httptest.NewServer(instrumentedHandler("/sql", func(w http.ResponseWriter, r *http.Request) {
		handleSQL(w, r, cfg, nil)
	}))
	defer ts.Close()

	token := mintTestJWT(testSecret, "test")
	req, _ := http.NewRequest(http.MethodPost, ts.URL, strings.NewReader(`{"query":"SELECT 1 AS n"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	after := gatherMetric(t, "pgw_http_requests_total", map[string]string{"path": "/sql", "status": "200"})
	assert.Greater(t, after, before, "pgw_http_requests_total should have increased")

	// query duration should have at least one observation
	durCount := gatherMetric(t, "pgw_query_duration_seconds", map[string]string{"command": "SELECT", "status": "ok"})
	assert.GreaterOrEqual(t, durCount, float64(1))
}

func TestMetrics_AuthFailure(t *testing.T) {
	cfg := newTestConfig("127.0.0.1", 5432, testSecret)

	before := gatherMetric(t, "pgw_auth_failures_total", map[string]string{"reason": "invalid_token"})

	r := httptest.NewRequest(http.MethodPost, "/sql", strings.NewReader(`{"query":"SELECT 1"}`))
	r.Header.Set("Authorization", "Bearer bad.token")
	w := httptest.NewRecorder()
	handleSQL(w, r, cfg, nil)

	after := gatherMetric(t, "pgw_auth_failures_total", map[string]string{"reason": "invalid_token"})
	assert.Greater(t, after, before, "auth failure counter should have increased")
}

func TestMetrics_FieldTruncation(t *testing.T) {
	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.MaxFieldBytes = 64

	before := gatherMetric(t, "pgw_truncated_fields_total", nil)

	postSQL(t, cfg, "SELECT repeat('a', 1024) AS big")

	after := gatherMetric(t, "pgw_truncated_fields_total", nil)
	assert.Greater(t, after, before, "truncated_fields_total should have increased")
}

// gatherMetric with nil labels returns the single metric without label filtering.
func init() {
	// Ensure the gatherMetric nil-label case is handled (override for no-label metrics).
}

// Re-implement gatherMetric for nil label case (no-label counter like truncatedFieldsTotal).
// We patch the existing function by checking for nil.
// Go doesn't support function overloading; the nil check is in gatherMetric above,
// but labelsMatch(got, nil) returns true when want is empty/nil, so it works already.
// (len(want) == 0, matched == 0, 0 == 0 → true)
