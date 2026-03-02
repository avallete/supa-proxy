package main

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgw_http_request_duration_seconds",
		Help:    "HTTP request duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"path", "status"})

	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgw_http_requests_total",
		Help: "Total number of HTTP requests.",
	}, []string{"path", "status"})

	activeConnections = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "pgw_active_connections",
		Help: "Number of currently active connections.",
	}, []string{"type"})

	queryDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "pgw_query_duration_seconds",
		Help:    "Query duration in seconds.",
		Buckets: prometheus.DefBuckets,
	}, []string{"command", "status"})

	rowsProcessedTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgw_rows_processed_total",
		Help: "Total number of rows processed.",
	}, []string{"command"})

	truncatedFieldsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pgw_truncated_fields_total",
		Help: "Total number of field values truncated due to MAX_FIELD_BYTES limit.",
	})

	relayBytesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgw_relay_bytes_total",
		Help: "Total bytes relayed.",
	}, []string{"direction"})

	relayDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "pgw_relay_duration_seconds",
		Help:    "WebSocket relay session duration in seconds.",
		Buckets: prometheus.DefBuckets,
	})

	authFailuresTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgw_auth_failures_total",
		Help: "Total number of authentication failures.",
	}, []string{"reason"})

	dbErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pgw_db_errors_total",
		Help: "Total number of database errors.",
	}, []string{"error_type"})
)

// statusResponseWriter wraps http.ResponseWriter to capture the status code.
type statusResponseWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusResponseWriter) WriteHeader(status int) {
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// instrumentedHandler wraps an HTTP handler to record request duration and count.
func instrumentedHandler(path string, fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusResponseWriter{ResponseWriter: w, status: http.StatusOK}
		fn(sw, r)
		dur := time.Since(start)
		status := strconv.Itoa(sw.status)
		httpRequestDuration.WithLabelValues(path, status).Observe(dur.Seconds())
		httpRequestsTotal.WithLabelValues(path, status).Inc()
	}
}
