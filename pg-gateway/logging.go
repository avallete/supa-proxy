package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type contextKey int

const requestIDKey contextKey = 0

// withRequestID extracts or generates a request ID and stores it in the context.
func withRequestID(ctx context.Context, r *http.Request) context.Context {
	id := r.Header.Get("X-Request-ID")
	if id == "" {
		b := make([]byte, 4)
		rand.Read(b)
		id = hex.EncodeToString(b)
	}
	return context.WithValue(ctx, requestIDKey, id)
}

// requestID retrieves the request ID from context.
func requestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// logQuery logs a completed query. Slow queries (>1s) are always logged at Info.
// Errors are logged at Error.
func logQuery(ctx context.Context, query string, duration time.Duration, rows int64, err error) {
	q := query
	if len(q) > 200 {
		q = q[:200] + "..."
	}
	attrs := []any{
		"request_id", requestID(ctx),
		"duration_ms", float64(duration.Microseconds()) / 1000.0,
		"rows", rows,
		"query", q,
	}
	if err != nil {
		slog.ErrorContext(ctx, "query failed", append(attrs, "error", err.Error())...)
	} else if duration > time.Second {
		slog.InfoContext(ctx, "slow query", attrs...)
	}
}

// logStream logs stream completion.
func logStream(ctx context.Context, rows int64, truncatedFields int, duration time.Duration) {
	slog.InfoContext(ctx, "stream complete",
		"request_id", requestID(ctx),
		"rows", rows,
		"truncated_fields", truncatedFields,
		"duration_ms", float64(duration.Microseconds())/1000.0,
	)
}

// authFailureReason extracts a label-friendly reason from an auth error.
func authFailureReason(err error) string {
	if errors.Is(err, jwt.ErrTokenExpired) {
		return "expired_token"
	}
	msg := err.Error()
	if len(msg) > 7 && msg[:7] == "missing" {
		return "missing_header"
	}
	return "invalid_token"
}
