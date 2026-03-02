package main

import (
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleWSRelay is the wsproxy-compatible WebSocket→TCP relay.
// It validates JWT, determines the target PG address, opens a TCP
// connection with server-side credentials, and does bidirectional
// zero-copy byte forwarding.
func handleWSRelay(w http.ResponseWriter, r *http.Request, cfg *Config) {
	ctx := r.Context()
	// --- JWT Auth ---
	claims, err := validateJWT(r, cfg.JWTSecret)
	if err != nil {
		authFailuresTotal.WithLabelValues(authFailureReason(err)).Inc()
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusUnauthorized)
		return
	}

	activeConnections.WithLabelValues("ws_relay").Inc()
	defer activeConnections.WithLabelValues("ws_relay").Dec()

	start := time.Now()
	defer func() {
		relayDuration.Observe(time.Since(start).Seconds())
	}()

	// --- Determine target address ---
	target, err := resolveTarget(r, cfg, claims)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusBadRequest)
		return
	}

	// --- Validate against allow regex ---
	if cfg.AllowAddrRegexp != nil && !cfg.AllowAddrRegexp.MatchString(target) {
		http.Error(w, fmt.Sprintf(`{"error":"target %q not allowed"}`, target), http.StatusForbidden)
		return
	}

	if cfg.LogTraffic {
		slog.Info("ws relay", "user", claimsSub(claims), "target", target)
	}

	// --- Open TCP connection to PG ---
	pgConn, err := net.Dial("tcp", target)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"cannot reach PG at %s: %v"}`, target, err), http.StatusBadGateway)
		return
	}
	defer pgConn.Close()

	// --- Upgrade HTTP to WebSocket ---
	wsConn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.Error("ws upgrade failed", "error", err)
		return
	}
	defer wsConn.Close()

	// --- Bidirectional relay ---
	// Two goroutines doing io.Copy between the WebSocket and the TCP connection.
	var wg sync.WaitGroup
	wg.Add(2)

	// WS → PG (client sends PG wire protocol messages)
	go func() {
		defer wg.Done()
		for {
			_, reader, err := wsConn.NextReader()
			if err != nil {
				slog.DebugContext(ctx, "ws→pg reader closed", "error", err)
				break
			}
			n, err := io.Copy(pgConn, reader)
			if n > 0 {
				relayBytesTotal.WithLabelValues("ws_to_pg").Add(float64(n))
			}
			if err != nil {
				slog.DebugContext(ctx, "ws→pg copy error", "error", err)
				break
			}
		}
		// Bug 3 fix: guard type assertion — underlying conn may not be *net.TCPConn
		// (e.g. when wrapped by a proxy or using Unix sockets).
		if tc, ok := pgConn.(*net.TCPConn); ok {
			tc.CloseWrite()
		}
	}()

	// PG → WS (PG sends wire protocol responses)
	go func() {
		defer wg.Done()
		buf := make([]byte, 32*1024) // 32KB read buffer
		for {
			n, err := pgConn.Read(buf)
			if n > 0 {
				relayBytesTotal.WithLabelValues("pg_to_ws").Add(float64(n))
				if writeErr := wsConn.WriteMessage(websocket.BinaryMessage, buf[:n]); writeErr != nil {
					break
				}
			}
			if err != nil && n == 0 {
				slog.DebugContext(ctx, "pg→ws read closed", "error", err)
				break
			}
		}
		wsConn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	}()

	wg.Wait()
}

// resolveTarget determines the PG host:port to connect to.
// Priority: APPEND_PORT env > JWT claims > ?address= query param
func resolveTarget(r *http.Request, cfg *Config, claims *JWTClaims) (string, error) {
	// Fixed target from config (single-DB deployment)
	if cfg.AppendPort != "" {
		return cfg.AppendPort, nil
	}

	// From JWT claims (multi-tenant: mgmt-api mints JWT with target)
	if claims.Host != "" {
		port := claims.Port
		if port == 0 {
			port = 5432
		}
		return fmt.Sprintf("%s:%d", claims.Host, port), nil
	}

	// From query param (wsproxy-compatible)
	addr := r.URL.Query().Get("address")
	if addr != "" {
		return addr, nil
	}

	// Default: local PG
	return fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort), nil
}

func claimsSub(c *JWTClaims) string {
	if c != nil && c.Subject != "" {
		return c.Subject
	}
	return "(anon)"
}
