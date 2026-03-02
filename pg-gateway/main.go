package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	cfg := loadConfig()

	// Init structured JSON logging (replaces log.Printf).
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})))

	slog.Info("pg-gateway starting",
		"listen", cfg.ListenPort,
		"metrics", cfg.MetricsPort,
		"region", cfg.Region,
		"pg_target", fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort),
		"db", cfg.PGDatabase,
		"max_field_bytes", cfg.MaxFieldBytes,
		"cursor_batch", cfg.CursorBatchSize,
		"statement_timeout", cfg.StatementTimeout.String(),
		"pool_enabled", cfg.PoolEnabled,
	)

	if cfg.JWTSecret == "" {
		slog.Warn("JWT_SECRET not set — all authenticated endpoints will reject requests")
	}

	// --- Connection pool ---
	var pm *poolManager
	if cfg.PoolEnabled {
		pm = newPoolManager(cfg)
		defer pm.Close()
		slog.Info("connection pooling enabled",
			"max_conns_per_target", cfg.PoolMaxConns,
			"min_conns_per_target", cfg.PoolMinConns,
			"max_conn_lifetime", cfg.PoolMaxConnLifetime.String(),
			"max_conn_idle_time", cfg.PoolMaxConnIdleTime.String(),
		)
	}

	// --- HTTP mux ---
	mux := http.NewServeMux()

	// Liveness probe (no auth)
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		addCORS(w, r, cfg)
		resp := map[string]string{"status": "ok", "version": "0.1.0"}
		if cfg.Region != "" {
			resp["region"] = cfg.Region
		}
		writeJSON(w, http.StatusOK, resp)
	})

	// Readiness probe — verifies PG connectivity (no auth)
	mux.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		addCORS(w, r, cfg)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		connCfg, err := newPGConnConfig(cfg, cfg.PGHost, cfg.PGPort, cfg.PGDatabase)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unavailable", "error": err.Error()})
			return
		}

		conn, err := pgx.ConnectConfig(ctx, connCfg)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"status": "unavailable", "error": err.Error()})
			return
		}
		conn.Close(ctx)

		writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	// CORS preflight + main routes
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			addCORS(w, r, cfg)
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// WS upgrade on /v1 (wsproxy-compatible path)
		if r.URL.Path == "/v1" || r.URL.Path == "/v1/" {
			instrumentedHandler("/v1", func(w http.ResponseWriter, r *http.Request) {
				handleWSRelay(w, r, cfg)
			})(w, r)
			return
		}

		// POST /sql — NDJSON streaming query endpoint
		if r.URL.Path == "/sql" {
			instrumentedHandler("/sql", func(w http.ResponseWriter, r *http.Request) {
				handleSQL(w, r, cfg, pm)
			})(w, r)
			return
		}

		http.NotFound(w, r)
	})

	// --- Pool metrics collector ---
	if pm != nil {
		go func() {
			ticker := time.NewTicker(15 * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					updatePoolMetrics(pm)
				}
			}
		}()
	}

	// --- Metrics server (separate port, like wsproxy) ---
	go func() {
		metricsMux := http.NewServeMux()
		metricsMux.Handle("/metrics", promhttp.Handler())
		slog.Info("metrics server starting", "addr", cfg.MetricsPort)
		metricsSrv := &http.Server{
			Addr:              cfg.MetricsPort,
			Handler:           metricsMux,
			ReadHeaderTimeout: 3 * time.Second,
		}
		if err := metricsSrv.ListenAndServe(); err != nil {
			slog.Error("metrics server error", "error", err)
		}
	}()

	// --- Main server ---
	slog.Info("pg-gateway ready", "addr", cfg.ListenPort)
	srv := &http.Server{
		Addr:              cfg.ListenPort,
		Handler:           mux,
		ReadHeaderTimeout: 3 * time.Second,
	}
	if err := srv.ListenAndServe(); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

// loadConfig reads env vars into the Config struct.
func loadConfig() *Config {
	cfg := &Config{}

	cfg.ListenPort = envOr("LISTEN_PORT", ":15432")
	cfg.MetricsPort = envOr("METRICS_PORT", ":2112")
	cfg.JWTSecret = envOr("JWT_SECRET", "")
	cfg.PGHost = envOr("PG_HOST", "127.0.0.1")
	cfg.PGPort = envInt("PG_PORT", 5432)
	cfg.PGUser = envOr("PG_USER", "postgres")
	cfg.PGPassword = envOr("PG_PASSWORD", "")
	cfg.PGDatabase = envOr("PG_DATABASE", "postgres")
	cfg.AllowAddrRegex = envOr("ALLOW_ADDR_REGEX", ".*")
	if cfg.AllowAddrRegex != "" {
		re, err := regexp.Compile(cfg.AllowAddrRegex)
		if err != nil {
			slog.Error("invalid ALLOW_ADDR_REGEX", "error", err)
			os.Exit(1)
		}
		cfg.AllowAddrRegexp = re
	}
	cfg.AppendPort = envOr("APPEND_PORT", "")
	cfg.StatementTimeout = envDuration("STATEMENT_TIMEOUT", "30s")
	cfg.MaxFieldBytes = envInt("MAX_FIELD_BYTES", 1048576)
	cfg.MaxRows = envInt("MAX_ROWS", 0)
	cfg.CursorBatchSize = envInt("CURSOR_BATCH_SIZE", 100)
	cfg.AllowedOrigins = envOr("ALLOWED_ORIGINS", "*")
	cfg.LogQueries = envOr("LOG_QUERIES", "false") == "true"
	cfg.LogTraffic = envOr("LOG_TRAFFIC", "false") == "true"

	// Region
	cfg.Region = envOr("REGION", "")

	// Connection pooling
	cfg.PoolEnabled = envOr("POOL_ENABLED", "false") == "true"
	cfg.PoolMaxConns = envInt("POOL_MAX_CONNS", 20)
	cfg.PoolMinConns = envInt("POOL_MIN_CONNS", 2)
	cfg.PoolMaxConnLifetime = envDuration("POOL_MAX_CONN_LIFETIME", "30m")
	cfg.PoolMaxConnIdleTime = envDuration("POOL_MAX_CONN_IDLE_TIME", "5m")

	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	var n int
	if _, err := fmt.Sscanf(v, "%d", &n); err != nil {
		return fallback
	}
	return n
}

func envDuration(key, fallback string) time.Duration {
	v := envOr(key, fallback)
	d, err := time.ParseDuration(v)
	if err != nil {
		d, _ = time.ParseDuration(fallback)
	}
	return d
}
