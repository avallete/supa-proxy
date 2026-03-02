package main

import (
	"regexp"
	"time"
)

// Config holds all pg-gateway configuration, loaded from env vars.
type Config struct {
	// Network
	ListenPort  string `env:"LISTEN_PORT" envDefault:":15432"`
	MetricsPort string `env:"METRICS_PORT" envDefault:":2112"`

	// Region (for observability and cross-region detection)
	Region string `env:"REGION"` // e.g., "ap-southeast-1"

	// JWT
	JWTSecret string `env:"JWT_SECRET"`

	// Target Postgres (server-side credentials, never exposed to clients)
	PGHost     string `env:"PG_HOST" envDefault:"127.0.0.1"`
	PGPort     int    `env:"PG_PORT" envDefault:"5432"`
	PGUser     string `env:"PG_USER" envDefault:"postgres"`
	PGPassword string `env:"PG_PASSWORD"`
	PGDatabase string `env:"PG_DATABASE" envDefault:"postgres"`

	// WS relay (wsproxy-compatible)
	AllowAddrRegex  string         `env:"ALLOW_ADDR_REGEX" envDefault:".*"`
	AllowAddrRegexp *regexp.Regexp // compiled from AllowAddrRegex; nil means allow all
	AppendPort      string         `env:"APPEND_PORT"` // if set, overrides ?address= param

	// Query safety
	StatementTimeout time.Duration `env:"STATEMENT_TIMEOUT" envDefault:"30s"`
	MaxFieldBytes    int           `env:"MAX_FIELD_BYTES" envDefault:"1048576"` // 1MB
	MaxRows          int           `env:"MAX_ROWS" envDefault:"0"`             // 0 = unlimited
	CursorBatchSize  int           `env:"CURSOR_BATCH_SIZE" envDefault:"100"`

	// Connection pooling (for /sql endpoint only)
	PoolEnabled         bool          `env:"POOL_ENABLED" envDefault:"false"`
	PoolMaxConns        int           `env:"POOL_MAX_CONNS" envDefault:"20"`        // per target database
	PoolMinConns        int           `env:"POOL_MIN_CONNS" envDefault:"2"`         // per target database
	PoolMaxConnLifetime time.Duration `env:"POOL_MAX_CONN_LIFETIME" envDefault:"30m"`
	PoolMaxConnIdleTime time.Duration `env:"POOL_MAX_CONN_IDLE_TIME" envDefault:"5m"`

	// CORS
	AllowedOrigins string `env:"ALLOWED_ORIGINS" envDefault:"*"`

	// Logging
	LogQueries bool `env:"LOG_QUERIES" envDefault:"false"`
	LogTraffic bool `env:"LOG_TRAFFIC" envDefault:"false"`
}
