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

	// CORS
	AllowedOrigins string `env:"ALLOWED_ORIGINS" envDefault:"*"`

	// Logging
	LogQueries bool `env:"LOG_QUERIES" envDefault:"false"`
	LogTraffic bool `env:"LOG_TRAFFIC" envDefault:"false"`
}
