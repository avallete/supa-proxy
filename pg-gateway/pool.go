package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// poolManager manages a map of pgxpool.Pool instances keyed by target database.
// Each unique "host:port/db" gets its own pool.
type poolManager struct {
	mu    sync.RWMutex
	pools map[string]*poolEntry
	cfg   *Config
	done  chan struct{}
}

type poolEntry struct {
	pool     *pgxpool.Pool
	lastUsed time.Time
}

// newPoolManager creates a poolManager and starts the idle eviction goroutine.
func newPoolManager(cfg *Config) *poolManager {
	pm := &poolManager{
		pools: make(map[string]*poolEntry),
		cfg:   cfg,
		done:  make(chan struct{}),
	}
	go pm.evictLoop()
	return pm
}

// poolKey returns the map key for a target database.
func poolKey(host string, port int, db string) string {
	return fmt.Sprintf("%s:%d/%s", host, port, db)
}

// getPool returns (or lazy-creates) a pgxpool.Pool for the given target.
func (pm *poolManager) getPool(ctx context.Context, host string, port int, db string) (*pgxpool.Pool, error) {
	key := poolKey(host, port, db)

	// Fast path: read lock
	pm.mu.RLock()
	if entry, ok := pm.pools[key]; ok {
		entry.lastUsed = time.Now()
		pm.mu.RUnlock()
		return entry.pool, nil
	}
	pm.mu.RUnlock()

	// Slow path: write lock (double-checked)
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if entry, ok := pm.pools[key]; ok {
		entry.lastUsed = time.Now()
		return entry.pool, nil
	}

	pool, err := pm.createPool(ctx, host, port, db)
	if err != nil {
		return nil, err
	}

	pm.pools[key] = &poolEntry{
		pool:     pool,
		lastUsed: time.Now(),
	}

	slog.Info("pool created", "target", key,
		"max_conns", pm.cfg.PoolMaxConns,
		"min_conns", pm.cfg.PoolMinConns,
	)

	return pool, nil
}

// createPool builds a new pgxpool.Pool for the given target.
func (pm *poolManager) createPool(ctx context.Context, host string, port int, db string) (*pgxpool.Pool, error) {
	connCfg, err := newPGConnConfig(pm.cfg, host, port, db)
	if err != nil {
		return nil, fmt.Errorf("pool config: %w", err)
	}

	poolCfg, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("pool parse config: %w", err)
	}

	poolCfg.ConnConfig = connCfg
	poolCfg.MaxConns = int32(pm.cfg.PoolMaxConns)
	poolCfg.MinConns = int32(pm.cfg.PoolMinConns)
	poolCfg.MaxConnLifetime = pm.cfg.PoolMaxConnLifetime
	poolCfg.MaxConnIdleTime = pm.cfg.PoolMaxConnIdleTime

	// AfterRelease runs DISCARD ALL to reset session state (ROLE, GUCs, temp tables,
	// prepared statements, LISTEN channels) before the connection returns to the pool.
	// Returns true to keep the connection in the pool.
	poolCfg.AfterRelease = func(conn *pgx.Conn) bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := conn.Exec(ctx, "DISCARD ALL"); err != nil {
			slog.Warn("DISCARD ALL failed on release, closing connection", "error", err)
			return false // destroy the connection
		}
		return true
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("pool create: %w", err)
	}

	return pool, nil
}

// evictLoop periodically removes pools that haven't been used for 10 minutes.
func (pm *poolManager) evictLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-pm.done:
			return
		case <-ticker.C:
			pm.evictIdle()
		}
	}
}

const poolIdleTimeout = 10 * time.Minute

func (pm *poolManager) evictIdle() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()
	for key, entry := range pm.pools {
		if now.Sub(entry.lastUsed) > poolIdleTimeout {
			slog.Info("evicting idle pool", "target", key,
				"idle_duration", now.Sub(entry.lastUsed).String(),
			)
			entry.pool.Close()
			delete(pm.pools, key)
		}
	}
}

// Close shuts down all pools and stops the eviction goroutine.
func (pm *poolManager) Close() {
	close(pm.done)

	pm.mu.Lock()
	defer pm.mu.Unlock()

	for key, entry := range pm.pools {
		entry.pool.Close()
		delete(pm.pools, key)
	}
}

// stats returns a snapshot of pool statistics for all targets.
func (pm *poolManager) stats() map[string]*pgxpool.Stat {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make(map[string]*pgxpool.Stat, len(pm.pools))
	for key, entry := range pm.pools {
		result[key] = entry.pool.Stat()
	}
	return result
}
