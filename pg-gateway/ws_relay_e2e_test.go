//go:build integration

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- wsNetConn: implements net.Conn over a gorilla *websocket.Conn ---

type wsAddr struct{}

func (wsAddr) Network() string { return "websocket" }
func (wsAddr) String() string  { return "ws" }

type wsNetConn struct {
	ws      *websocket.Conn
	reader  io.Reader
	writeMu sync.Mutex
}

func (c *wsNetConn) Read(b []byte) (int, error) {
	for {
		if c.reader != nil {
			n, err := c.reader.Read(b)
			if err == io.EOF {
				c.reader = nil
				if n > 0 {
					return n, nil
				}
				continue
			}
			return n, err
		}
		_, r, err := c.ws.NextReader()
		if err != nil {
			return 0, err
		}
		c.reader = r
	}
}

func (c *wsNetConn) Write(b []byte) (int, error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if err := c.ws.WriteMessage(websocket.BinaryMessage, b); err != nil {
		return 0, err
	}
	return len(b), nil
}

func (c *wsNetConn) Close() error                       { return c.ws.Close() }
func (c *wsNetConn) LocalAddr() net.Addr                { return wsAddr{} }
func (c *wsNetConn) RemoteAddr() net.Addr               { return wsAddr{} }
func (c *wsNetConn) SetDeadline(t time.Time) error      { return c.ws.SetReadDeadline(t) }
func (c *wsNetConn) SetReadDeadline(t time.Time) error  { return c.ws.SetReadDeadline(t) }
func (c *wsNetConn) SetWriteDeadline(t time.Time) error { return c.ws.SetWriteDeadline(t) }

// dialPGOverWS connects pgx to a real PostgreSQL server through the WS relay test server.
// cfg must have AppendPort set to the PG container's "host:port".
func dialPGOverWS(ctx context.Context, t *testing.T, ts *httptest.Server, cfg *Config, token string) *pgx.Conn {
	t.Helper()

	// Build pgx config with real credentials; dummy host (overridden by DialFunc).
	dsn := fmt.Sprintf(
		"postgres://%s:%s@ignored:5432/%s?sslmode=disable",
		cfg.PGUser, cfg.PGPassword, cfg.PGDatabase,
	)
	pgCfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)

	// Override dial: connect to the WS relay instead of PG directly.
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	pgCfg.Config.DialFunc = func(ctx context.Context, _, _ string) (net.Conn, error) {
		hdr := http.Header{}
		hdr.Set("Authorization", "Bearer "+token)
		wsConn, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, hdr)
		if err != nil {
			return nil, err
		}
		return &wsNetConn{ws: wsConn}, nil
	}

	conn, err := pgx.ConnectConfig(ctx, pgCfg)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// newWSRelayE2ESetup starts a PG container and a WS relay server pointing at it.
// Returns a ready-to-use *pgx.Conn talking through the relay.
func newWSRelayE2ESetup(t *testing.T) (context.Context, *pgx.Conn) {
	t.Helper()
	ctx := context.Background()

	connStr := startTestPG(t)
	cfg := cfgFromConnStr(t, connStr, testSecret)
	cfg.AppendPort = fmt.Sprintf("%s:%d", cfg.PGHost, cfg.PGPort)

	ts := newWSTestServer(t, cfg)
	token := mintTestJWT(testSecret, "ws-e2e-user")
	conn := dialPGOverWS(ctx, t, ts, cfg, token)
	return ctx, conn
}

func TestWSRelayE2E_SimpleQuery(t *testing.T) {
	ctx, conn := newWSRelayE2ESetup(t)

	var n int
	err := conn.QueryRow(ctx, "SELECT 1").Scan(&n)
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	var a, b, c, d int
	err = conn.QueryRow(ctx, "SELECT $1::int + $2::int, $3::int, $4::int", 1, 2, 3, 4).Scan(&a, &b, &c)
	require.NoError(t, err)
	assert.Equal(t, 3, a)
	assert.Equal(t, 3, b)
	assert.Equal(t, 4, c)
	_ = d
}

func TestWSRelayE2E_DDL_And_DML(t *testing.T) {
	ctx, conn := newWSRelayE2ESetup(t)

	tbl := "_pgw_ws_e2e_ddl"
	_, err := conn.Exec(ctx, "CREATE TABLE "+tbl+" (id SERIAL PRIMARY KEY)")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tbl) })

	_, err = conn.Exec(ctx, "INSERT INTO "+tbl+" DEFAULT VALUES")
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "INSERT INTO "+tbl+" DEFAULT VALUES")
	require.NoError(t, err)

	var count int64
	err = conn.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count)
}

func TestWSRelayE2E_Transaction_Commit(t *testing.T) {
	ctx, conn := newWSRelayE2ESetup(t)

	tbl := "_pgw_ws_e2e_txcommit"
	_, err := conn.Exec(ctx, "CREATE TABLE "+tbl+" (id INT)")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tbl) })

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	var count int64
	err = conn.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count, "committed row must be visible")
}

func TestWSRelayE2E_Transaction_Rollback(t *testing.T) {
	ctx, conn := newWSRelayE2ESetup(t)

	tbl := "_pgw_ws_e2e_txrollback"
	_, err := conn.Exec(ctx, "CREATE TABLE "+tbl+" (id INT)")
	require.NoError(t, err)
	t.Cleanup(func() { conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+tbl) })

	tx, err := conn.Begin(ctx)
	require.NoError(t, err)
	_, err = tx.Exec(ctx, "INSERT INTO "+tbl+" VALUES (1)")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	var count int64
	err = conn.QueryRow(ctx, "SELECT count(*) FROM "+tbl).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(0), count, "rolled-back row must not be visible")
}

func TestWSRelayE2E_LargeResultSet(t *testing.T) {
	ctx, conn := newWSRelayE2ESetup(t)

	rows, err := conn.Query(ctx, "SELECT i FROM generate_series(1, 1000) i")
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		var i int
		require.NoError(t, rows.Scan(&i))
		count++
	}
	require.NoError(t, rows.Err())
	assert.Equal(t, 1000, count, "all 1000 rows must be delivered through the WS relay")
}
