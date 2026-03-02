package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newWSTestServer creates an httptest.Server that serves handleWSRelay with the given config.
func newWSTestServer(t *testing.T, cfg *Config) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handleWSRelay(w, r, cfg)
	}))
	t.Cleanup(ts.Close)
	return ts
}

// dialWS connects a WebSocket client to the test server's URL with an optional token.
func dialWS(t *testing.T, ts *httptest.Server, token string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	header := http.Header{}
	if token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, header)
	if err != nil {
		if resp != nil {
			t.Fatalf("WS dial failed: %v (status %d)", err, resp.StatusCode)
		}
		t.Fatalf("WS dial failed: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// listenLocalTCP opens a random local TCP listener and registers cleanup.
func listenLocalTCP(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listenLocalTCP: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

// addrPort parses the port from a "host:port" string without importing strconv.
func addrPort(addr string) int {
	parts := strings.SplitN(addr, ":", 2)
	if len(parts) != 2 {
		return 0
	}
	n := 0
	for _, c := range parts[1] {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// addrHost parses the host from a "host:port" string.
func addrHost(addr string) string {
	parts := strings.SplitN(addr, ":", 2)
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

// TestWSRelay_MissingAuth verifies that a connection without a JWT is rejected
// before the WebSocket upgrade (HTTP 401).
func TestWSRelay_MissingAuth(t *testing.T) {
	mockAddr, _ := startMockPGServer(t)
	cfg := newTestConfig(addrHost(mockAddr), addrPort(mockAddr), testSecret)

	ts := newWSTestServer(t, cfg)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http")
	_, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	require.Error(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestWSRelay_Connect verifies that bytes sent via WS arrive at the mock TCP server.
func TestWSRelay_Connect(t *testing.T) {
	mockAddr, received := startMockPGServer(t)
	cfg := newTestConfig(addrHost(mockAddr), addrPort(mockAddr), testSecret)

	ts := newWSTestServer(t, cfg)
	token := mintTestJWT(testSecret, "relay-user")
	wsConn := dialWS(t, ts, token)

	// Fake PG startup message (8 bytes: length + protocol version)
	startupMsg := []byte{0, 0, 0, 8, 0, 3, 0, 0}
	err := wsConn.WriteMessage(websocket.BinaryMessage, startupMsg)
	require.NoError(t, err)

	select {
	case data := <-received:
		assert.Equal(t, startupMsg, data)
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: data never arrived at mock PG server")
	}
}

// TestWSRelay_BiDirectional verifies that bytes from the mock PG server reach the WS client.
func TestWSRelay_BiDirectional(t *testing.T) {
	pgResponse := []byte{0x52, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00}
	mockAddr, connected := startMockPGServerWithResponse(t, pgResponse)
	cfg := newTestConfig(addrHost(mockAddr), addrPort(mockAddr), testSecret)

	ts := newWSTestServer(t, cfg)
	token := mintTestJWT(testSecret, "relay-user")
	wsConn := dialWS(t, ts, token)

	// Trigger the server-side response
	err := wsConn.WriteMessage(websocket.BinaryMessage, []byte{0, 0, 0, 8, 0, 3, 0, 0})
	require.NoError(t, err)

	select {
	case <-connected:
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: mock PG server never accepted connection")
	}

	// Read the response forwarded by the relay
	wsConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := wsConn.ReadMessage()
	require.NoError(t, err)
	assert.Equal(t, pgResponse, data)
}

// TestWSRelay_CloseOnPGDisconnect verifies that when the PG TCP connection closes,
// the WS client receives a normal closure frame.
func TestWSRelay_CloseOnPGDisconnect(t *testing.T) {
	ln := listenLocalTCP(t)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		conn.Close() // close immediately → relay should send WS close frame
	}()

	addr := ln.Addr().String()
	cfg := newTestConfig(addrHost(addr), addrPort(addr), testSecret)

	ts := newWSTestServer(t, cfg)
	token := mintTestJWT(testSecret, "relay-user")
	wsConn := dialWS(t, ts, token)

	wsConn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := wsConn.ReadMessage()
	require.Error(t, err) // connection must close

	closeErr, ok := err.(*websocket.CloseError)
	if ok {
		assert.Equal(t, websocket.CloseNormalClosure, closeErr.Code)
	}
}

// TestWSRelay_CloseOnClientDisconnect verifies that when the WS client disconnects,
// the PG TCP connection is also closed (no goroutine/resource leak).
func TestWSRelay_CloseOnClientDisconnect(t *testing.T) {
	pgClosedCh := make(chan struct{}, 1)
	ln := listenLocalTCP(t)

	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 1)
		for {
			_, err := conn.Read(buf)
			if err != nil {
				pgClosedCh <- struct{}{}
				conn.Close()
				return
			}
		}
	}()

	addr := ln.Addr().String()
	cfg := newTestConfig(addrHost(addr), addrPort(addr), testSecret)

	ts := newWSTestServer(t, cfg)
	token := mintTestJWT(testSecret, "relay-user")
	wsConn := dialWS(t, ts, token)

	// Ensure the TCP connection is established before we disconnect
	wsConn.WriteMessage(websocket.BinaryMessage, []byte{0xFF})
	time.Sleep(100 * time.Millisecond)

	// Close WS client
	wsConn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"))
	wsConn.Close()

	select {
	case <-pgClosedCh:
		// PG TCP closed as expected
	case <-time.After(3 * time.Second):
		t.Fatal("timeout: PG TCP connection was not closed after WS client disconnect")
	}
}
