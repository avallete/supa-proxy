package main

import (
	"net"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// mintTestJWT creates a signed HS256 JWT for use in tests.
func mintTestJWT(secret, sub string) string {
	claims := &JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   sub,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, _ := token.SignedString([]byte(secret))
	return tokenStr
}

// mintExpiredJWT creates an already-expired JWT.
func mintExpiredJWT(secret, sub string) string {
	claims := &JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   sub,
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(-time.Hour)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, _ := token.SignedString([]byte(secret))
	return tokenStr
}

// newTestConfig returns a Config suitable for tests.
func newTestConfig(pgHost string, pgPort int, secret string) *Config {
	return &Config{
		ListenPort:       ":0",
		MetricsPort:      ":0",
		JWTSecret:        secret,
		PGHost:           pgHost,
		PGPort:           pgPort,
		PGUser:           "postgres",
		PGPassword:       "postgres",
		PGDatabase:       "postgres",
		AllowAddrRegex:   ".*",
		StatementTimeout: 30 * time.Second,
		MaxFieldBytes:    1048576,
		MaxMemory:        10485760,
		CursorBatchSize:  100,
		AllowedOrigins:   "*",
	}
}

// startMockPGServer starts a raw TCP listener on a random port.
// It accepts connections and forwards all received bytes to the returned channel.
// The listener is closed when the test ends.
func startMockPGServer(tb testing.TB) (addr string, received <-chan []byte) {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("startMockPGServer: listen: %v", err)
	}
	tb.Cleanup(func() { ln.Close() })

	ch := make(chan []byte, 256)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return // listener closed
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 65536)
				for {
					n, err := c.Read(buf)
					if n > 0 {
						data := make([]byte, n)
						copy(data, buf[:n])
						ch <- data
					}
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), ch
}

// startMockPGServerWithResponse starts a mock server that sends a fixed
// response back to any connecting client.
func startMockPGServerWithResponse(tb testing.TB, response []byte) (addr string, connected <-chan struct{}) {
	tb.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("startMockPGServerWithResponse: listen: %v", err)
	}
	tb.Cleanup(func() { ln.Close() })

	ch := make(chan struct{}, 1)

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			ch <- struct{}{}
			go func(c net.Conn) {
				defer c.Close()
				c.Write(response)
				// drain any incoming bytes to allow clean close
				buf := make([]byte, 4096)
				for {
					_, err := c.Read(buf)
					if err != nil {
						return
					}
				}
			}(conn)
		}
	}()

	return ln.Addr().String(), ch
}
