package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testSecret = "super-secret-key-for-testing"

func TestValidateJWT_Valid(t *testing.T) {
	token := mintTestJWT(testSecret, "user1")
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)

	claims, err := validateJWT(r, testSecret)
	require.NoError(t, err)
	assert.Equal(t, "user1", claims.Subject)
}

func TestValidateJWT_Expired(t *testing.T) {
	token := mintExpiredJWT(testSecret, "user1")
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)

	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid token")
}

func TestValidateJWT_BadSignature(t *testing.T) {
	token := mintTestJWT("different-secret", "user1")
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)

	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
}

func TestValidateJWT_WrongAlgorithm(t *testing.T) {
	// Mint an RS256 token (unsigned, just checking algorithm rejection)
	// We use HS256 with a different method identifier to simulate.
	// Real RS256 would need a key pair; use a crafted header instead.
	// golang-jwt will reject non-HMAC methods.
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer eyJhbGciOiJub25lIiwidHlwIjoiSldUIn0.eyJzdWIiOiJ1c2VyMSJ9.")

	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
}

func TestValidateJWT_MissingHeader(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)

	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing")
}

func TestValidateJWT_Claims(t *testing.T) {
	claims := &JWTClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   "svc-account",
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
		},
		Role:     "anon",
		DB:       "mydb",
		ReadOnly: true,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenStr, err := token.SignedString([]byte(testSecret))
	require.NoError(t, err)

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+tokenStr)

	parsed, err := validateJWT(r, testSecret)
	require.NoError(t, err)
	assert.Equal(t, "svc-account", parsed.Subject)
	assert.Equal(t, "anon", parsed.Role)
	assert.Equal(t, "mydb", parsed.DB)
	assert.True(t, parsed.ReadOnly)
}

func TestValidateJWT_BearerPrefix(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Token "+mintTestJWT(testSecret, "u"))

	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Bearer")
}

func TestAuthFailureReason_Expired(t *testing.T) {
	token := mintExpiredJWT(testSecret, "user1")
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer "+token)

	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
	assert.Equal(t, "expired_token", authFailureReason(err))
}

func TestAuthFailureReason_Missing(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
	assert.Equal(t, "missing_header", authFailureReason(err))
}

func TestAuthFailureReason_Invalid(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer bad.token.here")
	_, err := validateJWT(r, testSecret)
	require.Error(t, err)
	assert.Equal(t, "invalid_token", authFailureReason(err))
}

// TestSQL_MissingAuth verifies that /sql returns 401 without Authorization.
func TestSQL_MissingAuth(t *testing.T) {
	cfg := newTestConfig("127.0.0.1", 5432, testSecret)
	r := httptest.NewRequest(http.MethodPost, "/sql", nil)
	w := httptest.NewRecorder()

	handleSQL(w, r, cfg, nil)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestSQL_InvalidToken verifies that /sql returns 401 with a malformed token.
func TestSQL_InvalidToken(t *testing.T) {
	cfg := newTestConfig("127.0.0.1", 5432, testSecret)
	r := httptest.NewRequest(http.MethodPost, "/sql", nil)
	r.Header.Set("Authorization", "Bearer not.a.real.token")
	w := httptest.NewRecorder()

	handleSQL(w, r, cfg, nil)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

// TestSQL_ExpiredToken verifies that /sql returns 401 with an expired JWT.
func TestSQL_ExpiredToken(t *testing.T) {
	cfg := newTestConfig("127.0.0.1", 5432, testSecret)
	token := mintExpiredJWT(testSecret, "user1")
	r := httptest.NewRequest(http.MethodPost, "/sql", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	handleSQL(w, r, cfg, nil)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}
