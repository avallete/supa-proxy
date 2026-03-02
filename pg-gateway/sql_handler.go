package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	pgparser "github.com/multigres/multigres/go/parser"
	"github.com/multigres/multigres/go/parser/ast"
)

// --- Request/Response types ---

type sqlRequest struct {
	Query    string `json:"query"`
	DB       string `json:"db,omitempty"`
	ReadOnly bool   `json:"readonly,omitempty"`
}

type columnMsg struct {
	Columns []columnInfo `json:"columns"`
}
type columnInfo struct {
	Name    string `json:"name"`
	TypeOID uint32 `json:"type_oid"`
}

type rowMsg struct {
	Row map[string]interface{} `json:"row"`
}

type completeMsg struct {
	Complete completeInfo `json:"complete"`
}
type completeInfo struct {
	Command    string  `json:"command"`
	RowCount   int64   `json:"row_count"`
	DurationMs float64 `json:"duration_ms"`
}

type errorMsg struct {
	Error errorInfo `json:"error"`
}
type errorInfo struct {
	Message string `json:"message"`
	Code    string `json:"code,omitempty"`
	Detail  string `json:"detail,omitempty"`
	Hint    string `json:"hint,omitempty"`
}

// --- Handler ---

func handleSQL(w http.ResponseWriter, r *http.Request, cfg *Config) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, errorMsg{Error: errorInfo{Message: "method not allowed"}})
		return
	}

	// JWT auth
	claims, err := validateJWT(r, cfg.JWTSecret)
	if err != nil {
		authFailuresTotal.WithLabelValues(authFailureReason(err)).Inc()
		writeJSON(w, http.StatusUnauthorized, errorMsg{Error: errorInfo{Message: err.Error()}})
		return
	}

	activeConnections.WithLabelValues("sql_stream").Inc()
	defer activeConnections.WithLabelValues("sql_stream").Dec()

	// Parse body
	var req sqlRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorMsg{Error: errorInfo{Message: "invalid JSON body"}})
		return
	}
	if strings.TrimSpace(req.Query) == "" {
		writeJSON(w, http.StatusBadRequest, errorMsg{Error: errorInfo{Message: "missing 'query' field"}})
		return
	}

	// Resolve DB
	db := cfg.PGDatabase
	if req.DB != "" {
		db = req.DB
	} else if claims.DB != "" {
		db = claims.DB
	}

	// Resolve host (for multi-tenant)
	host := cfg.PGHost
	port := cfg.PGPort
	if claims.Host != "" {
		host = claims.Host
	}
	if claims.Port != 0 {
		port = claims.Port
	}

	// Bug 4 fix: build ConnConfig directly to avoid URL encoding issues with passwords containing %.
	connCfg, err := newPGConnConfig(cfg, host, port, db)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorMsg{Error: errorInfo{Message: err.Error()}})
		return
	}

	readonly := req.ReadOnly || claims.ReadOnly

	// Set up streaming response
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Transfer-Encoding", "chunked")
	w.Header().Set("Cache-Control", "no-cache")
	addCORS(w, r, cfg)
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)

	writeLine := func(v interface{}) error {
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		data = append(data, '\n')
		if _, err := w.Write(data); err != nil {
			return err
		}
		if flusher != nil {
			flusher.Flush()
		}
		return nil
	}

	ctx := withRequestID(r.Context(), r)
	start := time.Now()

	if cfg.LogQueries {
		q := req.Query
		if len(q) > 200 {
			q = q[:200] + "..."
		}
		slog.InfoContext(ctx, "sql query", "user", claimsSub(claims), "query", q)
	}

	if err := executeStreaming(ctx, connCfg, &req, claims, cfg, readonly, start, writeLine); err != nil {
		// Headers already sent; append an error line.
		writeLine(errorMsg{Error: errorInfo{Message: err.Error()}})
	}
}

type queryKind int

const (
	queryKindSelect       queryKind = iota // → streamSelect (cursor, bounded memory)
	queryKindDMLReturning                  // → streamReturning (conn.Query)
	queryKindDML                           // → execDML (conn.Exec)
)

// classifyQuery uses the multigres SQL parser to determine the query kind.
// On parse error it falls back to string matching for graceful degradation.
func classifyQuery(sql string) queryKind {
	stmts, err := pgparser.ParseSQL(sql)
	if err != nil || len(stmts) == 0 {
		upper := strings.ToUpper(strings.TrimSpace(sql))
		if isSelectLike(upper) {
			return queryKindSelect
		}
		if hasDMLReturning(upper) {
			return queryKindDMLReturning
		}
		return queryKindDML
	}

	switch stmt := stmts[0].(type) {
	case *ast.SelectStmt:
		return queryKindSelect
	case *ast.InsertStmt:
		if stmt.ReturningList != nil && stmt.ReturningList.Len() > 0 {
			return queryKindDMLReturning
		}
		return queryKindDML
	case *ast.UpdateStmt:
		if stmt.ReturningList != nil && stmt.ReturningList.Len() > 0 {
			return queryKindDMLReturning
		}
		return queryKindDML
	case *ast.DeleteStmt:
		if stmt.ReturningList != nil && stmt.ReturningList.Len() > 0 {
			return queryKindDMLReturning
		}
		return queryKindDML
	default:
		return queryKindDML
	}
}

func executeStreaming(
	ctx context.Context,
	connCfg *pgx.ConnConfig,
	req *sqlRequest,
	claims *JWTClaims,
	cfg *Config,
	readonly bool,
	start time.Time,
	writeLine func(interface{}) error,
) error {
	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		dbErrorsTotal.WithLabelValues("connection").Inc()
		return fmt.Errorf("connection failed: %w", err)
	}
	defer conn.Close(ctx)

	// Apply session settings
	if err := applySession(ctx, conn, claims, cfg, readonly); err != nil {
		return err
	}

	switch classifyQuery(req.Query) {
	case queryKindSelect:
		return streamSelect(ctx, conn, req.Query, cfg, start, writeLine)
	case queryKindDMLReturning:
		// Bug 1 fix: DML with RETURNING must use conn.Query, not conn.Exec,
		// otherwise returned rows are silently discarded.
		return streamReturning(ctx, conn, req.Query, cfg, start, writeLine)
	default:
		return execDML(ctx, conn, req.Query, start, writeLine)
	}
}

func applySession(ctx context.Context, conn *pgx.Conn, claims *JWTClaims, cfg *Config, readonly bool) error {
	if _, err := conn.Exec(ctx, "SET idle_in_transaction_session_timeout = '60s'"); err != nil {
		return err
	}
	if claims.Role != "" && isValidIdent(claims.Role) {
		if _, err := conn.Exec(ctx, "SET ROLE "+claims.Role); err != nil {
			return err
		}
	}
	if claims.Subject != "" {
		if _, err := conn.Exec(ctx, "SELECT set_config('request.jwt.sub', $1, false)", claims.Subject); err != nil {
			return err
		}
	}
	if readonly {
		if _, err := conn.Exec(ctx, "SET default_transaction_read_only = on"); err != nil {
			return err
		}
	}
	return nil
}

// streamSelect uses DECLARE CURSOR + FETCH for bounded-memory streaming.
func streamSelect(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	cfg *Config,
	start time.Time,
	writeLine func(interface{}) error,
) error {
	batch := cfg.CursorBatchSize
	if batch <= 0 {
		batch = 100
	}

	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		return err
	}
	// Bug 2 fix: log cleanup errors rather than silently ignoring them.
	defer func() {
		if _, err := conn.Exec(ctx, "ROLLBACK"); err != nil {
			slog.WarnContext(ctx, "ROLLBACK failed", "error", err)
		}
	}()

	cursorSQL := fmt.Sprintf("DECLARE _pgw_cur NO SCROLL CURSOR FOR %s", query)
	if _, err := conn.Exec(ctx, cursorSQL); err != nil {
		return err
	}

	var totalRows int64
	columnsSent := false

	for {
		rows, err := conn.Query(ctx, fmt.Sprintf("FETCH %d FROM _pgw_cur", batch))
		if err != nil {
			return err
		}

		if !columnsSent {
			fds := rows.FieldDescriptions()
			cols := make([]columnInfo, len(fds))
			for i, fd := range fds {
				cols[i] = columnInfo{Name: fd.Name, TypeOID: fd.DataTypeOID}
			}
			if err := writeLine(columnMsg{Columns: cols}); err != nil {
				rows.Close()
				return err
			}
			columnsSent = true
		}

		batchCount := 0
		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				rows.Close()
				return err
			}

			fds := rows.FieldDescriptions()
			row := make(map[string]interface{}, len(fds))
			for i, fd := range fds {
				row[fd.Name] = truncateValue(values[i], cfg.MaxFieldBytes)
			}

			if err := writeLine(rowMsg{Row: row}); err != nil {
				rows.Close()
				return err // client disconnected
			}
			batchCount++
			totalRows++

			if cfg.MaxRows > 0 && totalRows >= int64(cfg.MaxRows) {
				rows.Close()
				goto done
			}
		}
		rows.Close()
		if rows.Err() != nil {
			return rows.Err()
		}

		if batchCount < batch {
			break // no more rows
		}
	}

done:
	// Bug 2 fix: log errors on cleanup; don't return them (response headers already sent).
	if _, err := conn.Exec(ctx, "CLOSE _pgw_cur"); err != nil {
		slog.WarnContext(ctx, "CLOSE cursor failed", "error", err)
	}
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		slog.WarnContext(ctx, "COMMIT failed", "error", err)
	}

	dur := time.Since(start)
	rowsProcessedTotal.WithLabelValues("SELECT").Add(float64(totalRows))
	queryDuration.WithLabelValues("SELECT", "ok").Observe(dur.Seconds())
	return writeLine(completeMsg{Complete: completeInfo{
		Command:    "SELECT",
		RowCount:   totalRows,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
	}})
}

// streamReturning handles DML with RETURNING clauses.
// DECLARE CURSOR cannot wrap DML, so we use conn.Query directly.
func streamReturning(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	cfg *Config,
	start time.Time,
	writeLine func(interface{}) error,
) error {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		dbErrorsTotal.WithLabelValues("query").Inc()
		return err
	}

	// FieldDescriptions are available immediately after Query (from RowDescription).
	fds := rows.FieldDescriptions()
	cols := make([]columnInfo, len(fds))
	for i, fd := range fds {
		cols[i] = columnInfo{Name: fd.Name, TypeOID: fd.DataTypeOID}
	}
	if err := writeLine(columnMsg{Columns: cols}); err != nil {
		rows.Close()
		return err
	}

	var totalRows int64
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			rows.Close()
			return err
		}

		row := make(map[string]interface{}, len(fds))
		for i, fd := range fds {
			row[fd.Name] = truncateValue(values[i], cfg.MaxFieldBytes)
		}

		if err := writeLine(rowMsg{Row: row}); err != nil {
			rows.Close()
			return err
		}
		totalRows++
	}
	rows.Close()
	if rows.Err() != nil {
		dbErrorsTotal.WithLabelValues("query").Inc()
		return rows.Err()
	}

	cmd := rows.CommandTag().String()
	dur := time.Since(start)
	rowsProcessedTotal.WithLabelValues(cmd).Add(float64(totalRows))
	queryDuration.WithLabelValues(cmd, "ok").Observe(dur.Seconds())
	return writeLine(completeMsg{Complete: completeInfo{
		Command:    cmd,
		RowCount:   totalRows,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
	}})
}

// execDML handles INSERT, UPDATE, DELETE, CREATE, etc. (without RETURNING).
func execDML(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	start time.Time,
	writeLine func(interface{}) error,
) error {
	tag, err := conn.Exec(ctx, query)
	if err != nil {
		dbErrorsTotal.WithLabelValues("exec").Inc()
		return err
	}

	dur := time.Since(start)
	cmd := tag.String()
	queryDuration.WithLabelValues(cmd, "ok").Observe(dur.Seconds())

	// Empty columns for consistency
	if err := writeLine(columnMsg{Columns: []columnInfo{}}); err != nil {
		return err
	}

	return writeLine(completeMsg{Complete: completeInfo{
		Command:    cmd,
		RowCount:   tag.RowsAffected(),
		DurationMs: float64(dur.Microseconds()) / 1000.0,
	}})
}

// --- Helpers ---

// newPGConnConfig builds a pgx.ConnConfig without constructing a URL, which
// avoids URL-encoding issues with passwords that contain special characters like %.
func newPGConnConfig(cfg *Config, host string, port int, db string) (*pgx.ConnConfig, error) {
	// Use libpq key=value DSN for non-sensitive fields; set password directly on the struct.
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s sslmode=prefer statement_timeout=%s",
		host, port, cfg.PGUser, db,
		strconv.Itoa(int(cfg.StatementTimeout.Milliseconds())),
	)
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx config: %w", err)
	}
	// Bug 4 fix: set password directly to avoid any URL-encoding issues.
	connCfg.Password = cfg.PGPassword
	return connCfg, nil
}

func isSelectLike(upper string) bool {
	for strings.HasPrefix(upper, "WITH ") || strings.HasPrefix(upper, "(") {
		// skip CTEs and subquery wrappers
		idx := strings.Index(upper, "SELECT")
		if idx < 0 {
			return false
		}
		upper = upper[idx:]
	}
	return strings.HasPrefix(upper, "SELECT") ||
		strings.HasPrefix(upper, "TABLE") ||
		strings.HasPrefix(upper, "VALUES") ||
		strings.HasPrefix(upper, "SHOW") ||
		strings.HasPrefix(upper, "EXPLAIN")
}

// hasDMLReturning returns true for INSERT/UPDATE/DELETE queries that contain RETURNING.
func hasDMLReturning(upper string) bool {
	return strings.Contains(upper, " RETURNING") &&
		(strings.HasPrefix(upper, "INSERT") ||
			strings.HasPrefix(upper, "UPDATE") ||
			strings.HasPrefix(upper, "DELETE"))
}

func isValidIdent(s string) bool {
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
			return false
		}
	}
	return len(s) > 0
}

// truncateValue truncates large byte slices and strings to maxBytes.
// Other types pass through unchanged. This is the key to bounded memory:
// pgx already materialized the value, but we cap what we serialize to JSON.
func truncateValue(v interface{}, maxBytes int) interface{} {
	if maxBytes <= 0 {
		return v // no limit
	}
	switch val := v.(type) {
	case []byte:
		if len(val) > maxBytes {
			truncatedFieldsTotal.Inc()
			return fmt.Sprintf("[truncated: %d bytes, showing first %d]%s",
				len(val), maxBytes, string(val[:maxBytes]))
		}
	case string:
		if len(val) > maxBytes {
			truncatedFieldsTotal.Inc()
			return fmt.Sprintf("[truncated: %d bytes, showing first %d]%s",
				len(val), maxBytes, val[:maxBytes])
		}
	}
	return v
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func addCORS(w http.ResponseWriter, r *http.Request, cfg *Config) {
	origin := r.Header.Get("Origin")
	if origin == "" {
		origin = "*"
	}
	allowed := cfg.AllowedOrigins
	if allowed == "*" || strings.Contains(allowed, origin) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	w.Header().Set("Access-Control-Max-Age", "86400")
}
