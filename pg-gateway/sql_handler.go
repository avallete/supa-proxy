package main

import (
	"bytes"
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
	Query            string  `json:"query"`
	DB               string  `json:"db,omitempty"`
	ReadOnly         bool    `json:"readonly,omitempty"`
	MaxFieldBytes    *int    `json:"max_field_bytes,omitempty"`
	MaxRows          *int    `json:"max_rows,omitempty"`
	StatementTimeout *string `json:"statement_timeout,omitempty"`
	CursorBatchSize  *int    `json:"cursor_batch_size,omitempty"`
	MaxMemory        *int    `json:"max_memory,omitempty"`
}

type columnMsg struct {
	Columns []columnInfo `json:"columns"`
}
type columnInfo struct {
	Name    string `json:"name"`
	TypeOID uint32 `json:"type_oid"`
}

type rowMsg struct {
	Row             map[string]interface{} `json:"row"`
	TruncatedFields []string               `json:"truncated_fields,omitempty"`
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

// --- Per-query options ---

type queryOptions struct {
	MaxFieldBytes    int
	MaxRows          int
	StatementTimeout time.Duration
	CursorBatchSize  int
	MaxMemory        int
}

func resolveOptions(cfg *Config, req *sqlRequest) queryOptions {
	opts := queryOptions{
		MaxFieldBytes:    cfg.MaxFieldBytes,
		MaxRows:          cfg.MaxRows,
		StatementTimeout: cfg.StatementTimeout,
		CursorBatchSize:  cfg.CursorBatchSize,
		MaxMemory:        cfg.MaxMemory,
	}
	if opts.CursorBatchSize <= 0 {
		opts.CursorBatchSize = 100
	}
	if req.MaxFieldBytes != nil {
		opts.MaxFieldBytes = *req.MaxFieldBytes
	}
	if req.MaxRows != nil {
		opts.MaxRows = *req.MaxRows
	}
	if req.StatementTimeout != nil {
		if d, err := time.ParseDuration(*req.StatementTimeout); err == nil {
			opts.StatementTimeout = d
		}
	}
	if req.CursorBatchSize != nil && *req.CursorBatchSize > 0 {
		opts.CursorBatchSize = *req.CursorBatchSize
	}
	if req.MaxMemory != nil {
		opts.MaxMemory = *req.MaxMemory
	}
	return opts
}

// --- Stream writer ---

type streamWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
}

func (sw *streamWriter) writeLine(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if _, err := sw.w.Write(data); err != nil {
		return err
	}
	if sw.flusher != nil {
		sw.flusher.Flush()
	}
	return nil
}

func (sw *streamWriter) writeRaw(data []byte) error {
	if _, err := sw.w.Write(data); err != nil {
		return err
	}
	if sw.flusher != nil {
		sw.flusher.Flush()
	}
	return nil
}

// --- Handler ---

func handleSQL(w http.ResponseWriter, r *http.Request, cfg *Config, pm *poolManager) {
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

	opts := resolveOptions(cfg, &req)

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
	sw := &streamWriter{w: w, flusher: flusher}

	ctx := withRequestID(r.Context(), r)
	start := time.Now()

	if cfg.LogQueries {
		q := req.Query
		if len(q) > 200 {
			q = q[:200] + "..."
		}
		slog.InfoContext(ctx, "sql query", "user", claimsSub(claims), "query", q)
	}

	// Cross-region warning
	if cfg.Region != "" && claims.Host != "" && claims.Host != cfg.PGHost {
		slog.WarnContext(ctx, "cross-region query",
			"proxy_region", cfg.Region,
			"target_host", claims.Host,
		)
	}

	if err := executeStreaming(ctx, connCfg, &req, claims, cfg, pm, host, port, db, readonly, opts, start, sw); err != nil {
		sw.writeLine(errorMsg{Error: errorInfo{Message: err.Error()}})
	}
}

type queryKind int

const (
	queryKindSelect       queryKind = iota // → streamSelectWire (cursor, wire-level truncation)
	queryKindDMLReturning                  // → streamReturningWire (wire-level truncation)
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
	pm *poolManager,
	host string,
	port int,
	db string,
	readonly bool,
	opts queryOptions,
	start time.Time,
	sw *streamWriter,
) error {
	kind := classifyQuery(req.Query)

	switch kind {
	case queryKindSelect, queryKindDMLReturning:
		conn, err := acquireOwnedConn(ctx, connCfg, pm, host, port, db)
		if err != nil {
			dbErrorsTotal.WithLabelValues("connection").Inc()
			return fmt.Errorf("connection failed: %w", err)
		}
		if err := applySession(ctx, conn, claims, cfg, readonly, opts); err != nil {
			conn.Close(ctx)
			return err
		}
		if kind == queryKindSelect {
			return streamSelectWire(ctx, conn, req.Query, opts, start, sw)
		}
		return streamReturningWire(ctx, conn, req.Query, opts, start, sw)

	default:
		conn, release, err := acquireConn(ctx, connCfg, pm, host, port, db)
		if err != nil {
			dbErrorsTotal.WithLabelValues("connection").Inc()
			return fmt.Errorf("connection failed: %w", err)
		}
		defer release()
		if err := applySession(ctx, conn, claims, cfg, readonly, opts); err != nil {
			return err
		}
		return execDML(ctx, conn, req.Query, start, sw)
	}
}

// acquireConn gets a *pgx.Conn from the pool (borrowed) or via direct connection.
// The release function must be called when done.
func acquireConn(ctx context.Context, connCfg *pgx.ConnConfig, pm *poolManager, host string, port int, db string) (*pgx.Conn, func(), error) {
	if pm != nil {
		pool, err := pm.getPool(ctx, host, port, db)
		if err != nil {
			return nil, nil, err
		}
		poolConn, err := pool.Acquire(ctx)
		if err != nil {
			return nil, nil, err
		}
		return poolConn.Conn(), func() { poolConn.Release() }, nil
	}

	conn, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		return nil, nil, err
	}
	return conn, func() { conn.Close(ctx) }, nil
}

// acquireOwnedConn gets a *pgx.Conn with sole ownership (removed from pool if
// pooling is enabled). Caller must close the connection when done.
// This is required for the wire-level path where we hijack the underlying pgconn.
func acquireOwnedConn(ctx context.Context, connCfg *pgx.ConnConfig, pm *poolManager, host string, port int, db string) (*pgx.Conn, error) {
	if pm != nil {
		pool, err := pm.getPool(ctx, host, port, db)
		if err != nil {
			return nil, err
		}
		poolConn, err := pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		return poolConn.Hijack(), nil
	}

	return pgx.ConnectConfig(ctx, connCfg)
}

func applySession(ctx context.Context, conn *pgx.Conn, claims *JWTClaims, cfg *Config, readonly bool, opts queryOptions) error {
	if _, err := conn.Exec(ctx, "SET idle_in_transaction_session_timeout = '60s'"); err != nil {
		return err
	}
	timeoutMs := strconv.Itoa(int(opts.StatementTimeout.Milliseconds()))
	if _, err := conn.Exec(ctx, "SET statement_timeout = "+timeoutMs); err != nil {
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

// --- Wire-level streaming (Phase 2) ---

// streamSelectWire uses DECLARE CURSOR + FETCH with wire-level DataRow parsing.
// Columns exceeding opts.MaxFieldBytes are truncated at the TCP level before they
// enter any Go buffer. Combined with dynamic batch sizing, this enforces a hard
// per-query memory ceiling of opts.MaxMemory bytes.
//
// The conn is hijacked (caller must NOT close it); cleanup is handled internally.
func streamSelectWire(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	opts queryOptions,
	start time.Time,
	sw *streamWriter,
) error {
	// Use pgx for transaction + cursor setup.
	if _, err := conn.Exec(ctx, "BEGIN"); err != nil {
		conn.Close(ctx)
		return err
	}
	cursorSQL := fmt.Sprintf("DECLARE _pgw_cur NO SCROLL CURSOR FOR %s", query)
	if _, err := conn.Exec(ctx, cursorSQL); err != nil {
		conn.Exec(ctx, "ROLLBACK")
		conn.Close(ctx)
		return err
	}

	// Hijack the connection to get the raw net.Conn for wire-level parsing.
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		// pgconn hijack failed; attempt best-effort cleanup via pgx.
		conn.Exec(ctx, "CLOSE _pgw_cur")
		conn.Exec(ctx, "ROLLBACK")
		conn.Close(ctx)
		return fmt.Errorf("hijack connection: %w", err)
	}
	wc := newWireConn(hj.Conn)
	defer func() {
		wc.sendSimpleQuery("CLOSE _pgw_cur")
		wc.drainUntilReady()
		wc.sendSimpleQuery("ROLLBACK")
		wc.drainUntilReady()
		wc.sendTerminate()
		wc.Close()
	}()

	var (
		fields      []fieldDesc
		totalRows   int64
		columnsSent bool
		rowBuf      bytes.Buffer
	)

	batchSize := opts.CursorBatchSize

	for {
		fetchSQL := fmt.Sprintf("FETCH %d FROM _pgw_cur", batchSize)
		if err := wc.sendSimpleQuery(fetchSQL); err != nil {
			return fmt.Errorf("send FETCH: %w", err)
		}

		batchCount := 0
		for {
			msgType, bodyLen, err := wc.readMsgHeader()
			if err != nil {
				return fmt.Errorf("read message: %w", err)
			}

			switch msgType {
			case pgMsgRowDescription:
				body, err := wc.readBody(bodyLen)
				if err != nil {
					return err
				}
				fields, err = parseRowDescription(body)
				if err != nil {
					return err
				}
				if !columnsSent {
					cols := make([]columnInfo, len(fields))
					for i, fd := range fields {
						cols[i] = columnInfo{Name: fd.Name, TypeOID: fd.DataTypeOID}
					}
					if err := sw.writeLine(columnMsg{Columns: cols}); err != nil {
						return err
					}
					columnsSent = true
					batchSize = computeBatchSize(opts, len(fields))
				}

			case pgMsgDataRow:
				columns, err := wc.readDataRowTruncated(bodyLen, opts.MaxFieldBytes)
				if err != nil {
					return err
				}
				rowBuf.Reset()
				if err := writeJSONRow(&rowBuf, fields, columns); err != nil {
					return err
				}
				if err := sw.writeRaw(rowBuf.Bytes()); err != nil {
					return err
				}
				batchCount++
				totalRows++
				if opts.MaxRows > 0 && totalRows >= int64(opts.MaxRows) {
					goto done
				}

			case pgMsgCommandComplete:
				if _, err := wc.readBody(bodyLen); err != nil {
					return err
				}

			case pgMsgReadyForQuery:
				if _, err := wc.readBody(bodyLen); err != nil {
					return err
				}
				goto batchDone

			case pgMsgErrorResponse:
				body, err := wc.readBody(bodyLen)
				if err != nil {
					return err
				}
				ef := parseErrorResponse(body)
				dbErrorsTotal.WithLabelValues("query").Inc()
				return fmt.Errorf("%s: %s", ef['C'], ef['M'])

			case pgMsgEmptyQuery:
				if _, err := wc.readBody(bodyLen); err != nil {
					return err
				}

			case pgMsgNoticeResponse:
				if _, err := wc.readBody(bodyLen); err != nil {
					return err
				}

			default:
				if _, err := wc.readBody(bodyLen); err != nil {
					return err
				}
			}
		}
	batchDone:
		if batchCount < batchSize {
			break
		}
	}

done:
	dur := time.Since(start)
	rowsProcessedTotal.WithLabelValues("SELECT").Add(float64(totalRows))
	queryDuration.WithLabelValues("SELECT", "ok").Observe(dur.Seconds())
	return sw.writeLine(completeMsg{Complete: completeInfo{
		Command:    "SELECT",
		RowCount:   totalRows,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
	}})
}

// streamReturningWire handles DML with RETURNING using wire-level DataRow parsing.
// DECLARE CURSOR cannot wrap DML, so we send the query directly and read responses.
//
// The conn is hijacked (caller must NOT close it); cleanup is handled internally.
func streamReturningWire(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	opts queryOptions,
	start time.Time,
	sw *streamWriter,
) error {
	// Hijack immediately — we'll send the query ourselves at the wire level.
	hj, err := conn.PgConn().Hijack()
	if err != nil {
		conn.Close(ctx)
		return fmt.Errorf("hijack connection: %w", err)
	}
	wc := newWireConn(hj.Conn)
	defer func() {
		wc.sendTerminate()
		wc.Close()
	}()

	if err := wc.sendSimpleQuery(query); err != nil {
		return fmt.Errorf("send query: %w", err)
	}

	var (
		fields    []fieldDesc
		totalRows int64
		cmdTag    string
		rowBuf    bytes.Buffer
	)

	for {
		msgType, bodyLen, err := wc.readMsgHeader()
		if err != nil {
			return fmt.Errorf("read message: %w", err)
		}

		switch msgType {
		case pgMsgRowDescription:
			body, err := wc.readBody(bodyLen)
			if err != nil {
				return err
			}
			fields, err = parseRowDescription(body)
			if err != nil {
				return err
			}
			cols := make([]columnInfo, len(fields))
			for i, fd := range fields {
				cols[i] = columnInfo{Name: fd.Name, TypeOID: fd.DataTypeOID}
			}
			if err := sw.writeLine(columnMsg{Columns: cols}); err != nil {
				return err
			}

		case pgMsgDataRow:
			columns, err := wc.readDataRowTruncated(bodyLen, opts.MaxFieldBytes)
			if err != nil {
				return err
			}
			rowBuf.Reset()
			if err := writeJSONRow(&rowBuf, fields, columns); err != nil {
				return err
			}
			if err := sw.writeRaw(rowBuf.Bytes()); err != nil {
				return err
			}
			totalRows++

		case pgMsgCommandComplete:
			body, err := wc.readBody(bodyLen)
			if err != nil {
				return err
			}
			cmdTag = parseCommandComplete(body)

		case pgMsgReadyForQuery:
			if _, err := wc.readBody(bodyLen); err != nil {
				return err
			}
			goto done

		case pgMsgErrorResponse:
			body, err := wc.readBody(bodyLen)
			if err != nil {
				return err
			}
			ef := parseErrorResponse(body)
			dbErrorsTotal.WithLabelValues("query").Inc()
			return fmt.Errorf("%s: %s", ef['C'], ef['M'])

		default:
			if _, err := wc.readBody(bodyLen); err != nil {
				return err
			}
		}
	}

done:
	if cmdTag == "" {
		cmdTag = "UNKNOWN"
	}
	dur := time.Since(start)
	rowsProcessedTotal.WithLabelValues(cmdTag).Add(float64(totalRows))
	queryDuration.WithLabelValues(cmdTag, "ok").Observe(dur.Seconds())
	return sw.writeLine(completeMsg{Complete: completeInfo{
		Command:    cmdTag,
		RowCount:   totalRows,
		DurationMs: float64(dur.Microseconds()) / 1000.0,
	}})
}

// execDML handles INSERT, UPDATE, DELETE, CREATE, etc. (without RETURNING).
// No wire-level parsing needed since there are no DataRow messages.
func execDML(
	ctx context.Context,
	conn *pgx.Conn,
	query string,
	start time.Time,
	sw *streamWriter,
) error {
	tag, err := conn.Exec(ctx, query)
	if err != nil {
		dbErrorsTotal.WithLabelValues("exec").Inc()
		return err
	}

	dur := time.Since(start)
	cmd := tag.String()
	queryDuration.WithLabelValues(cmd, "ok").Observe(dur.Seconds())

	if err := sw.writeLine(columnMsg{Columns: []columnInfo{}}); err != nil {
		return err
	}

	return sw.writeLine(completeMsg{Complete: completeInfo{
		Command:    cmd,
		RowCount:   tag.RowsAffected(),
		DurationMs: float64(dur.Microseconds()) / 1000.0,
	}})
}

// --- Helpers ---

func newPGConnConfig(cfg *Config, host string, port int, db string) (*pgx.ConnConfig, error) {
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s dbname=%s sslmode=prefer statement_timeout=%s",
		host, port, cfg.PGUser, db,
		strconv.Itoa(int(cfg.StatementTimeout.Milliseconds())),
	)
	connCfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("pgx config: %w", err)
	}
	connCfg.Password = cfg.PGPassword
	return connCfg, nil
}

func isSelectLike(upper string) bool {
	for strings.HasPrefix(upper, "WITH ") || strings.HasPrefix(upper, "(") {
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

// Future: COPY-based true streaming for O(buffer_size) memory on /sql.
//
// For SELECT queries that need untruncated large fields (100MB+), the wire-level
// approach above still materializes up to max_field_bytes per column. To achieve
// O(32KB) memory regardless of column size, a future Phase 3 could use:
//
//   conn.PgConn().CopyTo(ctx, httpResponseWriter,
//       `COPY (SELECT row_to_json(t) FROM (`+query+`) t) TO STDOUT CSV QUOTE e'\x01'`)
//
// PostgreSQL does JSON serialization server-side; Go becomes a pure byte pipe.
// Limitations: SELECT only (not DML+RETURNING), no $1 params, PG's row_to_json format.
//
// Until then, use the WebSocket relay (/v1) for truly unlimited streaming — it
// forwards raw PG wire protocol over TCP with O(32KB) memory.
