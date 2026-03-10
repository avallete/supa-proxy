package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	pgMsgDataRow         = 'D'
	pgMsgRowDescription  = 'T'
	pgMsgCommandComplete = 'C'
	pgMsgReadyForQuery   = 'Z'
	pgMsgErrorResponse   = 'E'
	pgMsgNoticeResponse  = 'N'
	pgMsgEmptyQuery      = 'I'
)

// wireConn wraps a raw PostgreSQL connection for wire-level message parsing.
// It reads PG protocol messages directly, allowing per-column truncation of
// DataRow messages to enforce a hard memory ceiling.
type wireConn struct {
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

type wireColumn struct {
	Data        []byte
	OriginalLen int
	Truncated   bool
}

type fieldDesc struct {
	Name        string
	DataTypeOID uint32
}

func newWireConn(conn net.Conn) *wireConn {
	return &wireConn{
		conn: conn,
		r:    bufio.NewReaderSize(conn, 32*1024),
		w:    bufio.NewWriterSize(conn, 8*1024),
	}
}

func (wc *wireConn) Close() error {
	return wc.conn.Close()
}

// sendSimpleQuery sends a PG simple-query message ('Q').
func (wc *wireConn) sendSimpleQuery(sql string) error {
	sqlBytes := []byte(sql)
	msgLen := uint32(4 + len(sqlBytes) + 1)
	var header [5]byte
	header[0] = 'Q'
	binary.BigEndian.PutUint32(header[1:], msgLen)
	if _, err := wc.w.Write(header[:]); err != nil {
		return err
	}
	if _, err := wc.w.Write(sqlBytes); err != nil {
		return err
	}
	if err := wc.w.WriteByte(0); err != nil {
		return err
	}
	return wc.w.Flush()
}

// sendTerminate sends a PG Terminate message ('X') for a clean disconnect.
func (wc *wireConn) sendTerminate() error {
	msg := [5]byte{'X', 0, 0, 0, 4}
	_, err := wc.w.Write(msg[:])
	if err != nil {
		return err
	}
	return wc.w.Flush()
}

// readMsgHeader reads the next PG message header: type byte + body length.
func (wc *wireConn) readMsgHeader() (msgType byte, bodyLen int, err error) {
	var header [5]byte
	if _, err = io.ReadFull(wc.r, header[:]); err != nil {
		return 0, 0, fmt.Errorf("read message header: %w", err)
	}
	bodyLen = int(binary.BigEndian.Uint32(header[1:])) - 4
	if bodyLen < 0 {
		return 0, 0, fmt.Errorf("invalid message body length: %d", bodyLen)
	}
	return header[0], bodyLen, nil
}

// readBody reads exactly bodyLen bytes from the connection.
func (wc *wireConn) readBody(bodyLen int) ([]byte, error) {
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(wc.r, body); err != nil {
		return nil, fmt.Errorf("read message body (%d bytes): %w", bodyLen, err)
	}
	return body, nil
}

// readDataRowTruncated reads a DataRow body column-by-column, truncating any
// column whose wire length exceeds maxFieldBytes. Oversized trailing bytes are
// discarded via io.Discard in small chunks — they never enter a Go buffer.
// When maxFieldBytes <= 0, columns are read in full (no truncation).
func (wc *wireConn) readDataRowTruncated(bodyLen int, maxFieldBytes int) ([]wireColumn, error) {
	var numColBuf [2]byte
	if _, err := io.ReadFull(wc.r, numColBuf[:]); err != nil {
		return nil, fmt.Errorf("read DataRow column count: %w", err)
	}
	numCols := int(binary.BigEndian.Uint16(numColBuf[:]))
	columns := make([]wireColumn, numCols)

	for i := 0; i < numCols; i++ {
		var colLenBuf [4]byte
		if _, err := io.ReadFull(wc.r, colLenBuf[:]); err != nil {
			return nil, fmt.Errorf("read DataRow col %d length: %w", i, err)
		}
		colLen := int(int32(binary.BigEndian.Uint32(colLenBuf[:])))

		if colLen == -1 {
			columns[i] = wireColumn{OriginalLen: -1}
			continue
		}

		if maxFieldBytes <= 0 || colLen <= maxFieldBytes {
			data := make([]byte, colLen)
			if _, err := io.ReadFull(wc.r, data); err != nil {
				return nil, fmt.Errorf("read DataRow col %d data (%d bytes): %w", i, colLen, err)
			}
			columns[i] = wireColumn{Data: data, OriginalLen: colLen}
		} else {
			data := make([]byte, maxFieldBytes)
			if _, err := io.ReadFull(wc.r, data); err != nil {
				return nil, fmt.Errorf("read DataRow col %d truncated data: %w", i, err)
			}
			discard := int64(colLen - maxFieldBytes)
			if _, err := io.CopyN(io.Discard, wc.r, discard); err != nil {
				return nil, fmt.Errorf("discard DataRow col %d overflow (%d bytes): %w", i, discard, err)
			}
			columns[i] = wireColumn{Data: data, OriginalLen: colLen, Truncated: true}
		}
	}
	return columns, nil
}

// drainUntilReady reads and discards messages until ReadyForQuery ('Z') is
// received. Returns the last ErrorResponse fields if one was encountered.
func (wc *wireConn) drainUntilReady() (errFields map[byte]string, err error) {
	for {
		msgType, bodyLen, err := wc.readMsgHeader()
		if err != nil {
			return nil, err
		}
		body, err := wc.readBody(bodyLen)
		if err != nil {
			return nil, err
		}
		switch msgType {
		case pgMsgReadyForQuery:
			return errFields, nil
		case pgMsgErrorResponse:
			errFields = parseErrorResponse(body)
		}
	}
}

// parseRowDescription parses a RowDescription ('T') message body.
func parseRowDescription(body []byte) ([]fieldDesc, error) {
	if len(body) < 2 {
		return nil, fmt.Errorf("RowDescription body too short (%d bytes)", len(body))
	}
	numFields := int(binary.BigEndian.Uint16(body[:2]))
	pos := 2
	fields := make([]fieldDesc, numFields)

	for i := 0; i < numFields; i++ {
		nameEnd := pos
		for nameEnd < len(body) && body[nameEnd] != 0 {
			nameEnd++
		}
		if nameEnd >= len(body) {
			return nil, fmt.Errorf("RowDescription: unterminated field name at field %d", i)
		}
		name := string(body[pos:nameEnd])
		pos = nameEnd + 1

		// 18 bytes: table_oid(4) + col_attr(2) + type_oid(4) + type_len(2) + type_mod(4) + format(2)
		if pos+18 > len(body) {
			return nil, fmt.Errorf("RowDescription: truncated descriptor at field %d", i)
		}
		typeOID := binary.BigEndian.Uint32(body[pos+6:])
		fields[i] = fieldDesc{Name: name, DataTypeOID: typeOID}
		pos += 18
	}
	return fields, nil
}

// parseCommandComplete extracts the command tag from a CommandComplete ('C') body.
func parseCommandComplete(body []byte) string {
	for i, b := range body {
		if b == 0 {
			return string(body[:i])
		}
	}
	return string(body)
}

// parseErrorResponse extracts typed error fields from an ErrorResponse ('E') body.
// Common field types: 'S' severity, 'V' severity (non-localized), 'C' SQLSTATE code,
// 'M' message, 'D' detail, 'H' hint.
func parseErrorResponse(body []byte) map[byte]string {
	fields := make(map[byte]string)
	pos := 0
	for pos < len(body) {
		fieldType := body[pos]
		pos++
		if fieldType == 0 {
			break
		}
		end := pos
		for end < len(body) && body[end] != 0 {
			end++
		}
		fields[fieldType] = string(body[pos:end])
		pos = end + 1
	}
	return fields
}

// computeBatchSize dynamically adjusts the cursor batch size so that the worst-case
// memory for one batch (batch_size × num_columns × max_field_bytes) stays within
// the per-query memory budget.
func computeBatchSize(opts queryOptions, numColumns int) int {
	if opts.MaxMemory <= 0 || opts.MaxFieldBytes <= 0 || numColumns <= 0 {
		return opts.CursorBatchSize
	}
	maxRowMem := numColumns * opts.MaxFieldBytes
	batch := opts.MaxMemory / maxRowMem
	if batch < 1 {
		batch = 1
	}
	if batch > opts.CursorBatchSize {
		batch = opts.CursorBatchSize
	}
	return batch
}
