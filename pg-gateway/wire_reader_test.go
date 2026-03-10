package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers to build PG wire-format messages ---

func buildDataRow(columns ...[]byte) []byte {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(columns)))
	for _, col := range columns {
		if col == nil {
			binary.Write(&body, binary.BigEndian, int32(-1))
		} else {
			binary.Write(&body, binary.BigEndian, int32(len(col)))
			body.Write(col)
		}
	}
	b := body.Bytes()

	var msg bytes.Buffer
	msg.WriteByte('D')
	binary.Write(&msg, binary.BigEndian, int32(len(b)+4))
	msg.Write(b)
	return msg.Bytes()
}

func buildRowDescription(fields ...fieldDesc) []byte {
	var body bytes.Buffer
	binary.Write(&body, binary.BigEndian, int16(len(fields)))
	for _, f := range fields {
		body.WriteString(f.Name)
		body.WriteByte(0) // null terminator
		binary.Write(&body, binary.BigEndian, uint32(0))             // table OID
		binary.Write(&body, binary.BigEndian, int16(0))              // column attr
		binary.Write(&body, binary.BigEndian, f.DataTypeOID)         // type OID
		binary.Write(&body, binary.BigEndian, int16(0))              // type len
		binary.Write(&body, binary.BigEndian, int32(0))              // type mod
		binary.Write(&body, binary.BigEndian, int16(0))              // format code
	}
	b := body.Bytes()

	var msg bytes.Buffer
	msg.WriteByte('T')
	binary.Write(&msg, binary.BigEndian, int32(len(b)+4))
	msg.Write(b)
	return msg.Bytes()
}

func buildCommandComplete(tag string) []byte {
	body := append([]byte(tag), 0)
	var msg bytes.Buffer
	msg.WriteByte('C')
	binary.Write(&msg, binary.BigEndian, int32(len(body)+4))
	msg.Write(body)
	return msg.Bytes()
}

func buildReadyForQuery(status byte) []byte {
	var msg bytes.Buffer
	msg.WriteByte('Z')
	binary.Write(&msg, binary.BigEndian, int32(5))
	msg.WriteByte(status)
	return msg.Bytes()
}

func buildErrorResponse(code, message string) []byte {
	var body bytes.Buffer
	body.WriteByte('C') // code
	body.WriteString(code)
	body.WriteByte(0)
	body.WriteByte('M') // message
	body.WriteString(message)
	body.WriteByte(0)
	body.WriteByte(0) // terminator

	b := body.Bytes()
	var msg bytes.Buffer
	msg.WriteByte('E')
	binary.Write(&msg, binary.BigEndian, int32(len(b)+4))
	msg.Write(b)
	return msg.Bytes()
}

// pipeWireConn creates a wireConn backed by an in-memory pipe.
func pipeWireConn(data []byte) *wireConn {
	server, client := net.Pipe()
	go func() {
		server.Write(data)
		server.Close()
	}()
	return newWireConn(client)
}

// --- Tests ---

func TestReadDataRowTruncated_NormalRow(t *testing.T) {
	row := buildDataRow([]byte("hello"), []byte("42"), nil)
	wc := pipeWireConn(row)
	defer wc.Close()

	msgType, bodyLen, err := wc.readMsgHeader()
	require.NoError(t, err)
	assert.Equal(t, byte('D'), msgType)

	cols, err := wc.readDataRowTruncated(bodyLen, 1024)
	require.NoError(t, err)
	require.Len(t, cols, 3)

	assert.Equal(t, []byte("hello"), cols[0].Data)
	assert.Equal(t, 5, cols[0].OriginalLen)
	assert.False(t, cols[0].Truncated)

	assert.Equal(t, []byte("42"), cols[1].Data)
	assert.False(t, cols[1].Truncated)

	assert.Nil(t, cols[2].Data)
	assert.Equal(t, -1, cols[2].OriginalLen)
}

func TestReadDataRowTruncated_OversizedColumn(t *testing.T) {
	bigData := bytes.Repeat([]byte("x"), 5000)
	row := buildDataRow(bigData, []byte("ok"))
	wc := pipeWireConn(row)
	defer wc.Close()

	msgType, bodyLen, err := wc.readMsgHeader()
	require.NoError(t, err)
	assert.Equal(t, byte('D'), msgType)

	cols, err := wc.readDataRowTruncated(bodyLen, 100)
	require.NoError(t, err)
	require.Len(t, cols, 2)

	assert.Len(t, cols[0].Data, 100)
	assert.Equal(t, 5000, cols[0].OriginalLen)
	assert.True(t, cols[0].Truncated)

	assert.Equal(t, []byte("ok"), cols[1].Data)
	assert.False(t, cols[1].Truncated)
}

func TestReadDataRowTruncated_NoLimit(t *testing.T) {
	bigData := bytes.Repeat([]byte("a"), 2000)
	row := buildDataRow(bigData)
	wc := pipeWireConn(row)
	defer wc.Close()

	_, bodyLen, _ := wc.readMsgHeader()
	cols, err := wc.readDataRowTruncated(bodyLen, 0)
	require.NoError(t, err)
	assert.Len(t, cols[0].Data, 2000)
	assert.False(t, cols[0].Truncated)
}

func TestReadDataRowTruncated_AllNulls(t *testing.T) {
	row := buildDataRow(nil, nil, nil)
	wc := pipeWireConn(row)
	defer wc.Close()

	_, bodyLen, _ := wc.readMsgHeader()
	cols, err := wc.readDataRowTruncated(bodyLen, 1024)
	require.NoError(t, err)
	require.Len(t, cols, 3)
	for _, c := range cols {
		assert.Nil(t, c.Data)
		assert.Equal(t, -1, c.OriginalLen)
	}
}

func TestParseRowDescription(t *testing.T) {
	fields := []fieldDesc{
		{Name: "id", DataTypeOID: 23},
		{Name: "name", DataTypeOID: 25},
		{Name: "data", DataTypeOID: 3802},
	}
	msg := buildRowDescription(fields...)
	body := msg[5:] // skip 'T' + length

	parsed, err := parseRowDescription(body)
	require.NoError(t, err)
	require.Len(t, parsed, 3)
	assert.Equal(t, "id", parsed[0].Name)
	assert.Equal(t, uint32(23), parsed[0].DataTypeOID)
	assert.Equal(t, "name", parsed[1].Name)
	assert.Equal(t, uint32(25), parsed[1].DataTypeOID)
	assert.Equal(t, "data", parsed[2].Name)
	assert.Equal(t, uint32(3802), parsed[2].DataTypeOID)
}

func TestParseCommandComplete(t *testing.T) {
	body := append([]byte("SELECT 42"), 0)
	assert.Equal(t, "SELECT 42", parseCommandComplete(body))
}

func TestParseErrorResponse(t *testing.T) {
	msg := buildErrorResponse("42P01", "relation does not exist")
	body := msg[5:] // skip 'E' + length

	fields := parseErrorResponse(body)
	assert.Equal(t, "42P01", fields['C'])
	assert.Equal(t, "relation does not exist", fields['M'])
}

func TestComputeBatchSize(t *testing.T) {
	tests := []struct {
		name     string
		opts     queryOptions
		numCols  int
		expected int
	}{
		{
			name:     "no memory budget",
			opts:     queryOptions{MaxMemory: 0, MaxFieldBytes: 1024, CursorBatchSize: 100},
			numCols:  5,
			expected: 100,
		},
		{
			name:     "no field limit",
			opts:     queryOptions{MaxMemory: 10000, MaxFieldBytes: 0, CursorBatchSize: 100},
			numCols:  5,
			expected: 100,
		},
		{
			name:     "10MB budget, 1MB field, 10 cols",
			opts:     queryOptions{MaxMemory: 10 * 1024 * 1024, MaxFieldBytes: 1024 * 1024, CursorBatchSize: 100},
			numCols:  10,
			expected: 1,
		},
		{
			name:     "10MB budget, 1MB field, 2 cols",
			opts:     queryOptions{MaxMemory: 10 * 1024 * 1024, MaxFieldBytes: 1024 * 1024, CursorBatchSize: 100},
			numCols:  2,
			expected: 5,
		},
		{
			name:     "capped at cursor batch size",
			opts:     queryOptions{MaxMemory: 100 * 1024 * 1024, MaxFieldBytes: 1024, CursorBatchSize: 50},
			numCols:  1,
			expected: 50,
		},
		{
			name:     "minimum 1",
			opts:     queryOptions{MaxMemory: 1, MaxFieldBytes: 1024 * 1024, CursorBatchSize: 100},
			numCols:  10,
			expected: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, computeBatchSize(tt.opts, tt.numCols))
		})
	}
}

func TestDrainUntilReady(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildCommandComplete("SELECT 10"))
	buf.Write(buildReadyForQuery('I'))

	wc := pipeWireConn(buf.Bytes())
	defer wc.Close()

	ef, err := wc.drainUntilReady()
	require.NoError(t, err)
	assert.Nil(t, ef)
}

func TestDrainUntilReady_WithError(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(buildErrorResponse("42000", "syntax error"))
	buf.Write(buildReadyForQuery('I'))

	wc := pipeWireConn(buf.Bytes())
	defer wc.Close()

	ef, err := wc.drainUntilReady()
	require.NoError(t, err)
	require.NotNil(t, ef)
	assert.Equal(t, "42000", ef['C'])
}

func TestSendSimpleQuery(t *testing.T) {
	server, client := net.Pipe()
	wc := newWireConn(client)
	defer wc.Close()

	done := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(server)
		done <- data
	}()

	require.NoError(t, wc.sendSimpleQuery("SELECT 1"))
	wc.Close()

	select {
	case data := <-done:
		require.Greater(t, len(data), 5)
		assert.Equal(t, byte('Q'), data[0])
		msgLen := binary.BigEndian.Uint32(data[1:5])
		assert.Equal(t, uint32(4+len("SELECT 1")+1), msgLen)
		assert.Equal(t, "SELECT 1", string(data[5:5+len("SELECT 1")]))
		assert.Equal(t, byte(0), data[len(data)-1])
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}
