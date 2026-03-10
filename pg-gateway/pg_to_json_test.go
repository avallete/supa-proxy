package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteJSONValue_Bool(t *testing.T) {
	var buf bytes.Buffer

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("t"), oidBool))
	assert.Equal(t, "true", buf.String())

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("f"), oidBool))
	assert.Equal(t, "false", buf.String())
}

func TestWriteJSONValue_Int(t *testing.T) {
	for _, tc := range []struct {
		oid  uint32
		data string
	}{
		{oidInt2, "42"},
		{oidInt4, "-1"},
		{oidInt8, "9223372036854775807"},
	} {
		var buf bytes.Buffer
		require.NoError(t, writeJSONValue(&buf, []byte(tc.data), tc.oid))
		assert.Equal(t, tc.data, buf.String())
	}
}

func TestWriteJSONValue_Float(t *testing.T) {
	var buf bytes.Buffer

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("3.14"), oidFloat8))
	assert.Equal(t, "3.14", buf.String())

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("NaN"), oidFloat8))
	assert.Equal(t, `"NaN"`, buf.String())

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("Infinity"), oidFloat4))
	assert.Equal(t, `"Infinity"`, buf.String())

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("-Infinity"), oidFloat4))
	assert.Equal(t, `"-Infinity"`, buf.String())
}

func TestWriteJSONValue_Numeric(t *testing.T) {
	var buf bytes.Buffer

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte("123.456"), oidNumeric))
	assert.Equal(t, "123.456", buf.String())
}

func TestWriteJSONValue_JSON(t *testing.T) {
	raw := `{"key":"value","num":42}`
	var buf bytes.Buffer
	require.NoError(t, writeJSONValue(&buf, []byte(raw), oidJSON))
	assert.Equal(t, raw, buf.String())

	buf.Reset()
	require.NoError(t, writeJSONValue(&buf, []byte(raw), oidJSONB))
	assert.Equal(t, raw, buf.String())
}

func TestWriteJSONValue_Text(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeJSONValue(&buf, []byte("hello world"), 25))
	assert.Equal(t, `"hello world"`, buf.String())
}

func TestWriteJSONString_Escaping(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"plain", "hello", `"hello"`},
		{"quotes", `say "hi"`, `"say \"hi\""`},
		{"backslash", `a\b`, `"a\\b"`},
		{"newline", "line1\nline2", `"line1\nline2"`},
		{"tab", "col1\tcol2", `"col1\tcol2"`},
		{"null byte", "a\x00b", `"a\u0000b"`},
		{"unicode", "café", `"café"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, writeJSONString(&buf, []byte(tc.input)))
			assert.Equal(t, tc.expected, buf.String())
			var decoded string
			require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
			assert.Equal(t, tc.input, decoded)
		})
	}
}

func TestWriteJSONRow(t *testing.T) {
	fields := []fieldDesc{
		{Name: "id", DataTypeOID: oidInt4},
		{Name: "name", DataTypeOID: 25},
		{Name: "active", DataTypeOID: oidBool},
		{Name: "score", DataTypeOID: oidFloat8},
		{Name: "meta", DataTypeOID: oidJSONB},
		{Name: "nullable", DataTypeOID: 25},
	}
	columns := []wireColumn{
		{Data: []byte("42"), OriginalLen: 2},
		{Data: []byte("alice"), OriginalLen: 5},
		{Data: []byte("t"), OriginalLen: 1},
		{Data: []byte("3.14"), OriginalLen: 4},
		{Data: []byte(`{"k":"v"}`), OriginalLen: 9},
		{OriginalLen: -1}, // NULL
	}

	var buf bytes.Buffer
	require.NoError(t, writeJSONRow(&buf, fields, columns))

	line := strings.TrimSuffix(buf.String(), "\n")
	var parsed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(line), &parsed))

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(parsed["row"], &row))

	assert.JSONEq(t, `42`, string(row["id"]))
	assert.JSONEq(t, `"alice"`, string(row["name"]))
	assert.JSONEq(t, `true`, string(row["active"]))
	assert.JSONEq(t, `3.14`, string(row["score"]))
	assert.JSONEq(t, `{"k":"v"}`, string(row["meta"]))
	assert.JSONEq(t, `null`, string(row["nullable"]))
}

func TestWriteJSONRow_WithTruncation(t *testing.T) {
	fields := []fieldDesc{
		{Name: "small", DataTypeOID: 25},
		{Name: "big", DataTypeOID: 25},
	}
	columns := []wireColumn{
		{Data: []byte("ok"), OriginalLen: 2},
		{Data: []byte("abc"), OriginalLen: 10000, Truncated: true},
	}

	var buf bytes.Buffer
	require.NoError(t, writeJSONRow(&buf, fields, columns))

	line := strings.TrimSuffix(buf.String(), "\n")
	var parsed map[string]json.RawMessage
	require.NoError(t, json.Unmarshal([]byte(line), &parsed))

	var truncatedFields []string
	require.NoError(t, json.Unmarshal(parsed["truncated_fields"], &truncatedFields))
	assert.Equal(t, []string{"big"}, truncatedFields)

	var row map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(parsed["row"], &row))

	var bigVal string
	require.NoError(t, json.Unmarshal(row["big"], &bigVal))
	assert.True(t, strings.HasPrefix(bigVal, "[truncated: 10000 bytes, showing first 3]abc"))
}

func TestWriteJSONRow_EmptyRow(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, writeJSONRow(&buf, nil, nil))
	assert.Equal(t, "{\"row\":{}}\n", buf.String())
}
