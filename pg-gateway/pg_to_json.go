package main

import (
	"fmt"
	"io"
	"strconv"
	"unicode/utf8"
)

// Well-known PG type OIDs for type-aware JSON conversion.
const (
	oidBool        uint32 = 16
	oidBytea       uint32 = 17
	oidName        uint32 = 19
	oidInt8        uint32 = 20
	oidInt2        uint32 = 21
	oidInt4        uint32 = 23
	oidOid         uint32 = 26
	oidJSON        uint32 = 114
	oidXML         uint32 = 142
	oidFloat4      uint32 = 700
	oidFloat8      uint32 = 701
	oidVarchar     uint32 = 1043
	oidDate        uint32 = 1082
	oidTimestamp   uint32 = 1114
	oidTimestamptz uint32 = 1184
	oidNumeric     uint32 = 1700
	oidUUID        uint32 = 2950
	oidJSONB       uint32 = 3802
)

func isNumericOID(oid uint32) bool {
	switch oid {
	case oidInt2, oidInt4, oidInt8, oidFloat4, oidFloat8, oidOid, oidNumeric:
		return true
	}
	return false
}

func isJSONOID(oid uint32) bool {
	return oid == oidJSON || oid == oidJSONB
}

// writeJSONValue writes a PG text-format value as the appropriate JSON token.
//   - bool → true/false
//   - int/float/numeric → raw JSON number (special floats become strings)
//   - json/jsonb → raw JSON passthrough
//   - everything else → JSON string
func writeJSONValue(w io.Writer, data []byte, typeOID uint32) error {
	if isJSONOID(typeOID) {
		_, err := w.Write(data)
		return err
	}
	if typeOID == oidBool {
		if len(data) > 0 && (data[0] == 't' || data[0] == 'T') {
			_, err := io.WriteString(w, "true")
			return err
		}
		_, err := io.WriteString(w, "false")
		return err
	}
	if isNumericOID(typeOID) {
		s := string(data)
		switch s {
		case "NaN", "Infinity", "-Infinity":
			return writeJSONString(w, data)
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			_, werr := w.Write(data)
			return werr
		}
	}
	return writeJSONString(w, data)
}

// writeJSONString writes data as a properly escaped JSON string (with surrounding quotes).
func writeJSONString(w io.Writer, data []byte) error {
	if _, err := io.WriteString(w, `"`); err != nil {
		return err
	}
	start := 0
	for i := 0; i < len(data); {
		b := data[i]
		if b < 0x20 || b == '"' || b == '\\' {
			if start < i {
				if _, err := w.Write(data[start:i]); err != nil {
					return err
				}
			}
			var esc string
			switch b {
			case '"':
				esc = `\"`
			case '\\':
				esc = `\\`
			case '\n':
				esc = `\n`
			case '\r':
				esc = `\r`
			case '\t':
				esc = `\t`
			case '\b':
				esc = `\b`
			case '\f':
				esc = `\f`
			default:
				esc = fmt.Sprintf(`\u%04x`, b)
			}
			if _, err := io.WriteString(w, esc); err != nil {
				return err
			}
			i++
			start = i
		} else if b < 0x80 {
			i++
		} else {
			_, size := utf8.DecodeRune(data[i:])
			if size == 1 && data[i] >= 0x80 {
				if start < i {
					if _, err := w.Write(data[start:i]); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintf(w, `\u%04x`, data[i]); err != nil {
					return err
				}
				i++
				start = i
			} else {
				i += size
			}
		}
	}
	if start < len(data) {
		if _, err := w.Write(data[start:]); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, `"`)
	return err
}

// writeJSONRow writes a complete NDJSON row line: {"row":{...},"truncated_fields":[...]}\n
// It writes directly to w from raw PG text-format column bytes.
func writeJSONRow(w io.Writer, fields []fieldDesc, columns []wireColumn) error {
	if _, err := io.WriteString(w, `{"row":{`); err != nil {
		return err
	}

	var truncated []string
	for i, fd := range fields {
		if i > 0 {
			if _, err := io.WriteString(w, ","); err != nil {
				return err
			}
		}
		if err := writeJSONString(w, []byte(fd.Name)); err != nil {
			return err
		}
		if _, err := io.WriteString(w, ":"); err != nil {
			return err
		}

		col := columns[i]
		switch {
		case col.OriginalLen == -1:
			if _, err := io.WriteString(w, "null"); err != nil {
				return err
			}
		case col.Truncated:
			truncated = append(truncated, fd.Name)
			truncatedFieldsTotal.Inc()
			marker := fmt.Sprintf("[truncated: %d bytes, showing first %d]%s",
				col.OriginalLen, len(col.Data), string(col.Data))
			if err := writeJSONString(w, []byte(marker)); err != nil {
				return err
			}
		default:
			if err := writeJSONValue(w, col.Data, fd.DataTypeOID); err != nil {
				return err
			}
		}
	}

	if _, err := io.WriteString(w, "}"); err != nil {
		return err
	}
	if len(truncated) > 0 {
		if _, err := io.WriteString(w, `,"truncated_fields":[`); err != nil {
			return err
		}
		for i, name := range truncated {
			if i > 0 {
				if _, err := io.WriteString(w, ","); err != nil {
					return err
				}
			}
			if err := writeJSONString(w, []byte(name)); err != nil {
				return err
			}
		}
		if _, err := io.WriteString(w, "]"); err != nil {
			return err
		}
	}
	_, err := io.WriteString(w, "}\n")
	return err
}
