package main

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf16"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type BoundParam struct {
	cType     int16
	sqlType   int16
	valuePtr  unsafe.Pointer
	bufLen    int
	lenIndPtr unsafe.Pointer // *SQLLEN — read at execute time
}

type Statement struct {
	connID       uintptr
	stmt         adbc.Statement
	query        string
	prepared     bool
	result       *ResultSet
	rowsAffected int64
	diags        DiagnosticRecords
	params       map[int]*BoundParam // 1-based param number → binding
}

type ResultSet struct {
	reader       array.RecordReader
	schema       *arrow.Schema
	currentBatch arrow.Record
	rowIndex     int64
	batchRows    int64
	done         bool
	// Raw data mode (for catalog functions — avoids Arrow memory GC issues across CGO calls)
	rawRows     [][]any
	rawRowCount int64
}

// convertPlaceholders replaces ODBC ? parameter markers with $1, $2, ... markers
// that are supported by both DuckDB and PostgreSQL ADBC drivers.
// Respects single-quoted string literals.
func convertPlaceholders(sql string) string {
	var result strings.Builder
	result.Grow(len(sql) + 16)
	paramNum := 0
	inQuote := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			inQuote = !inQuote
			result.WriteByte(ch)
		} else if ch == '?' && !inQuote {
			paramNum++
			fmt.Fprintf(&result, "$%d", paramNum)
		} else {
			result.WriteByte(ch)
		}
	}
	if paramNum == 0 {
		return sql // no placeholders, return original
	}
	return result.String()
}

func (s *Statement) ensureStatement(conn *Connection) error {
	if s.stmt != nil {
		return nil
	}
	if conn.conn == nil {
		return fmt.Errorf("connection not open")
	}
	stmt, err := conn.conn.NewStatement()
	if err != nil {
		return err
	}
	s.stmt = stmt
	return nil
}

func (s *Statement) execDirect(conn *Connection, query string) error {
	s.closeResult()
	s.diags.clear()

	if err := s.ensureStatement(conn); err != nil {
		return err
	}

	if err := s.stmt.SetSqlQuery(convertPlaceholders(query)); err != nil {
		return err
	}

	reader, nRows, err := s.stmt.ExecuteQuery(context.Background())
	if err != nil {
		return err
	}

	s.query = query
	s.rowsAffected = nRows
	if reader != nil {
		s.result = &ResultSet{
			reader: reader,
			schema: reader.Schema(),
		}
	}
	return nil
}

func (s *Statement) execUpdate(conn *Connection) error {
	s.closeResult()
	s.diags.clear()

	if err := s.ensureStatement(conn); err != nil {
		return err
	}

	n, err := s.stmt.ExecuteUpdate(context.Background())
	if err != nil {
		return err
	}
	s.rowsAffected = n
	return nil
}

func (s *Statement) numResultCols() int {
	if s.result == nil || s.result.schema == nil {
		return 0
	}
	return len(s.result.schema.Fields())
}

func (s *Statement) describeCol(colNum int) (name string, sqlType int16, colSize int, decDigits int16, nullable int16, err error) {
	if s.result == nil || s.result.schema == nil {
		err = fmt.Errorf("no result set")
		return
	}
	fields := s.result.schema.Fields()
	if colNum < 1 || colNum > len(fields) {
		err = fmt.Errorf("column number %d out of range [1, %d]", colNum, len(fields))
		return
	}
	f := fields[colNum-1]
	name = f.Name
	sqlType = arrowTypeToSQLType(f.Type)
	colSize = arrowTypeColumnSize(f.Type)
	decDigits = arrowTypeDecimalDigits(f.Type)
	nullable = arrowTypeNullable(f)
	return
}

// fetch advances to the next row. Returns false when no more data.
func (s *Statement) fetch() bool {
	if s.result == nil || s.result.done {
		return false
	}

	rs := s.result

	// Raw data mode (catalog functions)
	if rs.rawRows != nil {
		rs.rowIndex++
		if rs.rowIndex >= rs.rawRowCount {
			rs.done = true
			return false
		}
		return true
	}

	// Arrow mode: advance within current batch
	if rs.currentBatch != nil {
		rs.rowIndex++
		if rs.rowIndex < rs.batchRows {
			return true
		}
		// Need next batch
		rs.currentBatch.Release()
		rs.currentBatch = nil
	}

	// Get next batch from reader
	if !rs.reader.Next() {
		rs.done = true
		return false
	}

	rs.currentBatch = rs.reader.Record()
	rs.currentBatch.Retain()
	rs.batchRows = rs.currentBatch.NumRows()
	rs.rowIndex = 0
	return rs.batchRows > 0
}

// getData reads the value of a column at the current row into a C buffer.
// targetType is the ODBC C type requested by the caller.
// Returns the data length, whether the value is null, whether it was truncated, and any error.
// TODO: support chunked reads — track per-column offset so repeated SQLGetData calls
// return successive chunks of the same value (needed for large strings/BLOBs).
func (s *Statement) getData(colNum int, targetType int16, buf unsafe.Pointer, bufLen int) (dataLen int, isNull bool, truncated bool, err error) {
	if s.result == nil {
		err = fmt.Errorf("no current row")
		return
	}

	rs := s.result

	// Raw data mode (catalog functions)
	if rs.rawRows != nil {
		row := int(rs.rowIndex)
		if row < 0 || row >= len(rs.rawRows) {
			err = fmt.Errorf("no current row")
			return
		}
		rowData := rs.rawRows[row]
		if colNum < 1 || colNum > len(rowData) {
			err = fmt.Errorf("column %d out of range", colNum)
			return
		}
		val := rowData[colNum-1]
		if val == nil {
			isNull = true
			return
		}
		dataLen, err = writeRawValue(val, targetType, buf, bufLen)
		if err == nil && buf != nil && bufLen > 0 {
			switch targetType {
			case SQL_C_CHAR, SQL_C_DEFAULT:
				truncated = dataLen > bufLen-1
			case SQL_C_WCHAR:
				truncated = dataLen > bufLen-2
			}
		}
		return
	}

	// Arrow mode
	if rs.currentBatch == nil {
		err = fmt.Errorf("no current row")
		return
	}

	if colNum < 1 || colNum > int(rs.currentBatch.NumCols()) {
		err = fmt.Errorf("column %d out of range", colNum)
		return
	}

	col := rs.currentBatch.Column(colNum - 1)
	row := int(rs.rowIndex)

	if col.IsNull(row) {
		isNull = true
		return
	}

	dataLen, err = writeArrowValue(col, row, targetType, buf, bufLen)
	if err == nil && buf != nil && bufLen > 0 {
		switch targetType {
		case SQL_C_CHAR, SQL_C_DEFAULT:
			truncated = dataLen > bufLen-1 // account for null terminator
		case SQL_C_WCHAR:
			truncated = dataLen > bufLen-2 // account for 2-byte null terminator
		}
	}
	return
}

// writeArrowValue converts an Arrow array value to ODBC C format and writes to buf.
// Returns the number of bytes written (or needed if buffer is too small).
func writeArrowValue(col arrow.Array, row int, targetType int16, buf unsafe.Pointer, bufLen int) (int, error) {
	switch targetType {
	case SQL_C_CHAR, SQL_C_DEFAULT:
		return writeAsString(col, row, buf, bufLen)

	case SQL_C_WCHAR:
		return writeAsWideString(col, row, buf, bufLen)

	case SQL_C_LONG, SQL_C_SLONG, SQL_C_ULONG:
		if bufLen < 4 {
			return 4, nil
		}
		val := getInt64Value(col, row)
		*(*int32)(buf) = int32(val)
		return 4, nil

	case SQL_C_SBIGINT, SQL_C_UBIGINT:
		if bufLen < 8 {
			return 8, nil
		}
		val := getInt64Value(col, row)
		*(*int64)(buf) = val
		return 8, nil

	case SQL_C_SHORT, SQL_C_SSHORT, SQL_C_USHORT:
		if bufLen < 2 {
			return 2, nil
		}
		val := getInt64Value(col, row)
		*(*int16)(buf) = int16(val)
		return 2, nil

	case SQL_C_STINYINT, SQL_C_UTINYINT:
		if bufLen < 1 {
			return 1, nil
		}
		val := getInt64Value(col, row)
		*(*int8)(buf) = int8(val)
		return 1, nil

	case SQL_C_FLOAT:
		if bufLen < 4 {
			return 4, nil
		}
		val := getFloat64Value(col, row)
		*(*float32)(buf) = float32(val)
		return 4, nil

	case SQL_C_DOUBLE:
		if bufLen < 8 {
			return 8, nil
		}
		val := getFloat64Value(col, row)
		*(*float64)(buf) = val
		return 8, nil

	case SQL_C_BIT:
		if bufLen < 1 {
			return 1, nil
		}
		val := getBoolValue(col, row)
		if val {
			*(*byte)(buf) = 1
		} else {
			*(*byte)(buf) = 0
		}
		return 1, nil

	case SQL_C_TYPE_DATE:
		if bufLen < 6 {
			return 6, nil
		}
		t := getTimeValue(col, row)
		dateStruct := (*sqlDateStruct)(buf)
		dateStruct.year = int16(t.Year())
		dateStruct.month = uint16(t.Month())
		dateStruct.day = uint16(t.Day())
		return 6, nil

	case SQL_C_TYPE_TIME:
		if bufLen < 6 {
			return 6, nil
		}
		t := getTimeValue(col, row)
		timeStruct := (*sqlTimeStruct)(buf)
		timeStruct.hour = uint16(t.Hour())
		timeStruct.minute = uint16(t.Minute())
		timeStruct.second = uint16(t.Second())
		return 6, nil

	case SQL_C_TYPE_TIMESTAMP:
		if bufLen < 16 {
			return 16, nil
		}
		t := getTimeValue(col, row)
		tsStruct := (*sqlTimestampStruct)(buf)
		tsStruct.year = int16(t.Year())
		tsStruct.month = uint16(t.Month())
		tsStruct.day = uint16(t.Day())
		tsStruct.hour = uint16(t.Hour())
		tsStruct.minute = uint16(t.Minute())
		tsStruct.second = uint16(t.Second())
		tsStruct.fraction = uint32(t.Nanosecond())
		return 16, nil

	default:
		return writeAsString(col, row, buf, bufLen)
	}
}

// SQL date/time struct layouts matching ODBC C structs
type sqlDateStruct struct {
	year  int16
	month uint16
	day   uint16
}

type sqlTimeStruct struct {
	hour   uint16
	minute uint16
	second uint16
}

type sqlTimestampStruct struct {
	year     int16
	month    uint16
	day      uint16
	hour     uint16
	minute   uint16
	second   uint16
	fraction uint32
}

func writeAsWideString(col arrow.Array, row int, buf unsafe.Pointer, bufLen int) (int, error) {
	str := col.ValueStr(row)
	runes := utf16.Encode([]rune(str))
	dataLen := len(runes) * 2 // bytes needed for UTF-16LE data (excluding null terminator)
	if buf == nil || bufLen <= 0 {
		return dataLen, nil
	}
	dst := unsafe.Slice((*byte)(buf), bufLen)
	written := 0
	for _, r := range runes {
		if written+2 > bufLen-2 { // reserve 2 bytes for null terminator
			break
		}
		dst[written] = byte(r)
		dst[written+1] = byte(r >> 8)
		written += 2
	}
	// null terminator (2 bytes for UTF-16)
	if written+2 <= bufLen {
		dst[written] = 0
		dst[written+1] = 0
	}
	return dataLen, nil
}

func writeAsString(col arrow.Array, row int, buf unsafe.Pointer, bufLen int) (int, error) {
	str := col.ValueStr(row)
	if buf == nil || bufLen <= 0 {
		return len(str), nil
	}
	dst := unsafe.Slice((*byte)(buf), bufLen)
	n := copy(dst, str)
	if n < bufLen {
		dst[n] = 0
	}
	return len(str), nil
}

func getInt64Value(col arrow.Array, row int) int64 {
	switch c := col.(type) {
	case *array.Int8:
		return int64(c.Value(row))
	case *array.Int16:
		return int64(c.Value(row))
	case *array.Int32:
		return int64(c.Value(row))
	case *array.Int64:
		return c.Value(row)
	case *array.Uint8:
		return int64(c.Value(row))
	case *array.Uint16:
		return int64(c.Value(row))
	case *array.Uint32:
		return int64(c.Value(row))
	case *array.Uint64:
		return int64(c.Value(row))
	case *array.Float32:
		return int64(c.Value(row))
	case *array.Float64:
		return int64(c.Value(row))
	case *array.Boolean:
		if c.Value(row) {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func getFloat64Value(col arrow.Array, row int) float64 {
	switch c := col.(type) {
	case *array.Float32:
		return float64(c.Value(row))
	case *array.Float64:
		return c.Value(row)
	case *array.Int8:
		return float64(c.Value(row))
	case *array.Int16:
		return float64(c.Value(row))
	case *array.Int32:
		return float64(c.Value(row))
	case *array.Int64:
		return float64(c.Value(row))
	case *array.Uint8:
		return float64(c.Value(row))
	case *array.Uint16:
		return float64(c.Value(row))
	case *array.Uint32:
		return float64(c.Value(row))
	case *array.Uint64:
		return float64(c.Value(row))
	default:
		return 0
	}
}

func getBoolValue(col arrow.Array, row int) bool {
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(row)
	default:
		return getInt64Value(col, row) != 0
	}
}

func getTimeValue(col arrow.Array, row int) time.Time {
	switch c := col.(type) {
	case *array.Date32:
		return c.Value(row).ToTime()
	case *array.Date64:
		return c.Value(row).ToTime()
	case *array.Time32:
		return time.Unix(0, 0).Add(time.Duration(c.Value(row)) * time.Second)
	case *array.Time64:
		return time.Unix(0, 0).Add(time.Duration(c.Value(row)) * time.Microsecond)
	case *array.Timestamp:
		toTime, _ := c.DataType().(*arrow.TimestampType).GetToTimeFunc()
		return toTime(c.Value(row))
	default:
		return time.Time{}
	}
}

// writeStringToBuffer writes a Go string to a C buffer in the requested ODBC C type format.
func writeStringToBuffer(str string, targetType int16, buf unsafe.Pointer, bufLen int) (int, error) {
	switch targetType {
	case SQL_C_WCHAR:
		return writeWCharFromString(str, buf, bufLen), nil
	default:
		return writeCharFromString(str, buf, bufLen), nil
	}
}

func writeCharFromString(str string, buf unsafe.Pointer, bufLen int) int {
	if buf == nil || bufLen <= 0 {
		return len(str)
	}
	dst := unsafe.Slice((*byte)(buf), bufLen)
	n := copy(dst, str)
	if n < bufLen {
		dst[n] = 0
	}
	return len(str)
}

func writeWCharFromString(str string, buf unsafe.Pointer, bufLen int) int {
	runes := utf16.Encode([]rune(str))
	dataLen := len(runes) * 2
	if buf == nil || bufLen <= 0 {
		return dataLen
	}
	dst := unsafe.Slice((*byte)(buf), bufLen)
	written := 0
	for _, r := range runes {
		if written+2 > bufLen-2 { // reserve 2 bytes for null terminator
			break
		}
		dst[written] = byte(r)
		dst[written+1] = byte(r >> 8)
		written += 2
	}
	if written+2 <= bufLen {
		dst[written] = 0
		dst[written+1] = 0
	}
	return dataLen
}

func (s *Statement) bindParam(paramNum int, cType, sqlType int16, valuePtr unsafe.Pointer, bufLen int, lenIndPtr unsafe.Pointer) {
	if s.params == nil {
		s.params = make(map[int]*BoundParam)
	}
	s.params[paramNum] = &BoundParam{
		cType:     cType,
		sqlType:   sqlType,
		valuePtr:  valuePtr,
		bufLen:    bufLen,
		lenIndPtr: lenIndPtr,
	}
}

func (s *Statement) resetParams() {
	s.params = nil
}

// numParams returns the number of parameters in the prepared statement.
func (s *Statement) numParams() int {
	if s.stmt != nil {
		schema, err := s.stmt.GetParameterSchema()
		if err == nil && schema != nil {
			return len(schema.Fields())
		}
	}
	// Fallback: count '?' in query (simple, doesn't handle quoted strings)
	return strings.Count(s.query, "?")
}

// bindParamsToStatement reads bound C parameter values and calls ADBC Bind.
func (s *Statement) bindParamsToStatement() error {
	if len(s.params) == 0 {
		return nil
	}

	nParams := len(s.params)

	// Build Arrow schema for parameters.
	// Try GetParameterSchema first, but fall back to bound SQL types
	// if unavailable or if it returns Null types (common with DuckDB).
	var schema *arrow.Schema
	if s.stmt != nil {
		schema, _ = s.stmt.GetParameterSchema()
	}

	// Build/fix schema using bound parameter SQL types
	fields := make([]arrow.Field, nParams)
	for i := 0; i < nParams; i++ {
		// Start from driver schema if available
		if schema != nil && i < len(schema.Fields()) && schema.Fields()[i].Type.ID() != arrow.NULL {
			fields[i] = schema.Fields()[i]
		} else {
			// Infer from bound param SQL type
			p := s.params[i+1]
			if p == nil {
				fields[i] = arrow.Field{Name: fmt.Sprintf("%d", i), Type: arrow.BinaryTypes.String, Nullable: true}
			} else {
				fields[i] = arrow.Field{Name: fmt.Sprintf("%d", i), Type: sqlTypeToArrowType(p.sqlType), Nullable: true}
			}
		}
	}
	schema = arrow.NewSchema(fields, nil)

	// Build 1-row record from bound parameters
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	for i := 0; i < nParams; i++ {
		p := s.params[i+1]
		if p == nil {
			bldr.Field(i).AppendNull()
			continue
		}

		val, isNull := readCParamValue(p)
		if isNull {
			bldr.Field(i).AppendNull()
			continue
		}

		if err := appendValueToBuilder(bldr.Field(i), val); err != nil {
			return fmt.Errorf("param %d: %w", i+1, err)
		}
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	return s.stmt.Bind(context.Background(), rec)
}

// readCParamValue reads the C value buffer for a bound parameter.
// Returns (value, isNull).
func readCParamValue(p *BoundParam) (any, bool) {
	// Check null indicator
	if p.lenIndPtr != nil {
		lenInd := *(*int64)(p.lenIndPtr)
		if lenInd == -1 { // SQL_NULL_DATA
			return nil, true
		}
	}

	if p.valuePtr == nil {
		return nil, true
	}

	switch p.cType {
	case SQL_C_SBIGINT:
		return *(*int64)(p.valuePtr), false
	case SQL_C_UBIGINT:
		return int64(*(*uint64)(p.valuePtr)), false
	case SQL_C_LONG, SQL_C_SLONG:
		return int64(*(*int32)(p.valuePtr)), false
	case SQL_C_ULONG:
		return int64(*(*uint32)(p.valuePtr)), false
	case SQL_C_SHORT, SQL_C_SSHORT:
		return int64(*(*int16)(p.valuePtr)), false
	case SQL_C_USHORT:
		return int64(*(*uint16)(p.valuePtr)), false
	case SQL_C_STINYINT:
		return int64(*(*int8)(p.valuePtr)), false
	case SQL_C_UTINYINT:
		return int64(*(*uint8)(p.valuePtr)), false
	case SQL_C_DOUBLE:
		return *(*float64)(p.valuePtr), false
	case SQL_C_FLOAT:
		return float64(*(*float32)(p.valuePtr)), false
	case SQL_C_BIT:
		if *(*byte)(p.valuePtr) != 0 {
			return true, false
		}
		return false, false
	case SQL_C_CHAR, SQL_C_DEFAULT:
		// Read string with length from lenIndPtr
		strLen := p.bufLen
		if p.lenIndPtr != nil {
			n := *(*int64)(p.lenIndPtr)
			if n >= 0 {
				strLen = int(n)
			}
		}
		if strLen > p.bufLen && p.bufLen > 0 {
			strLen = p.bufLen
		}
		bytes := unsafe.Slice((*byte)(p.valuePtr), strLen)
		return string(bytes), false
	case SQL_C_WCHAR:
		// Read UTF-16LE string
		byteLen := p.bufLen
		if p.lenIndPtr != nil {
			n := *(*int64)(p.lenIndPtr)
			if n >= 0 {
				byteLen = int(n)
			}
		}
		if byteLen > p.bufLen && p.bufLen > 0 {
			byteLen = p.bufLen
		}
		nUnits := byteLen / 2
		if nUnits == 0 {
			return "", false
		}
		u16 := unsafe.Slice((*uint16)(p.valuePtr), nUnits)
		runes := utf16.Decode(u16)
		return string(runes), false
	case SQL_C_BINARY:
		dataLen := p.bufLen
		if p.lenIndPtr != nil {
			n := *(*int64)(p.lenIndPtr)
			if n >= 0 {
				dataLen = int(n)
			}
		}
		bytes := make([]byte, dataLen)
		copy(bytes, unsafe.Slice((*byte)(p.valuePtr), dataLen))
		return bytes, false
	default:
		// Treat as string
		strLen := p.bufLen
		if p.lenIndPtr != nil {
			n := *(*int64)(p.lenIndPtr)
			if n >= 0 {
				strLen = int(n)
			}
		}
		bytes := unsafe.Slice((*byte)(p.valuePtr), strLen)
		return string(bytes), false
	}
}

// appendValueToBuilder appends a Go value to an Arrow array builder,
// converting types as needed.
func appendValueToBuilder(b array.Builder, val any) error {
	switch bldr := b.(type) {
	case *array.Int8Builder:
		bldr.Append(int8(toInt64(val)))
	case *array.Int16Builder:
		bldr.Append(int16(toInt64(val)))
	case *array.Int32Builder:
		bldr.Append(int32(toInt64(val)))
	case *array.Int64Builder:
		bldr.Append(toInt64(val))
	case *array.Uint8Builder:
		bldr.Append(uint8(toInt64(val)))
	case *array.Uint16Builder:
		bldr.Append(uint16(toInt64(val)))
	case *array.Uint32Builder:
		bldr.Append(uint32(toInt64(val)))
	case *array.Uint64Builder:
		bldr.Append(uint64(toInt64(val)))
	case *array.Float32Builder:
		bldr.Append(float32(toFloat64(val)))
	case *array.Float64Builder:
		bldr.Append(toFloat64(val))
	case *array.StringBuilder:
		bldr.Append(toString(val))
	case *array.BinaryBuilder:
		switch v := val.(type) {
		case []byte:
			bldr.Append(v)
		case string:
			bldr.Append([]byte(v))
		default:
			bldr.Append([]byte(fmt.Sprint(val)))
		}
	case *array.BooleanBuilder:
		switch v := val.(type) {
		case bool:
			bldr.Append(v)
		default:
			bldr.Append(toInt64(val) != 0)
		}
	case *array.Date32Builder:
		bldr.Append(arrow.Date32FromTime(toTime(val)))
	case *array.TimestampBuilder:
		t := toTime(val)
		bldr.Append(arrow.Timestamp(t.UnixMicro()))
	default:
		// Generic fallback: try string
		if sb, ok := b.(*array.StringBuilder); ok {
			sb.Append(toString(val))
		} else {
			return fmt.Errorf("unsupported builder type %T", b)
		}
	}
	return nil
}

func toInt64(val any) int64 {
	switch v := val.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int16:
		return int64(v)
	case int8:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	case bool:
		if v {
			return 1
		}
		return 0
	default:
		return 0
	}
}

func toFloat64(val any) float64 {
	switch v := val.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	case int16:
		return float64(v)
	default:
		return 0
	}
}

func toString(val any) string {
	switch v := val.(type) {
	case string:
		return v
	default:
		return fmt.Sprint(v)
	}
}

func toTime(val any) time.Time {
	switch v := val.(type) {
	case time.Time:
		return v
	case string:
		t, _ := time.Parse("2006-01-02 15:04:05", v)
		return t
	default:
		return time.Time{}
	}
}

// writeRawValue writes a Go value (from raw rows) to a C buffer.
func writeRawValue(val any, targetType int16, buf unsafe.Pointer, bufLen int) (int, error) {
	switch v := val.(type) {
	case string:
		return writeStringToBuffer(v, targetType, buf, bufLen)
	case int16:
		return writeInt16ToBuffer(v, targetType, buf, bufLen)
	case int32:
		return writeInt32ToBuffer(v, targetType, buf, bufLen)
	default:
		return writeStringToBuffer(fmt.Sprint(val), targetType, buf, bufLen)
	}
}

func writeInt16ToBuffer(val int16, targetType int16, buf unsafe.Pointer, bufLen int) (int, error) {
	switch targetType {
	case SQL_C_SHORT, SQL_C_SSHORT, SQL_C_USHORT:
		if bufLen < 2 {
			return 2, nil
		}
		*(*int16)(buf) = val
		return 2, nil
	case SQL_C_LONG, SQL_C_SLONG, SQL_C_ULONG:
		if bufLen < 4 {
			return 4, nil
		}
		*(*int32)(buf) = int32(val)
		return 4, nil
	default:
		return writeStringToBuffer(fmt.Sprintf("%d", val), targetType, buf, bufLen)
	}
}

func writeInt32ToBuffer(val int32, targetType int16, buf unsafe.Pointer, bufLen int) (int, error) {
	switch targetType {
	case SQL_C_LONG, SQL_C_SLONG, SQL_C_ULONG:
		if bufLen < 4 {
			return 4, nil
		}
		*(*int32)(buf) = val
		return 4, nil
	case SQL_C_SHORT, SQL_C_SSHORT, SQL_C_USHORT:
		if bufLen < 2 {
			return 2, nil
		}
		*(*int16)(buf) = int16(val)
		return 2, nil
	default:
		return writeStringToBuffer(fmt.Sprintf("%d", val), targetType, buf, bufLen)
	}
}

func (s *Statement) closeResult() {
	if s.result != nil {
		if s.result.currentBatch != nil {
			s.result.currentBatch.Release()
			s.result.currentBatch = nil
		}
		if s.result.reader != nil {
			s.result.reader.Release()
			s.result.reader = nil
		}
		s.result = nil
	}
}

func (s *Statement) close() error {
	s.closeResult()
	s.resetParams()
	if s.stmt != nil {
		err := s.stmt.Close()
		s.stmt = nil
		return err
	}
	return nil
}
