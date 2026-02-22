package main

/*
#include <sql.h>
#include <sqlext.h>
#include <sqltypes.h>
#include <string.h>
*/
import "C"
import (
	"context"
	"unsafe"
)

// sqlCharToString converts a C SQLCHAR pointer with length to a Go string.
// If length is SQL_NTS (-3), the string is treated as null-terminated.
func sqlCharToString(s *C.SQLCHAR, length C.SQLINTEGER) string {
	if length == C.SQL_NTS {
		return C.GoString((*C.char)(unsafe.Pointer(s)))
	}
	return C.GoStringN((*C.char)(unsafe.Pointer(s)), C.int(length))
}

//export SQLAllocHandle
func SQLAllocHandle(handleType C.SQLSMALLINT, inputHandle C.SQLHANDLE, outputHandle *C.SQLHANDLE) C.SQLRETURN {
	switch handleType {
	case C.SQL_HANDLE_ENV:
		id, _ := registry.allocEnv()
		*outputHandle = C.SQLHANDLE(uintptr(id))
		return C.SQL_SUCCESS

	case C.SQL_HANDLE_DBC:
		envID := uintptr(inputHandle)
		env := registry.getEnv(envID)
		if env == nil {
			return C.SQL_INVALID_HANDLE
		}
		id, _ := registry.allocConn(envID)
		*outputHandle = C.SQLHANDLE(uintptr(id))
		return C.SQL_SUCCESS

	case C.SQL_HANDLE_STMT:
		connID := uintptr(inputHandle)
		conn := registry.getConn(connID)
		if conn == nil {
			return C.SQL_INVALID_HANDLE
		}
		id, _ := registry.allocStmt(connID)
		*outputHandle = C.SQLHANDLE(uintptr(id))
		return C.SQL_SUCCESS

	default:
		return C.SQL_ERROR
	}
}

//export SQLFreeHandle
func SQLFreeHandle(handleType C.SQLSMALLINT, handle C.SQLHANDLE) C.SQLRETURN {
	id := uintptr(handle)
	switch handleType {
	case C.SQL_HANDLE_ENV:
		if registry.getEnv(id) == nil {
			return C.SQL_INVALID_HANDLE
		}
		registry.freeEnv(id)
		return C.SQL_SUCCESS

	case C.SQL_HANDLE_DBC:
		conn := registry.getConn(id)
		if conn == nil {
			return C.SQL_INVALID_HANDLE
		}
		conn.disconnect()
		registry.freeConn(id)
		return C.SQL_SUCCESS

	case C.SQL_HANDLE_STMT:
		stmt := registry.getStmt(id)
		if stmt == nil {
			return C.SQL_INVALID_HANDLE
		}
		stmt.close()
		registry.freeStmt(id)
		return C.SQL_SUCCESS

	default:
		return C.SQL_ERROR
	}
}

//export SQLSetEnvAttr
func SQLSetEnvAttr(envHandle C.SQLHENV, attribute C.SQLINTEGER, value C.SQLPOINTER, stringLength C.SQLINTEGER) C.SQLRETURN {
	env := registry.getEnv(uintptr(envHandle))
	if env == nil {
		return C.SQL_INVALID_HANDLE
	}
	env.diags.clear()

	switch attribute {
	case C.SQL_ATTR_ODBC_VERSION:
		env.odbcVersion = int(uintptr(value))
		return C.SQL_SUCCESS
	default:
		return C.SQL_SUCCESS // ignore unknown attributes
	}
}

//export SQLDriverConnect
func SQLDriverConnect(
	connHandle C.SQLHDBC,
	windowHandle C.SQLHWND,
	inConnStr *C.SQLCHAR,
	inConnStrLen C.SQLSMALLINT,
	outConnStr *C.SQLCHAR,
	outConnStrMax C.SQLSMALLINT,
	outConnStrLen *C.SQLSMALLINT,
	driverCompletion C.SQLUSMALLINT,
) C.SQLRETURN {
	conn := registry.getConn(uintptr(connHandle))
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}
	conn.diags.clear()

	connStr := sqlCharToString(inConnStr, C.SQLINTEGER(inConnStrLen))

	if err := conn.connect(connStr); err != nil {
		conn.diags.addErrorf("SQLDriverConnect: %v", err)
		return C.SQL_ERROR
	}

	// Copy connection string to output buffer if provided
	if outConnStr != nil && outConnStrMax > 0 {
		outBytes := []byte(connStr)
		maxCopy := int(outConnStrMax) - 1
		if len(outBytes) > maxCopy {
			outBytes = outBytes[:maxCopy]
		}
		dst := unsafe.Slice((*byte)(unsafe.Pointer(outConnStr)), int(outConnStrMax))
		n := copy(dst, outBytes)
		dst[n] = 0
		if outConnStrLen != nil {
			*outConnStrLen = C.SQLSMALLINT(len(connStr))
		}
	}

	return C.SQL_SUCCESS
}

//export SQLDisconnect
func SQLDisconnect(connHandle C.SQLHDBC) C.SQLRETURN {
	conn := registry.getConn(uintptr(connHandle))
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	if err := conn.disconnect(); err != nil {
		conn.diags.addErrorf("SQLDisconnect: %v", err)
		return C.SQL_ERROR
	}
	return C.SQL_SUCCESS
}

//export SQLExecDirect
func SQLExecDirect(stmtHandle C.SQLHSTMT, stmtText *C.SQLCHAR, textLength C.SQLINTEGER) C.SQLRETURN {
	stmtID := uintptr(stmtHandle)
	stmt := registry.getStmt(stmtID)
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	conn := registry.getConn(stmt.connID)
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	query := sqlCharToString(stmtText, textLength)

	if err := stmt.execDirect(conn, query); err != nil {
		stmt.diags.addErrorf("SQLExecDirect: %v", err)
		return C.SQL_ERROR
	}
	return C.SQL_SUCCESS
}

//export SQLNumResultCols
func SQLNumResultCols(stmtHandle C.SQLHSTMT, colCount *C.SQLSMALLINT) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}
	*colCount = C.SQLSMALLINT(stmt.numResultCols())
	return C.SQL_SUCCESS
}

//export SQLDescribeCol
func SQLDescribeCol(
	stmtHandle C.SQLHSTMT,
	colNumber C.SQLUSMALLINT,
	colName *C.SQLCHAR,
	bufferLength C.SQLSMALLINT,
	nameLengthPtr *C.SQLSMALLINT,
	dataTypePtr *C.SQLSMALLINT,
	colSizePtr *C.SQLULEN,
	decimalDigitsPtr *C.SQLSMALLINT,
	nullablePtr *C.SQLSMALLINT,
) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	name, sqlType, colSize, decDigits, nullable, err := stmt.describeCol(int(colNumber))
	if err != nil {
		stmt.diags.addErrorf("SQLDescribeCol: %v", err)
		return C.SQL_ERROR
	}

	if colName != nil && bufferLength > 0 {
		nameBytes := []byte(name)
		maxCopy := int(bufferLength) - 1
		if len(nameBytes) > maxCopy {
			nameBytes = nameBytes[:maxCopy]
		}
		dst := unsafe.Slice((*byte)(unsafe.Pointer(colName)), int(bufferLength))
		n := copy(dst, nameBytes)
		dst[n] = 0
	}
	if nameLengthPtr != nil {
		*nameLengthPtr = C.SQLSMALLINT(len(name))
	}
	if dataTypePtr != nil {
		*dataTypePtr = C.SQLSMALLINT(sqlType)
	}
	if colSizePtr != nil {
		*colSizePtr = C.SQLULEN(colSize)
	}
	if decimalDigitsPtr != nil {
		*decimalDigitsPtr = C.SQLSMALLINT(decDigits)
	}
	if nullablePtr != nil {
		*nullablePtr = C.SQLSMALLINT(nullable)
	}
	return C.SQL_SUCCESS
}

//export SQLFetch
func SQLFetch(stmtHandle C.SQLHSTMT) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	if !stmt.fetch() {
		return C.SQL_NO_DATA
	}
	return C.SQL_SUCCESS
}

//export SQLGetData
func SQLGetData(
	stmtHandle C.SQLHSTMT,
	colOrParamNum C.SQLUSMALLINT,
	targetType C.SQLSMALLINT,
	targetValue C.SQLPOINTER,
	bufferLength C.SQLLEN,
	strLenOrInd *C.SQLLEN,
) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	dataLen, isNull, truncated, err := stmt.getData(
		int(colOrParamNum),
		int16(targetType),
		unsafe.Pointer(targetValue),
		int(bufferLength),
	)
	if err != nil {
		stmt.diags.addErrorf("SQLGetData: %v", err)
		return C.SQL_ERROR
	}

	if strLenOrInd != nil {
		if isNull {
			*strLenOrInd = C.SQL_NULL_DATA
		} else {
			*strLenOrInd = C.SQLLEN(dataLen)
		}
	}
	if truncated {
		stmt.diags.add("01004", 0, "String data, right truncated")
		return C.SQL_SUCCESS_WITH_INFO
	}
	return C.SQL_SUCCESS
}

//export SQLCloseCursor
func SQLCloseCursor(stmtHandle C.SQLHSTMT) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}
	stmt.closeResult()
	return C.SQL_SUCCESS
}

//export SQLRowCount
func SQLRowCount(stmtHandle C.SQLHSTMT, rowCount *C.SQLLEN) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}
	if rowCount != nil {
		*rowCount = C.SQLLEN(stmt.rowsAffected)
	}
	return C.SQL_SUCCESS
}

//export SQLGetDiagRec
func SQLGetDiagRec(
	handleType C.SQLSMALLINT,
	handle C.SQLHANDLE,
	recNumber C.SQLSMALLINT,
	sqlState *C.SQLCHAR,
	nativeError *C.SQLINTEGER,
	messageText *C.SQLCHAR,
	bufferLength C.SQLSMALLINT,
	textLength *C.SQLSMALLINT,
) C.SQLRETURN {
	var diags *DiagnosticRecords
	id := uintptr(handle)

	switch handleType {
	case C.SQL_HANDLE_ENV:
		if env := registry.getEnv(id); env != nil {
			diags = &env.diags
		}
	case C.SQL_HANDLE_DBC:
		if conn := registry.getConn(id); conn != nil {
			diags = &conn.diags
		}
	case C.SQL_HANDLE_STMT:
		if stmt := registry.getStmt(id); stmt != nil {
			diags = &stmt.diags
		}
	}

	if diags == nil {
		return C.SQL_INVALID_HANDLE
	}

	rec := diags.get(int(recNumber))
	if rec == nil {
		return C.SQL_NO_DATA
	}

	if sqlState != nil {
		dst := unsafe.Slice((*byte)(unsafe.Pointer(sqlState)), 6)
		copy(dst, rec.sqlState[:])
		dst[5] = 0
	}
	if nativeError != nil {
		*nativeError = C.SQLINTEGER(rec.nativeError)
	}
	if messageText != nil && bufferLength > 0 {
		msg := []byte(rec.message)
		maxCopy := int(bufferLength) - 1
		if len(msg) > maxCopy {
			msg = msg[:maxCopy]
		}
		dst := unsafe.Slice((*byte)(unsafe.Pointer(messageText)), int(bufferLength))
		n := copy(dst, msg)
		dst[n] = 0
		if textLength != nil {
			*textLength = C.SQLSMALLINT(len(rec.message))
		}
	}
	return C.SQL_SUCCESS
}

//export SQLFreeStmt
func SQLFreeStmt(stmtHandle C.SQLHSTMT, option C.SQLUSMALLINT) C.SQLRETURN {
	stmtID := uintptr(stmtHandle)
	stmt := registry.getStmt(stmtID)
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	switch option {
	case C.SQL_CLOSE:
		stmt.closeResult()
	case C.SQL_DROP:
		stmt.close()
		registry.freeStmt(stmtID)
	case C.SQL_UNBIND:
		// No-op for now (no column bindings yet)
	case C.SQL_RESET_PARAMS:
		// No-op for now (no parameter bindings yet)
	}
	return C.SQL_SUCCESS
}

//export SQLEndTran
func SQLEndTran(handleType C.SQLSMALLINT, handle C.SQLHANDLE, completionType C.SQLSMALLINT) C.SQLRETURN {
	if handleType != C.SQL_HANDLE_DBC {
		return C.SQL_ERROR
	}

	conn := registry.getConn(uintptr(handle))
	if conn == nil || conn.conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	var err error
	switch completionType {
	case C.SQL_COMMIT:
		err = conn.conn.Commit(context.Background())
	case C.SQL_ROLLBACK:
		err = conn.conn.Rollback(context.Background())
	default:
		return C.SQL_ERROR
	}

	if err != nil {
		conn.diags.addErrorf("SQLEndTran: %v", err)
		return C.SQL_ERROR
	}
	return C.SQL_SUCCESS
}

//export SQLSetConnectAttr
func SQLSetConnectAttr(connHandle C.SQLHDBC, attribute C.SQLINTEGER, value C.SQLPOINTER, stringLength C.SQLINTEGER) C.SQLRETURN {
	conn := registry.getConn(uintptr(connHandle))
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}
	// Accept but ignore most attributes for now
	return C.SQL_SUCCESS
}

//export SQLGetInfo
func SQLGetInfo(connHandle C.SQLHDBC, infoType C.SQLUSMALLINT, infoValue C.SQLPOINTER, bufferLength C.SQLSMALLINT, stringLength *C.SQLSMALLINT) C.SQLRETURN {
	conn := registry.getConn(uintptr(connHandle))
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	writeString := func(s string) C.SQLRETURN {
		if infoValue != nil && bufferLength > 0 {
			dst := unsafe.Slice((*byte)(unsafe.Pointer(infoValue)), int(bufferLength))
			n := copy(dst, s)
			if n < int(bufferLength) {
				dst[n] = 0
			}
		}
		if stringLength != nil {
			*stringLength = C.SQLSMALLINT(len(s))
		}
		return C.SQL_SUCCESS
	}

	writeUint16 := func(v uint16) C.SQLRETURN {
		if infoValue != nil {
			*(*C.SQLUSMALLINT)(infoValue) = C.SQLUSMALLINT(v)
		}
		if stringLength != nil {
			*stringLength = 2
		}
		return C.SQL_SUCCESS
	}

	writeUint32 := func(v uint32) C.SQLRETURN {
		if infoValue != nil {
			*(*C.SQLUINTEGER)(infoValue) = C.SQLUINTEGER(v)
		}
		if stringLength != nil {
			*stringLength = 4
		}
		return C.SQL_SUCCESS
	}

	switch infoType {
	case C.SQL_DRIVER_NAME:
		return writeString("odbc-adbc")
	case C.SQL_DRIVER_VER:
		return writeString("01.00.0000")
	case C.SQL_DBMS_NAME:
		return writeString("ADBC")
	case C.SQL_DBMS_VER:
		return writeString("01.00.0000")
	case C.SQL_DATA_SOURCE_NAME:
		return writeString("")
	case C.SQL_SERVER_NAME:
		return writeString("")
	case C.SQL_SEARCH_PATTERN_ESCAPE:
		return writeString("\\")
	case C.SQL_IDENTIFIER_QUOTE_CHAR:
		return writeString("\"")
	case C.SQL_CATALOG_TERM:
		return writeString("catalog")
	case C.SQL_SCHEMA_TERM:
		return writeString("schema")
	case C.SQL_TABLE_TERM:
		return writeString("table")
	case C.SQL_CATALOG_NAME_SEPARATOR:
		return writeString(".")
	case C.SQL_MAX_IDENTIFIER_LEN:
		return writeUint16(128)
	case C.SQL_MAX_CATALOG_NAME_LEN:
		return writeUint16(128)
	case C.SQL_MAX_SCHEMA_NAME_LEN:
		return writeUint16(128)
	case C.SQL_MAX_TABLE_NAME_LEN:
		return writeUint16(128)
	case C.SQL_MAX_COLUMN_NAME_LEN:
		return writeUint16(128)
	case C.SQL_TXN_CAPABLE:
		return writeUint16(C.SQL_TC_ALL)
	case C.SQL_GETDATA_EXTENSIONS:
		return writeUint32(C.SQL_GD_ANY_COLUMN | C.SQL_GD_ANY_ORDER)
	case C.SQL_CURSOR_COMMIT_BEHAVIOR, C.SQL_CURSOR_ROLLBACK_BEHAVIOR:
		return writeUint16(C.SQL_CB_CLOSE)
	case C.SQL_TXN_ISOLATION_OPTION:
		return writeUint32(C.SQL_TXN_READ_COMMITTED)
	case C.SQL_DEFAULT_TXN_ISOLATION:
		return writeUint32(C.SQL_TXN_READ_COMMITTED)
	default:
		return writeString("")
	}
}

//export SQLPrepare
func SQLPrepare(stmtHandle C.SQLHSTMT, stmtText *C.SQLCHAR, textLength C.SQLINTEGER) C.SQLRETURN {
	stmtID := uintptr(stmtHandle)
	stmt := registry.getStmt(stmtID)
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	conn := registry.getConn(stmt.connID)
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	query := sqlCharToString(stmtText, textLength)

	if err := stmt.ensureStatement(conn); err != nil {
		stmt.diags.addErrorf("SQLPrepare: %v", err)
		return C.SQL_ERROR
	}

	if err := stmt.stmt.SetSqlQuery(query); err != nil {
		stmt.diags.addErrorf("SQLPrepare: %v", err)
		return C.SQL_ERROR
	}

	if err := stmt.stmt.Prepare(context.Background()); err != nil {
		stmt.diags.addErrorf("SQLPrepare: %v", err)
		return C.SQL_ERROR
	}

	stmt.query = query
	stmt.prepared = true
	return C.SQL_SUCCESS
}

//export SQLExecute
func SQLExecute(stmtHandle C.SQLHSTMT) C.SQLRETURN {
	stmtID := uintptr(stmtHandle)
	stmt := registry.getStmt(stmtID)
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	conn := registry.getConn(stmt.connID)
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	stmt.closeResult()
	stmt.diags.clear()

	reader, nRows, err := stmt.stmt.ExecuteQuery(context.Background())
	if err != nil {
		stmt.diags.addErrorf("SQLExecute: %v", err)
		return C.SQL_ERROR
	}

	stmt.rowsAffected = nRows
	if reader != nil {
		stmt.result = &ResultSet{
			reader: reader,
			schema: reader.Schema(),
		}
	}
	return C.SQL_SUCCESS
}

//export SQLColAttribute
func SQLColAttribute(
	stmtHandle C.SQLHSTMT,
	colNumber C.SQLUSMALLINT,
	fieldIdentifier C.SQLUSMALLINT,
	characterAttribute C.SQLPOINTER,
	bufferLength C.SQLSMALLINT,
	stringLength *C.SQLSMALLINT,
	numericAttribute *C.SQLLEN,
) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	if stmt.result == nil || stmt.result.schema == nil {
		stmt.diags.addError("no result set")
		return C.SQL_ERROR
	}

	fields := stmt.result.schema.Fields()
	idx := int(colNumber) - 1
	if idx < 0 || idx >= len(fields) {
		stmt.diags.addErrorf("column %d out of range", colNumber)
		return C.SQL_ERROR
	}
	f := fields[idx]

	writeStrAttr := func(s string) {
		if characterAttribute != nil && bufferLength > 0 {
			dst := unsafe.Slice((*byte)(characterAttribute), int(bufferLength))
			n := copy(dst, s)
			if n < int(bufferLength) {
				dst[n] = 0
			}
		}
		if stringLength != nil {
			*stringLength = C.SQLSMALLINT(len(s))
		}
	}

	writeNumAttr := func(v int64) {
		if numericAttribute != nil {
			*numericAttribute = C.SQLLEN(v)
		}
	}

	switch fieldIdentifier {
	case C.SQL_DESC_NAME, C.SQL_COLUMN_NAME:
		writeStrAttr(f.Name)
	case C.SQL_DESC_TYPE, C.SQL_COLUMN_TYPE:
		writeNumAttr(int64(arrowTypeToSQLType(f.Type)))
	case C.SQL_DESC_LENGTH, C.SQL_COLUMN_LENGTH:
		writeNumAttr(int64(arrowTypeColumnSize(f.Type)))
	case C.SQL_DESC_PRECISION, C.SQL_COLUMN_PRECISION:
		writeNumAttr(int64(arrowTypeColumnSize(f.Type)))
	case C.SQL_DESC_SCALE, C.SQL_COLUMN_SCALE:
		writeNumAttr(int64(arrowTypeDecimalDigits(f.Type)))
	case C.SQL_DESC_NULLABLE, C.SQL_COLUMN_NULLABLE:
		writeNumAttr(int64(arrowTypeNullable(f)))
	case C.SQL_DESC_TYPE_NAME:
		writeStrAttr(f.Type.Name())
	case C.SQL_DESC_LABEL:
		writeStrAttr(f.Name)
	case C.SQL_DESC_UNNAMED:
		if f.Name == "" {
			writeNumAttr(C.SQL_UNNAMED)
		} else {
			writeNumAttr(C.SQL_NAMED)
		}
	case C.SQL_DESC_COUNT:
		writeNumAttr(int64(len(fields)))
	case C.SQL_DESC_DISPLAY_SIZE:
		writeNumAttr(int64(arrowTypeColumnSize(f.Type)))
	case C.SQL_DESC_OCTET_LENGTH:
		writeNumAttr(int64(arrowTypeColumnSize(f.Type)))
	case C.SQL_DESC_AUTO_UNIQUE_VALUE:
		writeNumAttr(C.SQL_FALSE)
	case C.SQL_DESC_CASE_SENSITIVE:
		writeNumAttr(C.SQL_TRUE)
	case C.SQL_DESC_FIXED_PREC_SCALE:
		writeNumAttr(C.SQL_FALSE)
	case C.SQL_DESC_SEARCHABLE:
		writeNumAttr(C.SQL_SEARCHABLE)
	case C.SQL_DESC_UNSIGNED:
		writeNumAttr(C.SQL_FALSE)
	case C.SQL_DESC_UPDATABLE:
		writeNumAttr(C.SQL_ATTR_READWRITE_UNKNOWN)
	default:
		writeNumAttr(0)
	}

	return C.SQL_SUCCESS
}

//export SQLGetDiagField
func SQLGetDiagField(
	handleType C.SQLSMALLINT,
	handle C.SQLHANDLE,
	recNumber C.SQLSMALLINT,
	diagIdentifier C.SQLSMALLINT,
	diagInfo C.SQLPOINTER,
	bufferLength C.SQLSMALLINT,
	stringLength *C.SQLSMALLINT,
) C.SQLRETURN {
	var diags *DiagnosticRecords
	id := uintptr(handle)

	switch handleType {
	case C.SQL_HANDLE_ENV:
		if env := registry.getEnv(id); env != nil {
			diags = &env.diags
		}
	case C.SQL_HANDLE_DBC:
		if conn := registry.getConn(id); conn != nil {
			diags = &conn.diags
		}
	case C.SQL_HANDLE_STMT:
		if stmt := registry.getStmt(id); stmt != nil {
			diags = &stmt.diags
		}
	}

	if diags == nil {
		return C.SQL_INVALID_HANDLE
	}

	// Header fields (recNumber == 0)
	if recNumber == 0 {
		switch diagIdentifier {
		case C.SQL_DIAG_NUMBER:
			if diagInfo != nil {
				*(*C.SQLINTEGER)(diagInfo) = C.SQLINTEGER(diags.count())
			}
			return C.SQL_SUCCESS
		case C.SQL_DIAG_RETURNCODE:
			return C.SQL_SUCCESS
		}
	}

	rec := diags.get(int(recNumber))
	if rec == nil {
		return C.SQL_NO_DATA
	}

	switch diagIdentifier {
	case C.SQL_DIAG_SQLSTATE:
		if diagInfo != nil && bufferLength >= 6 {
			dst := unsafe.Slice((*byte)(diagInfo), 6)
			copy(dst, rec.sqlState[:])
			dst[5] = 0
		}
		if stringLength != nil {
			*stringLength = 5
		}
	case C.SQL_DIAG_NATIVE:
		if diagInfo != nil {
			*(*C.SQLINTEGER)(diagInfo) = C.SQLINTEGER(rec.nativeError)
		}
	case C.SQL_DIAG_MESSAGE_TEXT:
		if diagInfo != nil && bufferLength > 0 {
			msg := []byte(rec.message)
			maxCopy := int(bufferLength) - 1
			if len(msg) > maxCopy {
				msg = msg[:maxCopy]
			}
			dst := unsafe.Slice((*byte)(diagInfo), int(bufferLength))
			n := copy(dst, msg)
			dst[n] = 0
		}
		if stringLength != nil {
			*stringLength = C.SQLSMALLINT(len(rec.message))
		}
	}
	return C.SQL_SUCCESS
}

//export SQLSetStmtAttr
func SQLSetStmtAttr(stmtHandle C.SQLHSTMT, attribute C.SQLINTEGER, value C.SQLPOINTER, stringLength C.SQLINTEGER) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}
	// Accept but ignore most attributes for now
	return C.SQL_SUCCESS
}

//export SQLGetConnectAttr
func SQLGetConnectAttr(connHandle C.SQLHDBC, attribute C.SQLINTEGER, value C.SQLPOINTER, bufferLength C.SQLINTEGER, stringLength *C.SQLINTEGER) C.SQLRETURN {
	conn := registry.getConn(uintptr(connHandle))
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	switch attribute {
	case C.SQL_ATTR_AUTOCOMMIT:
		if value != nil {
			if conn.autocommit {
				*(*C.SQLUINTEGER)(value) = C.SQL_AUTOCOMMIT_ON
			} else {
				*(*C.SQLUINTEGER)(value) = C.SQL_AUTOCOMMIT_OFF
			}
		}
		return C.SQL_SUCCESS
	default:
		return C.SQL_SUCCESS
	}
}

//export SQLTables
func SQLTables(
	stmtHandle C.SQLHSTMT,
	catalogName *C.SQLCHAR,
	catalogNameLen C.SQLSMALLINT,
	schemaName *C.SQLCHAR,
	schemaNameLen C.SQLSMALLINT,
	tableName *C.SQLCHAR,
	tableNameLen C.SQLSMALLINT,
	tableType *C.SQLCHAR,
	tableTypeLen C.SQLSMALLINT,
) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	conn := registry.getConn(stmt.connID)
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	stmt.diags.clear()

	var catalog, schema, table, tableTypeStr *string
	if catalogName != nil {
		s := sqlCharToString(catalogName, C.SQLINTEGER(catalogNameLen))
		catalog = &s
	}
	if schemaName != nil {
		s := sqlCharToString(schemaName, C.SQLINTEGER(schemaNameLen))
		schema = &s
	}
	if tableName != nil {
		s := sqlCharToString(tableName, C.SQLINTEGER(tableNameLen))
		table = &s
	}
	if tableType != nil {
		s := sqlCharToString(tableType, C.SQLINTEGER(tableTypeLen))
		tableTypeStr = &s
	}

	if err := stmt.tables(conn, catalog, schema, table, tableTypeStr); err != nil {
		stmt.diags.addErrorf("SQLTables: %v", err)
		return C.SQL_ERROR
	}
	return C.SQL_SUCCESS
}

//export SQLColumns
func SQLColumns(
	stmtHandle C.SQLHSTMT,
	catalogName *C.SQLCHAR,
	catalogNameLen C.SQLSMALLINT,
	schemaName *C.SQLCHAR,
	schemaNameLen C.SQLSMALLINT,
	tableName *C.SQLCHAR,
	tableNameLen C.SQLSMALLINT,
	columnName *C.SQLCHAR,
	columnNameLen C.SQLSMALLINT,
) C.SQLRETURN {
	stmt := registry.getStmt(uintptr(stmtHandle))
	if stmt == nil {
		return C.SQL_INVALID_HANDLE
	}

	conn := registry.getConn(stmt.connID)
	if conn == nil {
		return C.SQL_INVALID_HANDLE
	}

	stmt.diags.clear()

	var catalog, schema, table, column *string
	if catalogName != nil {
		s := sqlCharToString(catalogName, C.SQLINTEGER(catalogNameLen))
		catalog = &s
	}
	if schemaName != nil {
		s := sqlCharToString(schemaName, C.SQLINTEGER(schemaNameLen))
		schema = &s
	}
	if tableName != nil {
		s := sqlCharToString(tableName, C.SQLINTEGER(tableNameLen))
		table = &s
	}
	if columnName != nil {
		s := sqlCharToString(columnName, C.SQLINTEGER(columnNameLen))
		column = &s
	}

	if err := stmt.columns(conn, catalog, schema, table, column); err != nil {
		stmt.diags.addErrorf("SQLColumns: %v", err)
		return C.SQL_ERROR
	}
	return C.SQL_SUCCESS
}

func main() {}
