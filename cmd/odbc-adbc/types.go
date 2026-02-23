package main

import (
	"github.com/apache/arrow-go/v18/arrow"
)

// ODBC SQL type constants
const (
	SQL_UNKNOWN_TYPE   = 0
	SQL_CHAR           = 1
	SQL_NUMERIC        = 2
	SQL_DECIMAL        = 3
	SQL_INTEGER        = 4
	SQL_SMALLINT       = 5
	SQL_FLOAT          = 6
	SQL_REAL           = 7
	SQL_DOUBLE         = 8
	SQL_VARCHAR        = 12
	SQL_TYPE_DATE      = 91
	SQL_TYPE_TIME      = 92
	SQL_TYPE_TIMESTAMP = 93
	SQL_LONGVARCHAR    = -1
	SQL_BINARY         = -2
	SQL_VARBINARY      = -3
	SQL_LONGVARBINARY  = -4
	SQL_BIGINT         = -5
	SQL_TINYINT        = -6
	SQL_BIT            = -7
	SQL_WCHAR          = -8
	SQL_WVARCHAR       = -9
	SQL_WLONGVARCHAR   = -10
)

// ODBC signed/unsigned offsets
const (
	SQL_SIGNED_OFFSET   = -20
	SQL_UNSIGNED_OFFSET = -22
)

// ODBC C type constants
const (
	SQL_C_CHAR           = SQL_CHAR     // 1
	SQL_C_LONG           = SQL_INTEGER  // 4
	SQL_C_SHORT          = SQL_SMALLINT // 5
	SQL_C_FLOAT          = SQL_REAL     // 7
	SQL_C_DOUBLE         = SQL_DOUBLE   // 8
	SQL_C_BIT            = SQL_BIT      // -7
	SQL_C_STINYINT       = SQL_TINYINT + SQL_SIGNED_OFFSET   // -26
	SQL_C_SSHORT         = SQL_C_SHORT + SQL_SIGNED_OFFSET   // -15
	SQL_C_SLONG          = SQL_C_LONG + SQL_SIGNED_OFFSET    // -16
	SQL_C_SBIGINT        = SQL_BIGINT + SQL_SIGNED_OFFSET    // -25
	SQL_C_UTINYINT       = SQL_TINYINT + SQL_UNSIGNED_OFFSET // -28
	SQL_C_USHORT         = SQL_C_SHORT + SQL_UNSIGNED_OFFSET // -17
	SQL_C_ULONG          = SQL_C_LONG + SQL_UNSIGNED_OFFSET  // -18
	SQL_C_UBIGINT        = SQL_BIGINT + SQL_UNSIGNED_OFFSET  // -27
	SQL_C_BINARY         = SQL_BINARY
	SQL_C_WCHAR          = SQL_WCHAR
	SQL_C_TYPE_DATE      = SQL_TYPE_DATE
	SQL_C_TYPE_TIME      = SQL_TYPE_TIME
	SQL_C_TYPE_TIMESTAMP = SQL_TYPE_TIMESTAMP
	SQL_C_DEFAULT        = 99
)

// ODBC nullable constants
const (
	SQL_NO_NULLS         = 0
	SQL_NULLABLE         = 1
	SQL_NULLABLE_UNKNOWN = 2
)

// arrowTypeToSQLType maps Arrow data types to ODBC SQL types.
func arrowTypeToSQLType(dt arrow.DataType) int16 {
	switch dt.ID() {
	case arrow.BOOL:
		return SQL_BIT
	case arrow.INT8:
		return SQL_TINYINT
	case arrow.INT16:
		return SQL_SMALLINT
	case arrow.INT32:
		return SQL_INTEGER
	case arrow.INT64:
		return SQL_BIGINT
	case arrow.UINT8:
		return SQL_TINYINT
	case arrow.UINT16:
		return SQL_SMALLINT
	case arrow.UINT32:
		return SQL_INTEGER
	case arrow.UINT64:
		return SQL_BIGINT
	case arrow.FLOAT16, arrow.FLOAT32:
		return SQL_REAL
	case arrow.FLOAT64:
		return SQL_DOUBLE
	case arrow.STRING, arrow.LARGE_STRING:
		return SQL_VARCHAR
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return SQL_VARBINARY
	case arrow.DATE32, arrow.DATE64:
		return SQL_TYPE_DATE
	case arrow.TIME32, arrow.TIME64:
		return SQL_TYPE_TIME
	case arrow.TIMESTAMP:
		return SQL_TYPE_TIMESTAMP
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return SQL_DECIMAL
	default:
		return SQL_VARCHAR // fallback: render as string
	}
}

// arrowTypeColumnSize returns the ODBC column size for an Arrow type.
func arrowTypeColumnSize(dt arrow.DataType) int {
	switch dt.ID() {
	case arrow.BOOL:
		return 1
	case arrow.INT8, arrow.UINT8:
		return 3
	case arrow.INT16, arrow.UINT16:
		return 5
	case arrow.INT32, arrow.UINT32:
		return 10
	case arrow.INT64, arrow.UINT64:
		return 19
	case arrow.FLOAT16, arrow.FLOAT32:
		return 7
	case arrow.FLOAT64:
		return 15
	case arrow.STRING, arrow.LARGE_STRING:
		return 65535 // max varchar
	case arrow.BINARY, arrow.LARGE_BINARY:
		return 65535
	case arrow.FIXED_SIZE_BINARY:
		return dt.(*arrow.FixedSizeBinaryType).ByteWidth
	case arrow.DATE32, arrow.DATE64:
		return 10
	case arrow.TIME32, arrow.TIME64:
		return 8
	case arrow.TIMESTAMP:
		return 26
	case arrow.DECIMAL128:
		return int(dt.(*arrow.Decimal128Type).Precision)
	case arrow.DECIMAL256:
		return int(dt.(*arrow.Decimal256Type).Precision)
	default:
		return 65535
	}
}

// arrowTypeDecimalDigits returns the ODBC decimal digits for an Arrow type.
func arrowTypeDecimalDigits(dt arrow.DataType) int16 {
	switch dt.ID() {
	case arrow.DECIMAL128:
		return int16(dt.(*arrow.Decimal128Type).Scale)
	case arrow.DECIMAL256:
		return int16(dt.(*arrow.Decimal256Type).Scale)
	case arrow.TIMESTAMP:
		return 6 // microseconds
	case arrow.TIME64:
		return 6
	case arrow.TIME32:
		return 0
	default:
		return 0
	}
}

// arrowTypeNullable returns the ODBC nullable indicator for an Arrow field.
func arrowTypeNullable(f arrow.Field) int16 {
	if f.Nullable {
		return SQL_NULLABLE
	}
	return SQL_NO_NULLS
}

// sqlTypeToArrowType maps ODBC SQL types to Arrow data types.
func sqlTypeToArrowType(sqlType int16) arrow.DataType {
	switch sqlType {
	case SQL_BIT:
		return arrow.FixedWidthTypes.Boolean
	case SQL_TINYINT:
		return arrow.PrimitiveTypes.Int8
	case SQL_SMALLINT:
		return arrow.PrimitiveTypes.Int16
	case SQL_INTEGER:
		return arrow.PrimitiveTypes.Int32
	case SQL_BIGINT:
		return arrow.PrimitiveTypes.Int64
	case SQL_REAL:
		return arrow.PrimitiveTypes.Float32
	case SQL_FLOAT, SQL_DOUBLE:
		return arrow.PrimitiveTypes.Float64
	case SQL_CHAR, SQL_VARCHAR, SQL_LONGVARCHAR,
		SQL_WCHAR, SQL_WVARCHAR, SQL_WLONGVARCHAR:
		return arrow.BinaryTypes.String
	case SQL_BINARY, SQL_VARBINARY, SQL_LONGVARBINARY:
		return arrow.BinaryTypes.Binary
	case SQL_NUMERIC, SQL_DECIMAL:
		return arrow.BinaryTypes.String // pass as string, let DB convert
	case SQL_TYPE_DATE:
		return arrow.FixedWidthTypes.Date32
	case SQL_TYPE_TIME:
		return arrow.FixedWidthTypes.Time64us
	case SQL_TYPE_TIMESTAMP:
		return &arrow.TimestampType{Unit: arrow.Microsecond}
	default:
		return arrow.BinaryTypes.String
	}
}
