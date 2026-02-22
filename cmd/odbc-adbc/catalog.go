package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

var sqlTablesSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "TABLE_CAT", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "TABLE_SCHEM", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "TABLE_NAME", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "TABLE_TYPE", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "REMARKS", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	nil,
)

var sqlColumnsSchema = arrow.NewSchema(
	[]arrow.Field{
		{Name: "TABLE_CAT", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "TABLE_SCHEM", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "TABLE_NAME", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "COLUMN_NAME", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "DATA_TYPE", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
		{Name: "TYPE_NAME", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "COLUMN_SIZE", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "BUFFER_LENGTH", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "DECIMAL_DIGITS", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "NUM_PREC_RADIX", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "NULLABLE", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
		{Name: "REMARKS", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "COLUMN_DEF", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "SQL_DATA_TYPE", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
		{Name: "SQL_DATETIME_SUB", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		{Name: "CHAR_OCTET_LENGTH", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "ORDINAL_POSITION", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "IS_NULLABLE", Type: arrow.BinaryTypes.String, Nullable: true},
	},
	nil,
)

func (s *Statement) tables(conn *Connection, catalog, schema, table, tableTypeStr *string) error {
	s.closeResult()

	if conn.conn == nil {
		return fmt.Errorf("connection not open")
	}

	var tableTypes []string
	if tableTypeStr != nil {
		tableTypes = parseTableTypes(*tableTypeStr)
	}

	reader, err := conn.conn.GetObjects(
		context.Background(),
		adbc.ObjectDepthTables,
		catalog,
		schema,
		table,
		nil,
		tableTypes,
	)
	if err != nil {
		return fmt.Errorf("GetObjects: %w", err)
	}
	defer reader.Release()

	rows, err := flattenGetObjectsToTableRows(reader)
	if err != nil {
		return fmt.Errorf("flatten GetObjects: %w", err)
	}
	if rows == nil {
		rows = [][]any{}
	}

	s.result = &ResultSet{
		schema:      sqlTablesSchema,
		rawRows:     rows,
		rawRowCount: int64(len(rows)),
		rowIndex:    -1,
	}
	return nil
}

func (s *Statement) columns(conn *Connection, catalog, schema, table, column *string) error {
	s.closeResult()

	if conn.conn == nil {
		return fmt.Errorf("connection not open")
	}

	reader, err := conn.conn.GetObjects(
		context.Background(),
		adbc.ObjectDepthColumns,
		catalog,
		schema,
		table,
		column,
		nil,
	)
	if err != nil {
		return fmt.Errorf("GetObjects: %w", err)
	}
	defer reader.Release()

	rows, err := flattenGetObjectsToColumnRows(reader)
	if err != nil {
		return fmt.Errorf("flatten GetObjects: %w", err)
	}
	if rows == nil {
		rows = [][]any{}
	}

	s.result = &ResultSet{
		schema:      sqlColumnsSchema,
		rawRows:     rows,
		rawRowCount: int64(len(rows)),
		rowIndex:    -1,
	}
	return nil
}

// parseTableTypes splits an ODBC-style comma-separated table type string.
// "'TABLE','VIEW'" → []string{"TABLE", "VIEW"}
func parseTableTypes(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, "'")
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

// copyStr forces a copy of a string so it doesn't reference Arrow buffer memory.
func copyStr(s string) string {
	return string([]byte(s))
}

// nullableStr returns a copied string or nil if the Arrow value is null.
func nullableStr(col arrow.Array, idx int) any {
	if col.IsNull(idx) {
		return nil
	}
	return copyStr(col.ValueStr(idx))
}

// nullableInt16 returns an int16 value or nil if the Arrow value is null.
func nullableInt16(col arrow.Array, idx int) any {
	if col.IsNull(idx) {
		return nil
	}
	return col.(*array.Int16).Value(idx)
}

// nullableInt32 returns an int32 value or nil if the Arrow value is null.
func nullableInt32(col arrow.Array, idx int) any {
	if col.IsNull(idx) {
		return nil
	}
	return col.(*array.Int32).Value(idx)
}

func flattenGetObjectsToTableRows(reader array.RecordReader) ([][]any, error) {
	var rows [][]any

	for reader.Next() {
		batch := reader.Record()
		nRows := int(batch.NumRows())

		catalogCol := batch.Column(0)
		dbSchemasListCol := batch.Column(1).(*array.List)
		dbSchemasValues := dbSchemasListCol.ListValues().(*array.Struct)

		dbSchemaNameCol := dbSchemasValues.Field(0)
		dbSchemaTablesListCol := dbSchemasValues.Field(1).(*array.List)
		tableStructValues := dbSchemaTablesListCol.ListValues().(*array.Struct)

		tableNameCol := tableStructValues.Field(0)
		tableTypeCol := tableStructValues.Field(1)

		for catIdx := 0; catIdx < nRows; catIdx++ {
			catName := nullableStr(catalogCol, catIdx)

			if dbSchemasListCol.IsNull(catIdx) {
				continue
			}
			schStart, schEnd := dbSchemasListCol.ValueOffsets(catIdx)
			for schIdx := int(schStart); schIdx < int(schEnd); schIdx++ {
				schName := nullableStr(dbSchemaNameCol, schIdx)

				if dbSchemaTablesListCol.IsNull(schIdx) {
					continue
				}
				tblStart, tblEnd := dbSchemaTablesListCol.ValueOffsets(schIdx)
				for tblIdx := int(tblStart); tblIdx < int(tblEnd); tblIdx++ {
					tblName := copyStr(tableNameCol.ValueStr(tblIdx))
					tblType := copyStr(tableTypeCol.ValueStr(tblIdx))
					rows = append(rows, []any{catName, schName, tblName, tblType, nil})
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func flattenGetObjectsToColumnRows(reader array.RecordReader) ([][]any, error) {
	var rows [][]any

	for reader.Next() {
		batch := reader.Record()
		nRows := int(batch.NumRows())

		catalogCol := batch.Column(0)
		dbSchemasListCol := batch.Column(1).(*array.List)
		dbSchemasValues := dbSchemasListCol.ListValues().(*array.Struct)

		dbSchemaNameCol := dbSchemasValues.Field(0)
		dbSchemaTablesListCol := dbSchemasValues.Field(1).(*array.List)
		tableStructValues := dbSchemaTablesListCol.ListValues().(*array.Struct)

		tableNameCol := tableStructValues.Field(0)
		// Field 1 = table_type, Field 2 = table_columns, Field 3 = table_constraints
		tableColumnsListCol := tableStructValues.Field(2).(*array.List)
		columnStructValues := tableColumnsListCol.ListValues().(*array.Struct)

		// COLUMN_SCHEMA fields (by index):
		// 0: column_name, 1: ordinal_position, 2: remarks
		// 3: xdbc_data_type, 4: xdbc_type_name, 5: xdbc_column_size
		// 6: xdbc_decimal_digits, 7: xdbc_num_prec_radix, 8: xdbc_nullable
		// 9: xdbc_column_def, 10: xdbc_sql_data_type, 11: xdbc_datetime_sub
		// 12: xdbc_char_octet_length, 13: xdbc_is_nullable
		colNameCol := columnStructValues.Field(0)
		ordinalPosCol := columnStructValues.Field(1)
		remarksCol := columnStructValues.Field(2)
		xdbcDataTypeCol := columnStructValues.Field(3)
		xdbcTypeNameCol := columnStructValues.Field(4)
		xdbcColumnSizeCol := columnStructValues.Field(5)
		xdbcDecimalDigitsCol := columnStructValues.Field(6)
		xdbcNumPrecRadixCol := columnStructValues.Field(7)
		xdbcNullableCol := columnStructValues.Field(8)
		xdbcColumnDefCol := columnStructValues.Field(9)
		xdbcSqlDataTypeCol := columnStructValues.Field(10)
		xdbcDatetimeSubCol := columnStructValues.Field(11)
		xdbcCharOctetLenCol := columnStructValues.Field(12)
		xdbcIsNullableCol := columnStructValues.Field(13)

		for catIdx := 0; catIdx < nRows; catIdx++ {
			catName := nullableStr(catalogCol, catIdx)

			if dbSchemasListCol.IsNull(catIdx) {
				continue
			}
			schStart, schEnd := dbSchemasListCol.ValueOffsets(catIdx)
			for schIdx := int(schStart); schIdx < int(schEnd); schIdx++ {
				schName := nullableStr(dbSchemaNameCol, schIdx)

				if dbSchemaTablesListCol.IsNull(schIdx) {
					continue
				}
				tblStart, tblEnd := dbSchemaTablesListCol.ValueOffsets(schIdx)
				for tblIdx := int(tblStart); tblIdx < int(tblEnd); tblIdx++ {
					tblName := copyStr(tableNameCol.ValueStr(tblIdx))

					if tableColumnsListCol.IsNull(tblIdx) {
						continue
					}
					colStart, colEnd := tableColumnsListCol.ValueOffsets(tblIdx)
					for colIdx := int(colStart); colIdx < int(colEnd); colIdx++ {
						columnName := copyStr(colNameCol.ValueStr(colIdx))

						// Required fields with fallbacks
						dataType := int16(SQL_VARCHAR)
						if !xdbcDataTypeCol.IsNull(colIdx) {
							dataType = xdbcDataTypeCol.(*array.Int16).Value(colIdx)
						}
						typeName := "UNKNOWN"
						if !xdbcTypeNameCol.IsNull(colIdx) {
							typeName = copyStr(xdbcTypeNameCol.ValueStr(colIdx))
						}
						nullable := int16(SQL_NULLABLE_UNKNOWN)
						if !xdbcNullableCol.IsNull(colIdx) {
							nullable = xdbcNullableCol.(*array.Int16).Value(colIdx)
						}
						sqlDataType := dataType
						if !xdbcSqlDataTypeCol.IsNull(colIdx) {
							sqlDataType = xdbcSqlDataTypeCol.(*array.Int16).Value(colIdx)
						}
						ordinalPos := int32(0)
						if !ordinalPosCol.IsNull(colIdx) {
							ordinalPos = ordinalPosCol.(*array.Int32).Value(colIdx)
						}

						// 18 columns matching sqlColumnsSchema
						rows = append(rows, []any{
							catName,                                    // TABLE_CAT
							schName,                                    // TABLE_SCHEM
							tblName,                                    // TABLE_NAME
							columnName,                                 // COLUMN_NAME
							dataType,                                   // DATA_TYPE
							typeName,                                   // TYPE_NAME
							nullableInt32(xdbcColumnSizeCol, colIdx),   // COLUMN_SIZE
							nullableInt32(xdbcColumnSizeCol, colIdx),   // BUFFER_LENGTH (= COLUMN_SIZE)
							nullableInt16(xdbcDecimalDigitsCol, colIdx), // DECIMAL_DIGITS
							nullableInt16(xdbcNumPrecRadixCol, colIdx), // NUM_PREC_RADIX
							nullable,                                   // NULLABLE
							nullableStr(remarksCol, colIdx),            // REMARKS
							nullableStr(xdbcColumnDefCol, colIdx),      // COLUMN_DEF
							sqlDataType,                                // SQL_DATA_TYPE
							nullableInt16(xdbcDatetimeSubCol, colIdx),  // SQL_DATETIME_SUB
							nullableInt32(xdbcCharOctetLenCol, colIdx), // CHAR_OCTET_LENGTH
							ordinalPos,                                 // ORDINAL_POSITION
							nullableStr(xdbcIsNullableCol, colIdx),     // IS_NULLABLE
						})
					}
				}
			}
		}
	}

	if err := reader.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}
