def test_tables_returns_results(cursor):
    cursor.execute("CREATE TABLE test_tbl_meta (id INTEGER)")
    cursor.tables()
    rows = cursor.fetchall()
    assert len(rows) > 0


def test_tables_column_count(cursor):
    cursor.tables()
    assert cursor.description is not None
    assert len(cursor.description) == 5


def test_tables_column_names(cursor):
    cursor.tables()
    names = [col[0].upper() for col in cursor.description]
    assert names == ["TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS"]


def test_tables_finds_created_table(cursor):
    cursor.execute("CREATE TABLE test_find_me (id INTEGER)")
    cursor.tables()
    rows = cursor.fetchall()
    table_names = [row[2] for row in rows]
    assert "test_find_me" in table_names


def test_tables_filter_by_name(cursor):
    cursor.execute("CREATE TABLE test_filter_a (id INTEGER)")
    cursor.execute("CREATE TABLE test_filter_b (id INTEGER)")
    cursor.tables(table="test_filter_a")
    rows = cursor.fetchall()
    table_names = [row[2] for row in rows]
    assert "test_filter_a" in table_names
    assert "test_filter_b" not in table_names


def test_tables_pattern(cursor):
    cursor.execute("CREATE TABLE test_pattern_xyz (id INTEGER)")
    cursor.tables(table="test_pattern_%")
    rows = cursor.fetchall()
    table_names = [row[2] for row in rows]
    assert "test_pattern_xyz" in table_names


def test_tables_empty_result(cursor):
    cursor.tables(table="nonexistent_table_xyz_123")
    rows = cursor.fetchall()
    assert len(rows) == 0


# --- SQLColumns tests ---


def test_columns_returns_results(cursor):
    cursor.execute("CREATE TABLE test_col_meta (id INTEGER, name VARCHAR(100))")
    cursor.columns(table="test_col_meta")
    rows = cursor.fetchall()
    assert len(rows) > 0


def test_columns_column_count(cursor):
    cursor.execute("CREATE TABLE test_col_cnt (id INTEGER)")
    cursor.columns(table="test_col_cnt")
    assert cursor.description is not None
    assert len(cursor.description) == 18


def test_columns_column_names(cursor):
    cursor.execute("CREATE TABLE test_col_names (id INTEGER)")
    cursor.columns(table="test_col_names")
    names = [col[0].upper() for col in cursor.description]
    assert names == [
        "TABLE_CAT",
        "TABLE_SCHEM",
        "TABLE_NAME",
        "COLUMN_NAME",
        "DATA_TYPE",
        "TYPE_NAME",
        "COLUMN_SIZE",
        "BUFFER_LENGTH",
        "DECIMAL_DIGITS",
        "NUM_PREC_RADIX",
        "NULLABLE",
        "REMARKS",
        "COLUMN_DEF",
        "SQL_DATA_TYPE",
        "SQL_DATETIME_SUB",
        "CHAR_OCTET_LENGTH",
        "ORDINAL_POSITION",
        "IS_NULLABLE",
    ]


def test_columns_finds_column(cursor):
    cursor.execute("CREATE TABLE test_col_find (alpha INTEGER, beta VARCHAR(50))")
    cursor.columns(table="test_col_find")
    rows = cursor.fetchall()
    col_names = [row[3] for row in rows]
    assert "alpha" in col_names
    assert "beta" in col_names


def test_columns_filter_by_table(cursor):
    cursor.execute("CREATE TABLE test_col_filt_a (x INTEGER)")
    cursor.execute("CREATE TABLE test_col_filt_b (y INTEGER)")
    cursor.columns(table="test_col_filt_a")
    rows = cursor.fetchall()
    table_names = set(row[2] for row in rows)
    assert "test_col_filt_a" in table_names
    assert "test_col_filt_b" not in table_names


def test_columns_ordinal_position(cursor):
    cursor.execute("CREATE TABLE test_col_ord (first_col INTEGER, second_col VARCHAR(20))")
    cursor.columns(table="test_col_ord")
    rows = cursor.fetchall()
    # ORDINAL_POSITION is column 17 (index 16), 1-based
    positions = {row[3]: row[16] for row in rows}
    assert positions["first_col"] == 1
    assert positions["second_col"] == 2


def test_columns_nullable(cursor):
    cursor.execute(
        "CREATE TABLE test_col_null (required_col INTEGER NOT NULL, optional_col INTEGER)"
    )
    cursor.columns(table="test_col_null")
    rows = cursor.fetchall()
    # NULLABLE is column 11 (index 10): 0=NO_NULLS, 1=NULLABLE, 2=UNKNOWN
    # ADBC drivers may not populate xdbc_nullable; accept any valid value
    nullability = {row[3]: row[10] for row in rows}
    assert nullability["required_col"] in (0, 2)  # SQL_NO_NULLS or SQL_NULLABLE_UNKNOWN
    assert nullability["optional_col"] in (1, 2)  # SQL_NULLABLE or SQL_NULLABLE_UNKNOWN


def test_columns_empty_result(cursor):
    cursor.columns(table="nonexistent_table_xyz_456")
    rows = cursor.fetchall()
    assert len(rows) == 0
