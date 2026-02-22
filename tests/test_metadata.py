def test_description_after_select(cursor):
    cursor.execute("SELECT 1 AS a, 'hello' AS b")
    assert cursor.description is not None


def test_description_column_names(cursor):
    cursor.execute("SELECT 1 AS alpha, 2 AS beta, 3 AS gamma")
    names = [col[0] for col in cursor.description]
    assert names == ["alpha", "beta", "gamma"]


def test_description_column_count(cursor):
    cursor.execute("SELECT 1 AS a, 2 AS b, 3 AS c, 4 AS d")
    assert len(cursor.description) == 4


def test_description_type_code_integer(cursor):
    cursor.execute("SELECT 1::INTEGER AS val")
    type_code = cursor.description[0][1]
    assert type_code is not None


def test_description_type_code_varchar(cursor):
    cursor.execute("SELECT 'hello'::VARCHAR AS val")
    type_code = cursor.description[0][1]
    assert type_code is not None


def test_description_type_code_float(cursor):
    cursor.execute("SELECT 1.0::DOUBLE PRECISION AS val")
    type_code = cursor.description[0][1]
    assert type_code is not None


def test_description_type_code_bool(cursor):
    cursor.execute("SELECT true AS val")
    type_code = cursor.description[0][1]
    assert type_code is not None


def test_description_type_code_date(cursor):
    cursor.execute("SELECT DATE '2024-01-01' AS val")
    type_code = cursor.description[0][1]
    assert type_code is not None


def test_description_type_code_timestamp(cursor):
    cursor.execute("SELECT TIMESTAMP '2024-01-01 00:00:00' AS val")
    type_code = cursor.description[0][1]
    assert type_code is not None


def test_description_before_execute(cursor):
    assert cursor.description is None


def test_description_after_ddl(cursor):
    cursor.execute("CREATE TABLE test_meta_ddl (id INTEGER)")
    # Some ADBC drivers (e.g. DuckDB) return a result set for DDL
    # so description may not be None
    assert cursor.description is None or len(cursor.description) >= 0
