import datetime

import pytest


def test_bool_true(cursor):
    cursor.execute("SELECT true AS val")
    row = cursor.fetchone()
    assert row[0] is True or row[0] == 1


def test_bool_false(cursor):
    cursor.execute("SELECT false AS val")
    row = cursor.fetchone()
    assert row[0] is False or row[0] == 0


@pytest.mark.duckdb
def test_tinyint(cursor):
    cursor.execute("SELECT 127::TINYINT AS val")
    row = cursor.fetchone()
    assert row[0] == 127


def test_smallint(cursor):
    cursor.execute("SELECT 32000::SMALLINT AS val")
    row = cursor.fetchone()
    assert row[0] == 32000


def test_integer(cursor):
    cursor.execute("SELECT 2147483647::INTEGER AS val")
    row = cursor.fetchone()
    assert row[0] == 2147483647


def test_bigint(cursor):
    cursor.execute("SELECT 9223372036854775807::BIGINT AS val")
    row = cursor.fetchone()
    assert row[0] == 9223372036854775807


def test_integer_negative(cursor):
    cursor.execute("SELECT -42::INTEGER AS val")
    row = cursor.fetchone()
    assert row[0] == -42


def test_float(cursor):
    cursor.execute("SELECT 1.5::REAL AS val")
    row = cursor.fetchone()
    assert row[0] == pytest.approx(1.5)


def test_double(cursor):
    cursor.execute("SELECT 3.141592653589793::DOUBLE PRECISION AS val")
    row = cursor.fetchone()
    assert row[0] == pytest.approx(3.141592653589793)


def test_varchar(cursor):
    cursor.execute("SELECT 'hello world'::VARCHAR AS val")
    row = cursor.fetchone()
    assert row[0] == "hello world"


def test_varchar_empty(cursor):
    cursor.execute("SELECT ''::VARCHAR AS val")
    row = cursor.fetchone()
    assert row[0] == ""


def test_varchar_unicode(cursor):
    cursor.execute("SELECT '\u00e9\u00e8\u00ea' AS val")
    row = cursor.fetchone()
    assert row[0] == "\u00e9\u00e8\u00ea"


def test_date(cursor):
    cursor.execute("SELECT DATE '2024-06-15' AS val")
    row = cursor.fetchone()
    assert row[0] == datetime.date(2024, 6, 15)


def test_timestamp(cursor):
    cursor.execute("SELECT TIMESTAMP '2024-06-15 10:30:45' AS val")
    row = cursor.fetchone()
    assert row[0] == datetime.datetime(2024, 6, 15, 10, 30, 45)


def test_null_integer(cursor):
    cursor.execute("SELECT NULL::INTEGER AS val")
    row = cursor.fetchone()
    assert row[0] is None


def test_null_varchar(cursor):
    cursor.execute("SELECT NULL::VARCHAR AS val")
    row = cursor.fetchone()
    assert row[0] is None


@pytest.mark.duckdb
def test_blob(cursor):
    cursor.execute("SELECT '\\x010203'::BLOB AS val")
    row = cursor.fetchone()
    assert isinstance(row[0], (bytes, bytearray, str))


def test_bytea(cursor, driver_name):
    if driver_name == "duckdb":
        pytest.skip("DuckDB uses BLOB, not BYTEA")
    cursor.execute("SELECT '\\x010203'::BYTEA AS val")
    row = cursor.fetchone()
    assert isinstance(row[0], (bytes, bytearray, str))


def test_mixed_types_in_row(cursor):
    cursor.execute(
        "SELECT 1 AS a, 'hello' AS b, 3.14::DOUBLE PRECISION AS c, true AS d, NULL AS e"
    )
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == "hello"
    assert row[2] == pytest.approx(3.14)
    assert row[3] is True or row[3] == 1
    assert row[4] is None
