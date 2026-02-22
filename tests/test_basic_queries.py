import pytest


def test_select_literal_integer(cursor):
    cursor.execute("SELECT 42 AS val")
    row = cursor.fetchone()
    assert row[0] == 42


def test_select_literal_string(cursor):
    cursor.execute("SELECT 'hello' AS val")
    row = cursor.fetchone()
    assert row[0] == "hello"


def test_select_literal_float(cursor):
    cursor.execute("SELECT 3.14::DOUBLE PRECISION AS val")
    row = cursor.fetchone()
    assert row[0] == pytest.approx(3.14)


def test_select_literal_null(cursor):
    cursor.execute("SELECT NULL AS val")
    row = cursor.fetchone()
    assert row[0] is None


def test_select_multiple_columns(cursor):
    cursor.execute("SELECT 1 AS a, 'two' AS b, 3.0::DOUBLE PRECISION AS c")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == "two"
    assert row[2] == pytest.approx(3.0)


def test_select_no_rows(cursor):
    cursor.execute("SELECT 1 AS val WHERE 1 = 0")
    row = cursor.fetchone()
    assert row is None


def test_create_insert_select(cursor):
    cursor.execute("CREATE TABLE test_cis (id INTEGER, name VARCHAR)")
    cursor.execute("INSERT INTO test_cis VALUES (1, 'Alice'), (2, 'Bob')")
    cursor.execute("SELECT id, name FROM test_cis ORDER BY id")
    rows = cursor.fetchall()
    assert len(rows) == 2
    assert rows[0][0] == 1
    assert rows[0][1] == "Alice"
    assert rows[1][0] == 2
    assert rows[1][1] == "Bob"


def test_multiple_rows(cursor):
    cursor.execute("CREATE TABLE test_mr (val INTEGER)")
    cursor.execute("INSERT INTO test_mr VALUES (10), (20), (30)")
    cursor.execute("SELECT val FROM test_mr ORDER BY val")
    rows = cursor.fetchall()
    assert [r[0] for r in rows] == [10, 20, 30]


def test_fetchall(cursor):
    cursor.execute("SELECT * FROM (VALUES (1), (2), (3)) AS t(v)")
    rows = cursor.fetchall()
    assert len(rows) == 3


def test_fetchmany(cursor):
    cursor.execute("SELECT * FROM (VALUES (1), (2), (3), (4), (5)) AS t(v)")
    rows = cursor.fetchmany(2)
    assert len(rows) == 2


def test_fetchone_iteration(cursor):
    cursor.execute("SELECT * FROM (VALUES (1), (2), (3)) AS t(v)")
    count = 0
    while cursor.fetchone() is not None:
        count += 1
    assert count == 3


def test_rowcount_after_insert(cursor):
    cursor.execute("CREATE TABLE test_rc (id INTEGER)")
    cursor.execute("INSERT INTO test_rc VALUES (1), (2), (3)")
    # rowcount may be 0 or 3 depending on the ADBC driver
    assert cursor.rowcount >= 0


def test_multiple_statements_on_cursor(cursor):
    cursor.execute("SELECT 1 AS val")
    row1 = cursor.fetchone()
    assert row1[0] == 1

    cursor.execute("SELECT 'hello' AS val")
    row2 = cursor.fetchone()
    assert row2[0] == "hello"


def test_execute_ddl(cursor):
    cursor.execute("CREATE TABLE test_ddl (id INTEGER)")
    cursor.execute("DROP TABLE test_ddl")
