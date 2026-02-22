def test_execute_same_query_twice(cursor):
    cursor.execute("SELECT 42 AS val")
    assert cursor.fetchone()[0] == 42

    cursor.execute("SELECT 42 AS val")
    assert cursor.fetchone()[0] == 42


def test_execute_different_queries(cursor):
    cursor.execute("SELECT 1 AS val")
    assert cursor.fetchone()[0] == 1

    cursor.execute("SELECT 'hello' AS val")
    assert cursor.fetchone()[0] == "hello"


def test_execute_select_after_dml(cursor):
    cursor.execute("CREATE TABLE test_prep (id INTEGER, name VARCHAR)")
    cursor.execute("INSERT INTO test_prep VALUES (1, 'Alice')")
    cursor.execute("SELECT id, name FROM test_prep")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == "Alice"
