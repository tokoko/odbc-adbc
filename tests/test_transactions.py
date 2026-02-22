import pytest


def test_autocommit_on_by_default(cursor):
    cursor.execute("CREATE TABLE test_ac (id INTEGER)")
    cursor.execute("INSERT INTO test_ac VALUES (1)")
    cursor.execute("SELECT * FROM test_ac")
    assert cursor.fetchone() is not None


@pytest.mark.xfail(reason="SQLSetConnectAttr does not yet handle autocommit")
def test_commit_persists_data(connection_string):
    import pyodbc

    conn = pyodbc.connect(connection_string, autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE test_commit (id INTEGER)")
        cur.execute("INSERT INTO test_commit VALUES (1)")
        conn.commit()
        cur.execute("SELECT * FROM test_commit")
        assert cur.fetchone() is not None
    finally:
        cur.close()
        conn.close()


@pytest.mark.xfail(reason="SQLSetConnectAttr does not yet handle autocommit")
def test_rollback_discards_data(connection_string):
    import pyodbc

    conn = pyodbc.connect(connection_string, autocommit=False)
    cur = conn.cursor()
    try:
        cur.execute("CREATE TABLE test_rb (id INTEGER)")
        conn.commit()
        cur.execute("INSERT INTO test_rb VALUES (1)")
        conn.rollback()
        cur.execute("SELECT * FROM test_rb")
        row = cur.fetchone()
        assert row is None
    finally:
        cur.close()
        conn.close()
