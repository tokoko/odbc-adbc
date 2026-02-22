import pyodbc
import pytest


def test_invalid_sql_raises(cursor):
    with pytest.raises(pyodbc.Error):
        cursor.execute("NOT VALID SQL")


def test_error_message_contains_info(cursor):
    with pytest.raises(pyodbc.Error, match=r".+"):
        cursor.execute("NOT VALID SQL")


def test_select_from_nonexistent_table(cursor):
    with pytest.raises(pyodbc.Error):
        cursor.execute("SELECT * FROM no_such_table")


def test_double_close_cursor(conn):
    cur = conn.cursor()
    cur.close()
    # pyodbc raises ProgrammingError on double close, which is acceptable
    with pytest.raises(pyodbc.ProgrammingError):
        cur.close()


def test_error_clears_on_success(cursor):
    with pytest.raises(pyodbc.Error):
        cursor.execute("INVALID SQL")
    cursor.execute("SELECT 1 AS val")
    assert cursor.fetchone()[0] == 1
