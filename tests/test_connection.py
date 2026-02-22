import pyodbc
import pytest


def test_connect_basic(connection_string):
    conn = pyodbc.connect(connection_string, autocommit=True)
    assert conn is not None
    conn.close()


def test_connect_and_close(connection_string):
    conn = pyodbc.connect(connection_string, autocommit=True)
    conn.close()


def test_connect_missing_adbc_driver(driver_path):
    with pytest.raises(pyodbc.Error):
        pyodbc.connect(
            f"Driver={driver_path};path=:memory:",
            autocommit=True,
        )


def test_connect_invalid_adbc_driver(driver_path):
    with pytest.raises(pyodbc.Error):
        pyodbc.connect(
            f"Driver={driver_path};adbc_driver=nonexistent;path=:memory:",
            autocommit=True,
        )


def test_multiple_connections(connection_string):
    conn1 = pyodbc.connect(connection_string, autocommit=True)
    conn2 = pyodbc.connect(connection_string, autocommit=True)

    cur1 = conn1.cursor()
    cur2 = conn2.cursor()

    cur1.execute("SELECT 1 AS val")
    cur2.execute("SELECT 2 AS val")

    assert cur1.fetchone()[0] == 1
    assert cur2.fetchone()[0] == 2

    cur1.close()
    cur2.close()
    conn1.close()
    conn2.close()
