import os
from pathlib import Path

import pyodbc
import pytest

DRIVER_CONFIGS = {
    "duckdb": {
        "adbc_driver": "duckdb",
        "path": ":memory:",
    },
    "postgresql": {
        "adbc_driver": "postgresql",
        "uri": os.environ.get(
            "POSTGRES_URI", "postgresql://testuser:testpass@localhost:5432/testdb"
        ),
    },
}

PROJECT_ROOT = Path(__file__).parent.parent


def _active_drivers():
    driver = os.environ.get("ODBC_ADBC_DRIVER")
    if driver:
        return [driver]
    return list(DRIVER_CONFIGS.keys())


def pytest_configure(config):
    config.addinivalue_line("markers", "duckdb: test requires duckdb driver")
    config.addinivalue_line("markers", "postgresql: test requires postgresql driver")


@pytest.fixture(scope="session")
def driver_path():
    path = os.environ.get("ODBC_ADBC_DRIVER_PATH", str(PROJECT_ROOT / "odbc-adbc.so"))
    if not os.path.exists(path):
        pytest.skip(f"Driver not found at {path}. Run 'pixi run build' first.")
    return path


@pytest.fixture(scope="session", params=_active_drivers())
def driver_name(request):
    return request.param


@pytest.fixture(scope="session")
def driver_config(driver_name):
    return DRIVER_CONFIGS[driver_name]


@pytest.fixture(scope="session")
def connection_string(driver_path, driver_config, driver_name):
    parts = [f"Driver={driver_path}"]
    for key, value in driver_config.items():
        parts.append(f"{key}={value}")
    cs = ";".join(parts)
    try:
        c = pyodbc.connect(cs, autocommit=True)
        c.close()
    except pyodbc.Error as e:
        pytest.skip(f"{driver_name} not available: {e}")
    return cs


@pytest.fixture()
def conn(connection_string, driver_name, request):
    for marker_name in ("duckdb", "postgresql"):
        if marker_name in request.node.keywords and driver_name != marker_name:
            pytest.skip(f"requires {marker_name} driver")
    c = pyodbc.connect(connection_string, autocommit=True)
    yield c
    if driver_name != "duckdb":
        cur = c.cursor()
        try:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
            )
            tables = [row[0] for row in cur.fetchall()]
            for table in tables:
                cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
        except Exception:
            pass
        finally:
            cur.close()
    c.close()


@pytest.fixture()
def cursor(conn):
    cur = conn.cursor()
    yield cur
    cur.close()
