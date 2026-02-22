# odbc-adbc

An ODBC driver that wraps [ADBC](https://arrow.apache.org/adbc/) drivers, allowing any ODBC application to connect to ADBC-supported databases.

Built in Go as a C shared library, it translates ODBC calls into ADBC calls and serves Arrow columnar data row-by-row to ODBC clients.

## Prerequisites

- [Go](https://go.dev/)
- [pixi](https://pixi.sh/)
- ADBC drivers installed via [dbc](https://github.com/apache/arrow-adbc) (e.g. `dbc install duckdb`)

## Build

```sh
pixi run build
```

This produces `odbc-adbc.so`, an ODBC driver you can use directly.

## Usage

Connect using `Driver=` with the path to the built `.so` and pass `adbc_driver=` to specify which ADBC backend to use. Any additional parameters are forwarded to the ADBC driver.

### pyodbc

```python
import pyodbc

conn = pyodbc.connect(
    "Driver=./odbc-adbc.so;"
    "adbc_driver=duckdb;"
    "path=:memory:",
    autocommit=True,
)

cursor = conn.cursor()

cursor.execute("CREATE TABLE people (id INTEGER, name VARCHAR, age INTEGER)")
cursor.execute("INSERT INTO people VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)")
cursor.execute("SELECT * FROM people ORDER BY id")

print(f"Columns: {[col[0] for col in cursor.description]}")
for row in cursor:
    print(row)

cursor.close()
conn.close()
```

### PostgreSQL

```python
conn = pyodbc.connect(
    "Driver=./odbc-adbc.so;"
    "adbc_driver=postgresql;"
    "uri=postgresql://user:pass@localhost:5432/mydb",
    autocommit=True,
)
```

## Connection String Parameters

| Parameter | Description |
|---|---|
| `Driver` | Path to `odbc-adbc.so` |
| `adbc_driver` | ADBC driver name (e.g. `duckdb`, `postgresql`) or path to `.so` |

All other parameters are passed through to the ADBC driver (e.g. `path`, `uri`).

## Tests

```sh
pixi run test
```

Tests run against all configured ADBC drivers. To run against a single driver:

```sh
ODBC_ADBC_DRIVER=duckdb pixi run test
```

PostgreSQL tests require a running instance (e.g. `docker compose up -d`).
