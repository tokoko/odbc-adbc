import pyodbc
import os

driver_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "odbc-adbc.so")

conn = pyodbc.connect(
    f"Driver={driver_path};adbc_driver=duckdb;path=:memory:",
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
