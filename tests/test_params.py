def test_param_int(cursor):
    cursor.execute("SELECT ? + 1", 42)
    row = cursor.fetchone()
    assert row[0] == 43


def test_param_string(cursor):
    cursor.execute("SELECT ?", "hello")
    row = cursor.fetchone()
    assert row[0] == "hello"


def test_param_float(cursor):
    cursor.execute("SELECT ?", 3.14)
    row = cursor.fetchone()
    # PostgreSQL may return as string when type context is missing
    assert abs(float(row[0]) - 3.14) < 0.001


def test_param_null(cursor):
    cursor.execute("SELECT ?", None)
    row = cursor.fetchone()
    assert row[0] is None


def test_param_multiple(cursor):
    cursor.execute("SELECT ?, ?", 1, "two")
    row = cursor.fetchone()
    assert int(row[0]) == 1
    assert str(row[1]) == "two"


def test_param_in_where(cursor):
    cursor.execute("CREATE TABLE test_param_where (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO test_param_where VALUES (1, 'alice')")
    cursor.execute("INSERT INTO test_param_where VALUES (2, 'bob')")
    cursor.execute("SELECT name FROM test_param_where WHERE id = ?", 1)
    row = cursor.fetchone()
    assert row[0] == "alice"


def test_param_insert(cursor):
    cursor.execute("CREATE TABLE test_param_ins (id INTEGER, name VARCHAR(50))")
    cursor.execute("INSERT INTO test_param_ins VALUES (?, ?)", 1, "hello")
    cursor.execute("SELECT id, name FROM test_param_ins")
    row = cursor.fetchone()
    assert row[0] == 1
    assert row[1] == "hello"
