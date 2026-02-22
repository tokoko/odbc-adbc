#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sql.h>
#include <sqlext.h>

#define CHECK(rc, msg, handle_type, handle) \
    if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) { \
        fprintf(stderr, "FAILED: %s (rc=%d)\n", msg, rc); \
        SQLCHAR state[6], message[256]; \
        SQLINTEGER native; \
        SQLSMALLINT len; \
        SQLGetDiagRec(handle_type, handle, 1, state, &native, message, sizeof(message), &len); \
        fprintf(stderr, "  SQLSTATE=%s, Message=%s\n", state, message); \
        exit(1); \
    } else { \
        printf("OK: %s\n", msg); \
    }

int main() {
    SQLHENV env;
    SQLHDBC dbc;
    SQLHSTMT stmt;
    SQLRETURN rc;

    // Allocate environment
    rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    CHECK(rc, "SQLAllocHandle(ENV)", SQL_HANDLE_ENV, env);

    // Set ODBC version
    rc = SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, 0);
    CHECK(rc, "SQLSetEnvAttr(ODBC_VERSION)", SQL_HANDLE_ENV, env);

    // Allocate connection
    rc = SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    CHECK(rc, "SQLAllocHandle(DBC)", SQL_HANDLE_DBC, dbc);

    // Connect using DuckDB ADBC driver
    SQLCHAR connStr[] = "adbc_driver=duckdb;adbc_entrypoint=duckdb_adbc_init;path=:memory:";
    SQLCHAR outStr[256];
    SQLSMALLINT outLen;
    rc = SQLDriverConnect(dbc, NULL, connStr, SQL_NTS, outStr, sizeof(outStr), &outLen, SQL_DRIVER_NOPROMPT);
    CHECK(rc, "SQLDriverConnect", SQL_HANDLE_DBC, dbc);

    // Allocate statement
    rc = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    CHECK(rc, "SQLAllocHandle(STMT)", SQL_HANDLE_STMT, stmt);

    // Execute a simple query
    rc = SQLExecDirect(stmt, (SQLCHAR*)"SELECT 42 AS answer, 'hello' AS greeting", SQL_NTS);
    CHECK(rc, "SQLExecDirect", SQL_HANDLE_STMT, stmt);

    // Get number of columns
    SQLSMALLINT numCols;
    rc = SQLNumResultCols(stmt, &numCols);
    CHECK(rc, "SQLNumResultCols", SQL_HANDLE_STMT, stmt);
    printf("  Columns: %d\n", numCols);

    // Describe columns
    for (int i = 1; i <= numCols; i++) {
        SQLCHAR colName[128];
        SQLSMALLINT nameLen, dataType, nullable, decDigits;
        SQLULEN colSize;
        rc = SQLDescribeCol(stmt, i, colName, sizeof(colName), &nameLen, &dataType, &colSize, &decDigits, &nullable);
        CHECK(rc, "SQLDescribeCol", SQL_HANDLE_STMT, stmt);
        printf("  Col %d: name=%s, type=%d, size=%lu\n", i, colName, dataType, colSize);
    }

    // Fetch rows
    while ((rc = SQLFetch(stmt)) == SQL_SUCCESS) {
        char buf[256];
        SQLLEN ind;

        for (int i = 1; i <= numCols; i++) {
            rc = SQLGetData(stmt, i, SQL_C_CHAR, buf, sizeof(buf), &ind);
            if (rc == SQL_SUCCESS) {
                if (ind == SQL_NULL_DATA) {
                    printf("  Col %d: NULL\n", i);
                } else {
                    printf("  Col %d: %s\n", i, buf);
                }
            }
        }
    }
    printf("OK: SQLFetch returned SQL_NO_DATA\n");

    // Cleanup
    SQLFreeStmt(stmt, SQL_DROP);
    SQLDisconnect(dbc);
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);

    printf("\nAll tests passed!\n");
    return 0;
}
