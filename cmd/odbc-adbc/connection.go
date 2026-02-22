package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

type Connection struct {
	envID      uintptr
	db         adbc.Database
	conn       adbc.Connection
	autocommit bool
	diags      DiagnosticRecords
}

// parseConnectionString parses an ODBC connection string into key-value pairs.
// Format: key1=value1;key2=value2;...
// Values can be enclosed in curly braces: key={value with ; semicolons}
func parseConnectionString(connStr string) map[string]string {
	opts := make(map[string]string)
	remaining := connStr

	for len(remaining) > 0 {
		remaining = strings.TrimSpace(remaining)
		if len(remaining) == 0 {
			break
		}

		eqIdx := strings.IndexByte(remaining, '=')
		if eqIdx < 0 {
			break
		}
		key := strings.TrimSpace(remaining[:eqIdx])
		remaining = remaining[eqIdx+1:]

		var value string
		if len(remaining) > 0 && remaining[0] == '{' {
			// Brace-delimited value
			endIdx := strings.IndexByte(remaining, '}')
			if endIdx < 0 {
				value = remaining[1:]
				remaining = ""
			} else {
				value = remaining[1:endIdx]
				remaining = remaining[endIdx+1:]
				if len(remaining) > 0 && remaining[0] == ';' {
					remaining = remaining[1:]
				}
			}
		} else {
			semiIdx := strings.IndexByte(remaining, ';')
			if semiIdx < 0 {
				value = strings.TrimSpace(remaining)
				remaining = ""
			} else {
				value = strings.TrimSpace(remaining[:semiIdx])
				remaining = remaining[semiIdx+1:]
			}
		}

		if len(key) > 0 {
			opts[key] = value
		}
	}
	return opts
}

func (c *Connection) connect(connStr string) error {
	c.diags.clear()

	opts := parseConnectionString(connStr)

	// Remove the ODBC Driver= key (set by the Driver Manager or user).
	// The Driver Manager treats "driver" as case-insensitive and consumes it,
	// so we use "adbc_driver" and "adbc_entrypoint" in the connection string
	// and map them to the drivermgr's expected "driver" and "entrypoint" keys.
	delete(opts, "Driver")
	delete(opts, "driver")

	if v, ok := opts["adbc_driver"]; ok {
		opts["driver"] = v
		delete(opts, "adbc_driver")
	}
	if v, ok := opts["adbc_entrypoint"]; ok {
		opts["entrypoint"] = v
		delete(opts, "adbc_entrypoint")
	}

	if _, ok := opts["driver"]; !ok {
		return fmt.Errorf("connection string must contain adbc_driver parameter (ADBC driver name or path)")
	}

	var drv drivermgr.Driver

	db, err := drv.NewDatabase(opts)
	if err != nil {
		return fmt.Errorf("failed to create ADBC database: %w", err)
	}

	conn, err := db.Open(context.Background())
	if err != nil {
		db.Close()
		return fmt.Errorf("failed to open ADBC connection: %w", err)
	}

	c.db = db
	c.conn = conn
	c.autocommit = true
	return nil
}

func (c *Connection) disconnect() error {
	c.diags.clear()
	var firstErr error
	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			firstErr = err
		}
		c.conn = nil
	}
	if c.db != nil {
		if err := c.db.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.db = nil
	}
	return firstErr
}
