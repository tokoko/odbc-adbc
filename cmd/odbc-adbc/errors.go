package main

import "fmt"

type DiagRecord struct {
	sqlState    [5]byte
	nativeError int32
	message     string
}

type DiagnosticRecords struct {
	records []DiagRecord
}

func (d *DiagnosticRecords) clear() {
	d.records = d.records[:0]
}

func (d *DiagnosticRecords) add(sqlState string, nativeError int32, message string) {
	rec := DiagRecord{nativeError: nativeError, message: message}
	copy(rec.sqlState[:], sqlState)
	d.records = append(d.records, rec)
}

func (d *DiagnosticRecords) addError(msg string) {
	d.add("HY000", 0, msg)
}

func (d *DiagnosticRecords) addErrorf(format string, args ...interface{}) {
	d.add("HY000", 0, fmt.Sprintf(format, args...))
}

func (d *DiagnosticRecords) count() int {
	return len(d.records)
}

func (d *DiagnosticRecords) get(recNumber int) *DiagRecord {
	if recNumber < 1 || recNumber > len(d.records) {
		return nil
	}
	return &d.records[recNumber-1]
}
