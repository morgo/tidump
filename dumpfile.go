package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"
	"strings"
	"sync/atomic"

	"go.uber.org/zap"
)

type dumpFile struct {
	sql    string
	file   string
	start  int64 // primary key
	end    int64 // offset
	fi     *os.File
	gf     *gzip.Writer
	fw     *bufio.Writer
	buffer *bytes.Buffer
	d      *dumper
	zlen   *int64 // actual bytes
	schema string
	table  string
}

func (df dumpFile) close() {

	if err := df.fw.Flush(); err != nil {
		zap.S().Fatal("can not flush buffer: %s", err)
		return
	}

	// Close the gzip first.
	df.gf.Close()
	df.fi.Close()

	df.updateBytesWritten(true)

}

func (df dumpFile) write(s string) (int, error) {
	return df.buffer.WriteString(s)
}

func (df dumpFile) bufferLen() int {
	return df.buffer.Len()
}

/*
 The stat to find length may return an incomplete number,
 since flush() writes but does not sync.  Rather than
 introduce a sync (expensive), we get a final
 number after all files are closed to reconcile.
*/

func (df dumpFile) updateBytesWritten(final bool) {

	var newzlen int64

	if final {
		file, _ := os.Open(df.file)
		stat, _ := file.Stat()
		newzlen = stat.Size()
		file.Close()
	} else {
		stat, _ := df.fi.Stat()
		newzlen = stat.Size()
	}

	diff := newzlen - *df.zlen

	atomic.AddInt64(&df.d.bytesWritten, diff)
	*df.zlen = newzlen

}

func (df dumpFile) flush() error {

	n, err := df.buffer.WriteTo(df.fw)
	atomic.AddInt64(&df.d.bytesDumped, n) // adding uncompressed len

	df.updateBytesWritten(false)

	if err != nil {
		zap.S().Fatal("Could not write to gz file: %s", df.file)
		return err
	}

	df.buffer.Reset()
	return nil
}

func (df *dumpFile) dump() (err error) {
	defer df.close()

	tx := df.d.newTx()

	rows, err := tx.Query(df.sql)
	zap.S().Debug(df.sql)

	if err != nil {
		zap.S().Fatalf("Could not retrieve table data: %s, error: %s", df.schema, df.table, err)
		return err
	}

	cols, _ := rows.Columns()
	types, _ := rows.ColumnTypes()
	colsstr := strings.Join(fnMap(cols, quoteIdentifier), ",")

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	for rows.Next() {
		err = rows.Scan(dest...)
		if err != nil {
			zap.S().Fatalf("Failed to scan row: %s", err)
			return
		}

		for i, raw := range rawResult {
			if raw == nil {
				result[i] = "NULL"
			} else {

				t := types[i].DatabaseTypeName()

				// TODO: are there more numeric types?
				if t == "BIGINT" || t == "INT" || t == "DECIMAL" || t == "FLOAT" {
					result[i] = string(raw)
				} else {
					result[i] = fmt.Sprintf("'%s'", quoteString(string(raw)))
				}
			}
		}

		values := fmt.Sprintf("(%s)", strings.Join(result, ","))

		if int64(df.bufferLen()+len(values)) > df.d.cfg.BulkInsertLimit {
			df.write(";\n")
			df.flush()
		}

		if df.bufferLen() == 0 {
			df.write(fmt.Sprintf("INSERT INTO %s (%s) VALUES \n%s", df.table, colsstr, values))
		} else {
			df.write(",\n")
			df.write(values)
		}

	}

	rows.Close()
	tx.Commit() // return to pool

	// Flush any remaining buffer

	if df.bufferLen() > 0 {
		df.write(";\n")
		df.flush()
	}

	if err = df.d.queueFileToS3(df.file); err != nil {
		return err
	}

	return nil
}
