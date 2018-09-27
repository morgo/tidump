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
	dt     *dumpTable
	zlen   *int64 // actual bytes
}

func (dt *dumpTable) NewDumpFile(start int64, end int64) (df *dumpFile, err error) {

	dt.d.dumpWg.Add(1)
	atomic.AddInt64(&dt.d.totalFiles, 1)

	df = &dumpFile{
		start: start,
		end:   end,
		dt:    dt,
	}

	startSql := "1=1"
	endSql := "1=1"

	if df.start != 0 {
		startSql = fmt.Sprintf("%s > %d", df.dt.primaryKey, df.start)
	}

	if df.end != 0 {
		endSql = fmt.Sprintf("%s < %d", df.dt.primaryKey, df.end)
	}

	df.sql = fmt.Sprintf("SELECT LOW_PRIORITY %s FROM `%s`.`%s` WHERE %s AND %s", df.dt.insertableColumns, df.dt.schema, df.dt.table, startSql, endSql)
	df.file = fmt.Sprintf("%s/%s.%s.%d.sql.gz", df.dt.d.cfg.TmpDir, df.dt.schema, df.dt.table, df.start)

	df.fi, err = os.Create(df.file)

	if err != nil {
		zap.S().Fatalf("Error in creating file: %s", df.file)
	}

	df.gf = gzip.NewWriter(df.fi)
	df.fw = bufio.NewWriter(df.gf)
	df.buffer = new(bytes.Buffer)

	df.zlen = new(int64)

	return

}

func (df dumpFile) close() {

	df.fw.Flush()
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

	atomic.AddInt64(&df.dt.d.bytesWritten, diff)
	*df.zlen = newzlen

}

func (df dumpFile) flush() {

	uncompressedLen := int64(df.bufferLen())

	if df.dt.d.canSafelyWriteToTmpdir(uncompressedLen) {

		n, err := df.buffer.WriteTo(df.fw)
		atomic.AddInt64(&df.dt.d.bytesDumped, n) // adding uncompressed len

		df.updateBytesWritten(false)

		if err != nil {
			zap.S().Fatal("Could not write to gz file: %s", df.file)
		}

		df.buffer.Reset()
	}

}

func (df *dumpFile) dump() {

	defer df.dt.d.dumpWg.Done()

	tx := df.dt.d.newTx()

	rows, err := tx.Query(df.sql)
	zap.S().Debug(df.sql)

	if err != nil {
		zap.S().Fatalf("Could not retrieve table data: %s, error: %s", df.dt.schema, df.dt.table, err)
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

		if int64(df.bufferLen()+len(values)) > df.dt.d.cfg.BulkInsertLimit {
			df.write(";\n")
			df.flush()
		}

		if df.bufferLen() == 0 {
			df.write(fmt.Sprintf("INSERT INTO %s (%s) VALUES \n%s", df.dt.table, colsstr, values))
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

	df.close()
	df.dt.d.copyFileToS3(df.file)

}
