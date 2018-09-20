package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/ngaut/log"
	"os"
	"strings"
	"sync/atomic"
)

type dumpFile struct {
	schema string
	table  string
	sql    string
	file   string
	start  int64
	end    int64
	fi     *os.File
	gf     *gzip.Writer
	fw     *bufio.Writer
	buffer *bytes.Buffer
	dt     *dumpTable
}

func (dt *dumpTable) NewDumpFile(start int64, end int64) (*dumpFile, error) {

	var err error

	df := &dumpFile{
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

	df.sql = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s AND %s", df.dt.insertableColumns, df.dt.schema, df.dt.table, startSql, endSql)
	df.file = fmt.Sprintf("%s/%s.%s.%d.sql.gz", df.dt.d.cfg.TmpDir, df.dt.schema, df.dt.table, df.start)

	df.fi, err = os.Create(df.file)

	if err != nil {
		log.Fatalf("Error in creating file: %s", df.file)
	}

	df.gf = gzip.NewWriter(df.fi)
	df.fw = bufio.NewWriter(df.gf)
	df.buffer = new(bytes.Buffer)

	return df, nil

}

func (df dumpFile) close() {
	df.fw.Flush()
	// Close the gzip first.
	df.gf.Close()
	df.fi.Close()
}

func (df dumpFile) write(s string) (int, error) {

	// TODO: Get the zlib buffer len?
	return df.buffer.WriteString(s)
	// TODO: return zlib new length - old length?

}

func (df dumpFile) bufferLen() int {
	return df.buffer.Len()
}

func (df dumpFile) flush() {

	uncompressedLen := int64(df.bufferLen())

	if df.dt.d.canSafelyWriteToTmpdir(uncompressedLen) {

		n, err := df.buffer.WriteTo(df.fw)
		atomic.AddInt64(&df.dt.d.BytesDumped, n)

		if err != nil {
			log.Fatal("Could not write to gz file: %s", df.file)
		}

		df.buffer.Reset()
	}

}

func (df *dumpFile) dump() {

	defer df.dt.d.TableDumpWg.Done()

	db := df.dt.d.newDbConnection()
	defer db.Close()

	rows, err := db.Query(df.sql)
	log.Debug(df.sql)

	if err != nil {
		log.Fatal("Could not retrieve table data: %s", df.dt.schema, df.dt.table)
	}

	cols, _ := rows.Columns()
	types, _ := rows.ColumnTypes()
	colsstr := strings.Join(Map(cols, quoteIdentifier), ",")

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
			fmt.Println("Failed to scan row", err)
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

	// Flush any remaining buffer

	if df.bufferLen() > 0 {
		df.write(";\n")
		df.flush()
	}

	df.close()
	atomic.AddInt64(&df.dt.d.FilesDumpCompleted, 1)
	df.dt.d.TableCopyWg.Add(1)
	go df.dt.d.copyFileToS3(df.file, "table")

}
