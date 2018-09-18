package main

import (
	"fmt"
	"math"
	"sync/atomic"
	log "github.com/sirupsen/logrus"
	"strings"
	"bytes"
	"os"
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

)

func prepareDumpTable(db *sql.DB, schema string, table string, primaryKey string, avgRowLength int, dataLength int64, insertableCols string) {

	if dataLength < FileTargetSize {
		TableDumpWg.Add(1)
		go dumpTableData(schema, table, primaryKey, insertableCols, 0, 0) // small table
		atomic.AddInt64(&TotalFiles, 1)
	} else {

		/*
		 We need to figure out based on the reported avgRowLength,
		 how many rows per file.  We can then quer the max+min values,
		 and add some unncessary off by one handling.
		*/

		rowsPerFile := discoverRowsPerFile(avgRowLength, FileTargetSize)
		min, max := discoverTableMinMax(db, schema, table, primaryKey)
		atomic.AddInt64(&TotalFiles, int64(math.Ceil(float64(max-min)/float64(rowsPerFile))))

		for i := min; i < max; i += rowsPerFile {

			start := i
			end := i + rowsPerFile - 1

			if i == min {
				start = 0
			}

			if end > max {
				end = 0
			}

			log.Debug(fmt.Sprintf("Table: %s.%s.  Start: %d End: %d\n", schema, table, start, end))
			TableDumpWg.Add(1)
			go dumpTableData(schema, table, primaryKey, insertableCols, start, end)

		}
	}
}

func dumpTableData(schema string, table string, primaryKey string, insertableCols string, start int, end int) {

	defer TableDumpWg.Done()

	/* Create a new MySQL connection for this thread */
	db, err := sql.Open("mysql", MySQLConnectionString)

	if err != nil {
		log.Fatal("Could not create worker thread connection to MySQL.")
	}

	defer db.Close()
	setTiDBSnapshot(db) // set worker thread to same place as master thread.

	startSql := "1=1"
	endSql := "1=1"

	var query string

	if start != 0 {
		startSql = fmt.Sprintf("%s > %d", primaryKey, start)
	}

	if end != 0 {
		endSql = fmt.Sprintf("%s < %d", primaryKey, end)
	}

	if primaryKey == "_tidb_rowid" {
		query = fmt.Sprintf("SELECT %s, _tidb_rowid FROM `%s`.`%s` WHERE %s AND %s", insertableCols, schema, table, startSql, endSql)
	} else {
		query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s AND %s", insertableCols, schema, table, startSql, endSql)
	}

	file := fmt.Sprintf("%s/%s.%s.%d.sql", TmpDir, schema, table, start)

	// ------------- Dump Data ------------------- //

	rows, err := db.Query(query)
	log.Debug(query)
	check(err)

	f, err := os.Create(file)
	check(err)

	cols, _ := rows.Columns()
	types, _ := rows.ColumnTypes()
	colsstr := strings.Join(Map(cols, quoteIdentifier), ",")

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
		dest[i] = &rawResult[i] // Put pointers to each string in the interface slice
	}

	var buffer bytes.Buffer

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
					result[i] = fmt.Sprintf("'%s'", escape(string(raw)))
				}
			}
		}

		values := fmt.Sprintf("(%s)", strings.Join(result, ","))

		if int64(buffer.Len()+len(values)) > BulkInsertLimit {
			buffer.WriteString(";\n")

			if canSafelyWriteToTmpdir(int64(buffer.Len())) {
				n, err := buffer.WriteTo(f)
				atomic.AddInt64(&BytesDumped, n)
				check(err)
				buffer.Reset()
			}
		}

		if buffer.Len() == 0 {
			buffer.WriteString(fmt.Sprintf("INSERT INTO %s (%s) VALUES \n%s", table, colsstr, values))
		} else {
			buffer.WriteString(",\n")
			buffer.WriteString(values)
		}

	}

	// Flush any remaining buffer

	if buffer.Len() > 0 {
		buffer.WriteString(";\n")

		if canSafelyWriteToTmpdir(int64(buffer.Len())) {
			n, err := buffer.WriteTo(f)
			atomic.AddInt64(&BytesDumped, n)
			check(err)
			buffer.Reset()
		}
	}

	f.Close()
	atomic.AddInt64(&FilesDumpCompleted, 1)
	TableCopyWg.Add(1)
	go copyFileToS3(file, "table")

}
