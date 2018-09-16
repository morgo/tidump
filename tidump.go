package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
	"bytes"

	_ "github.com/go-sql-driver/mysql"
)

/*
 Ideas:

 1) The whole backup can be lock-less by initially setting a @@tidb_snapshot.

 2) Every TiDB table has a numeric primary key (sometimes it is hidden
 as _tidb_rowid, but it's there!).  This means that data can be divided
 into chunks very easily!

 3) I will start with ~parity of mydumper format for export, before
 working on parallel execution.

 4) After parallel execution, work on s3 interface.

 5) Work on compression

*/

func main() {

	start := time.Now()

	db, err := sql.Open("mysql", "root@tcp(localhost:4000)/")
	check(err)

	// Find all the tables in the system

	tables, err := db.Query("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA')")
	check(err)

	os.Mkdir("dumpdir", 0700)

	for tables.Next() {
		var schema string
		var table string
		err = tables.Scan(&schema, &table)
		check(err)

		dumpCreateTable(db, schema, table)
		dumpTableData(db, schema, table) /* @todo: dump ranges */

	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Printf("Execution took %v\n", elapsed)

	// @todo: write the metadata file.

}

func dumpCreateTable(db *sql.DB, schema string, table string) {

	var fakeTable string
	var createTable string

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	file := fmt.Sprintf("dumpdir/%s.%s-schema.sql", schema, table)

	err := db.QueryRow(query).Scan(&fakeTable, &createTable)
	check(err)

	f, err := os.Create(file)
	check(err)

	n, err := f.WriteString(fmt.Sprintf("%s;\n", createTable))
	debug(fmt.Sprintf("wrote %d bytes\n", n))
	check(err)

	f.Close()

}

func dumpTableData(db *sql.DB, schema string, table string) {

	query := fmt.Sprintf("SELECT * FROM `%s`.`%s`", schema, table) // @todo: add _tidb_rowid if present
	file := fmt.Sprintf("dumpdir/%s.%s.sql", schema, table)
	var buffer bytes.Buffer
	bufferLimit := 1024; /* 1024 bytes: will excced because of escape characters and comma delimiter */

	// ------------- Dump Data ------------------- //

	rows, err := db.Query(query)
	check(err)

	f, err := os.Create(file)
	check(err)

	cols, err := rows.Columns()
	colsstr := strings.Join(Map(cols, quoteIdentifier), ",")

	// Result is your slice string.
	rawResult := make([][]byte, len(cols))
	result := make([]string, len(cols))

	dest := make([]interface{}, len(cols)) // A temporary interface{} slice
	for i, _ := range rawResult {
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
				result[i] = fmt.Sprintf("'%s'", escape(string(raw))) // @todo: get smart about escaping the value for numerics.
			}
		}

		values := fmt.Sprintf("(%s)", strings.Join(result, ","))

		if buffer.Len() + len(values) > bufferLimit {
			buffer.WriteString(";\n")
			n, err := buffer.WriteTo(f)
			debug(fmt.Sprintf("wrote %d bytes\n", n))
			check(err)
			buffer.Reset()
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
		n, err := buffer.WriteTo(f)
		debug(fmt.Sprintf("wrote %d bytes\n", n))
		check(err)
		buffer.Reset()
	}

	f.Close()

}


func Map(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func debug(message string) {
	//	fmt.Printf(message)
}

func quoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func escape(source string) string {
	var j int = 0
	if len(source) == 0 {
		return ""
	}
	tempStr := source[:]
	desc := make([]byte, len(tempStr)*2)
	for i := 0; i < len(tempStr); i++ {
		flag := false
		var escape byte
		switch tempStr[i] {
		case '\r':
			flag = true
			escape = '\r'
			break
		case '\n':
			flag = true
			escape = '\n'
			break
		case '\\':
			flag = true
			escape = '\\'
			break
		case '\'':
			flag = true
			escape = '\''
			break
		case '"':
			flag = true
			escape = '"'
			break
		case '\032':
			flag = true
			escape = 'Z'
			break
		default:
		}
		if flag {
			desc[j] = '\\'
			desc[j+1] = escape
			j = j + 2
		} else {
			desc[j] = tempStr[i]
			j = j + 1
		}
	}
	return string(desc[0:j])
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
