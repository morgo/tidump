package main

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"
	"bytes"
	"math"

	_ "github.com/go-sql-driver/mysql"
)

/*
 TODO:

 * Make consistent snapshot, not with locks but
   set a @@tidb_snapshot.
 * Add virtual column support.
 * Add support for _tidb_rowid
 * Add parallel execution
 * Add S3 Interface
 * Add compression
 * Add progress reporting

LIMITATIONS:
* Does not backup mysql system tables (plan to do GRANT syntax only)
* Only backups up complete databases

*/

const (
 dumpdir = "dumpdir"
 connection = "root@tcp(localhost:4000)/"
 bufferLimit = 1024 /* in bytes: the goal of every batch insert.  Will excced because of escape characters and comma delimiter */
 fileLimit = 100*1024 /* Goal of every file is "100K", based on compressed data_length reported by TiDB */
)

func main() {

	start := time.Now()

	db, err := sql.Open("mysql", connection)
	check(err)

	/*
	 Find all the tables in the system.
	 TODO:
	 * Support regex
	 * Get True Primary Key Name
	*/

	 // avg row length may be zero.
	tables, err := db.Query("SELECT TABLE_SCHEMA, TABLE_NAME, AVG_ROW_LENGTH, DATA_LENGTH FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA')")
	check(err)

	os.Mkdir(dumpdir, 0700)

	for tables.Next() {
		var schema string
		var table string
		var avgRowLength int
		var dataLength uint64
		err = tables.Scan(&schema, &table, &avgRowLength, &dataLength)
		check(err)

		dumpTable(db,schema,table, "_tidb_rowid", avgRowLength, dataLength)

	}

	t := time.Now()
	elapsed := t.Sub(start)
	fmt.Printf("Execution took %v\n", elapsed)
//	writeMetaDataFile(start, t);

}
/*
func writeMetaDataFile(start time, finish time) {
	// TODO: write meta data file
}
*/

func discoverTableMinMax(db *sql.DB, schema string, table string, primaryKey string) (min int, max int) {

		query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", primaryKey, primaryKey, schema, table)
		err := db.QueryRow(query).Scan(&min, &max)
		check(err)

		return
		
}

func discoverRowsPerChunk(avgRowLength int, limit int) int {
	return int(math.Abs(math.Floor(float64(limit) / float64(avgRowLength))))
}


func dumpTable(db *sql.DB, schema string, table string, primaryKey string, avgRowLength int, dataLength uint64) {

	dumpCreateTable(db, schema, table)

	if dataLength < fileLimit {
		dumpTableData(db, schema, table, primaryKey, -1, -1) // small table
	} else {

		/*
		 We need to figure out based on the reported avgRowLength,
		 how many rows per chunk.  We can then quer the max+min values,
		 and add some unncessary off by one handling.
		*/

		rowsPerChunk := discoverRowsPerChunk(avgRowLength, fileLimit)
		min, max := discoverTableMinMax(db, schema, table, primaryKey)


		for i := min; i < max; i += rowsPerChunk {

			start := i
			end   := i + rowsPerChunk - 1

			if i == min {
				start = -1
			}

			if end > max {
				end = -1
			}

			fmt.Printf("Table: %s.%s.  Start: %d End: %d\n", schema, table, start, end)
			dumpTableData(db, schema, table, primaryKey, start, end)

		}

	}

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

func dumpTableData(db *sql.DB, schema string, table string, primaryKey string, start int, end int) {

	var buffer bytes.Buffer
	var where, query string
	var prefix = ""

	if start == -1 && end != -1 {
		where = fmt.Sprintf("WHERE %s < %d", primaryKey, end)
		prefix = fmt.Sprintf(".%d", 0)
	} else if start != -1 && end != -1 {
		where = fmt.Sprintf("WHERE %s BETWEEN %d AND %d", primaryKey, start, end)
		prefix = fmt.Sprintf(".%d", start)
	} else if start != -1 && end == -1 {
		where = fmt.Sprintf("WHERE %s > %d", primaryKey, start)
		prefix = fmt.Sprintf(".%d", start)
	}

	if primaryKey == "_tidb_rowid" {
		query = fmt.Sprintf("SELECT *, _tidb_rowid FROM `%s`.`%s` %s ", schema, table, where)
	} else {
		query = fmt.Sprintf("SELECT * FROM `%s`.`%s` %s ", schema, table, where)
	}

	fmt.Println(query)

	file := fmt.Sprintf("dumpdir/%s.%s%s.sql", schema, table, prefix)

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
