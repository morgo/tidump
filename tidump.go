package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"os"
	"strings"
	"time"
	"path/filepath"
	log "github.com/sirupsen/logrus"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	_ "github.com/go-sql-driver/mysql"
)

/*
 TODO:
 * Add S3 Interface
 * Add parallel execution
 * Add compression
 * Add progress reporting
 * Add a debug/logging package.

LIMITATIONS:
* Does not backup mysql system tables (plan to do GRANT syntax only)
* Only backups up complete databases

*/

const (
	dumpdir     = "dumpdir"
	bufferLimit = 1024       /* in bytes: the goal of every batch insert.  Will excced because of escape characters and comma delimiter */
	fileLimit   = 100 * 1024 /* Goal of every file is "100K", based on compressed data_length reported by TiDB */
	awsS3Bucket = "backups.tocker.ca"
	awsS3Region = "us-east-1"
)

var StartTime = time.Now()

func main() {

	start := time.Now()
	db, err := sql.Open("mysql", os.Getenv("TIDUMP_MYSQL_CONNECTION"))

	log.SetLevel(log.InfoLevel)
//	log.SetLevel(log.DebugLevel)

	if err != nil {
		log.Fatal("Could not connect to MySQL.  Please make sure you've set MYSQL_CONNECTION.")
	}


	/*
	 Set the tidb_snapshot to NOW()-INTERVAL 1 SECOND.
	 before doing anything else.
	 In future this might be configurable.
	*/

	checkAndSetTiDB(db)


	/*
	 @TODO: Add a Regex to filter the list of tables.

	 This query can be improved by adding more meta data to the server:

	 Create information_schema.TIDB_TABLE_PRIMARY_KEY
	 https://github.com/pingcap/tidb/issues/7714

	*/

	query := `SELECT
	t.table_schema,
	t.table_name,
	if(AVG_ROW_LENGTH=0,100,AVG_ROW_LENGTH) as avg_row_length,
	t.data_length,
	IFNULL(pk.likely_primary_key,'_tidb_rowid'),
	c.insertable
FROM INFORMATION_SCHEMA.TABLES t
LEFT JOIN 
(SELECT table_schema, table_name, column_name as likely_primary_key FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY') pk
ON t.table_schema = pk.table_schema AND t.table_name=pk.table_name
LEFT JOIN 
(SELECT table_schema, table_name, GROUP_CONCAT(COLUMN_NAME)as insertable FROM information_schema.COLUMNS WHERE extra NOT LIKE '%%GENERATED%%' GROUP BY table_schema, table_name) c
ON t.table_schema = c.table_schema AND t.table_name=c.table_name
WHERE t.TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA')`;

	tables, err := db.Query(query)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not read tables from information_schema.  Check MYSQL_CONNECTION is configured correctly.")
	}

	os.Mkdir(dumpdir, 0700)

	for tables.Next() {

		var schema string
		var table string
		var avgRowLength int
		var dataLength uint64
		var likelyPrimaryKey string
		var insertableColumns string

		err = tables.Scan(&schema, &table, &avgRowLength, &dataLength, &likelyPrimaryKey, &insertableColumns)
		check(err)

		primaryKey := discoverPrimaryKey(db, schema, table, likelyPrimaryKey)

		dumpTable(db, schema, table, primaryKey, avgRowLength, dataLength, insertableColumns)

	}

	t := time.Now()
	elapsed := t.Sub(start)

	log.WithFields(log.Fields{		
		"elapsed": elapsed,
	}).Info("Complete")
	
	//	writeMetaDataFile(start, t);

}

/*
func writeMetaDataFile(start time, finish time) {
	// TODO: write meta data file
}
*/

func checkAndSetTiDB(db *sql.DB) {

	/*
	 Check that the minimum version is TiDB 2.1.
	 information_schema.tables was not accurate
	 until RC2.
	*/

	db.Exec("SET group_concat_max_len = 1024 * 1024")

	query := "SET tidb_snapshot = NOW() - INTERVAL 1 SECOND";

	_, err := db.Exec(query)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not set tidb_snapshot.  Check MySQL_CONNECTION is configured and server is TiDB.")
	}

}


/*
 Hopefully this nonsense one day becomes obsolete.

 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func discoverPrimaryKey(db *sql.DB, schema string, table string, likelyPrimaryKey string) (columnName string) {

	// Guess the primary key of the table.
	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s.%s LIMIT 1", schema, table)
	_, err := db.Query(query)
	log.Debug(query)

	if err != nil {
		columnName = likelyPrimaryKey
	} else {
		columnName = "_tidb_rowid"
	}

	return columnName

}

func discoverTableMinMax(db *sql.DB, schema string, table string, primaryKey string) (min int, max int) {

	query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", primaryKey, primaryKey, schema, table)
	err := db.QueryRow(query).Scan(&min, &max)
	log.Debug(query)
	check(err)

	return

}

func discoverRowsPerChunk(avgRowLength int, limit int) int {
	return int(math.Abs(math.Floor(float64(limit) / float64(avgRowLength))))
}


func dumpTable(db *sql.DB, schema string, table string, primaryKey string, avgRowLength int, dataLength uint64, insertableCols string) {

	dumpCreateTable(db, schema, table)

	if dataLength < fileLimit {
		dumpTableData(db, schema, table, primaryKey, insertableCols, -1, -1) // small table
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
			end := i + rowsPerChunk - 1

			if i == min {
				start = -1
			}

			if end > max {
				end = -1
			}

			log.Debug(fmt.Sprintf("Table: %s.%s.  Start: %d End: %d\n", schema, table, start, end))
			dumpTableData(db, schema, table, primaryKey, insertableCols, start, end)

		}

	}

}

func dumpCreateTable(db *sql.DB, schema string, table string) {

	var fakeTable string
	var createTable string

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	file := fmt.Sprintf("dumpdir/%s.%s-schema.sql", schema, table)

	err := db.QueryRow(query).Scan(&fakeTable, &createTable)
	log.Debug(query)
	check(err)

	f, err := os.Create(file)
	check(err)

	_, err = f.WriteString(fmt.Sprintf("%s;\n", createTable))
//	log.Debug(fmt.Sprintf("wrote %d bytes\n", n))
	check(err)

	f.Close()
	copyFileToS3(file)

}

func dumpTableData(db *sql.DB, schema string, table string, primaryKey string, insertableCols string, start int, end int) {

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
		query = fmt.Sprintf("SELECT %s, _tidb_rowid FROM `%s`.`%s` %s ", insertableCols, schema, table, where)
	} else {
		query = fmt.Sprintf("SELECT %s FROM `%s`.`%s` %s ", insertableCols, schema, table, where)
	}

	file := fmt.Sprintf("dumpdir/%s.%s%s.sql", schema, table, prefix)

	// ------------- Dump Data ------------------- //

	rows, err := db.Query(query)
	log.Debug(query)
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

		if buffer.Len()+len(values) > bufferLimit {
			buffer.WriteString(";\n")
			_, err := buffer.WriteTo(f)

//			log.Debug(fmt.Sprintf("wrote %d bytes\n", n))
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
		_, err := buffer.WriteTo(f)
//		log.Debug(fmt.Sprintf("wrote %d bytes\n", n))
		check(err)
		buffer.Reset()
	}

	f.Close()
	copyFileToS3(file)

}

func Map(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}


func quoteIdentifier(identifier string) string {
	return fmt.Sprintf("`%s`", identifier)
}

func copyFileToS3(filename string) {

	awsS3Prefix := fmt.Sprintf("tidb-%s", StartTime)	

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(fmt.Sprintf("Could not open file for upload", filename))
	}
	defer file.Close()

	//select Region to use.
	conf := aws.Config{Region: aws.String(awsS3Region)}
	sess := session.New(&conf)
	svc := s3manager.NewUploader(sess)

	log.Debug("Uploading file to S3...")

	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(awsS3Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", awsS3Prefix, filepath.Base(filename))),
		Body:   file,
	})
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}

	log.Debug(fmt.Sprintf("Successfully uploaded %s to %s\n", filename, result.Location))

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
		log.Fatal(e)
		panic(e)
	}
}
