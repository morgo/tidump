package main

import (
	"bytes"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	_ "github.com/go-sql-driver/mysql"
)

/*
 TODO:
 * Change the mysql connection to be a pool -> Make sure all members of the pool have the TIDB_SNAPSHOT set.
 * Delete local files after copy to S3
 * Pause dumps if local tmpsize exceeds threshold.
 * Add compression
 * Improve escaping function (don't quote integers!)
 * Add regular expression to match databases

LIMITATIONS:
* Does not backup mysql system tables (plan to do GRANT syntax only)

*/

var StartTime = time.Now()
var MySQLConnectionString, AwsS3Bucket, AwsS3BucketPrefix, AwsS3Region, TmpDir string
var FileTargetSize, BulkInsertLimit, TmpDirMax uint64
var BytesDumped, BytesCopied int64
var FilesDumpCompleted, FilesCopyCompleted, TotalFiles int

var TableDumpWg, TableCopyWg, SchemaCopyWg, SchemaDumpWg sync.WaitGroup

func main() {

	MySQLConnectionString = getenv("TIDUMP_MYSQL_CONNECTION", "root@tcp(localhost:4000)/")
	db, err := sql.Open("mysql", MySQLConnectionString)

	if err != nil {
		log.Fatal("Could not connect to MySQL.  Please make sure you've set MYSQL_CONNECTION.")
	}

	/*
	 Set the tidb_snapshot to NOW()-INTERVAL 1 SECOND.
	 before doing anything else.
	 In future this might be configurable.
	*/

	configCheckAndSetTiDBSnapshot(db)

	go publishStatus()

	/*

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
FROM
 INFORMATION_SCHEMA.TABLES t
LEFT JOIN 
 (SELECT table_schema, table_name, column_name as likely_primary_key FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY') pk
 ON t.table_schema = pk.table_schema AND t.table_name=pk.table_name
LEFT JOIN 
 (SELECT table_schema, table_name, GROUP_CONCAT(COLUMN_NAME)as insertable FROM information_schema.COLUMNS WHERE extra NOT LIKE '%%GENERATED%%' GROUP BY table_schema, table_name) c
 ON t.table_schema = c.table_schema AND t.table_name=c.table_name
WHERE
 t.TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA')`

	tables, err := db.Query(query)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not read tables from information_schema.  Check MYSQL_CONNECTION is configured correctly.")
	}

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

		go dumpCreateTable(db, schema, table)
		SchemaDumpWg.Add(1)

		prepareDumpTable(db, schema, table, primaryKey, avgRowLength, dataLength, insertableColumns)

	}

	// There is no schema dump Wg, it happens syncronous.

	TableDumpWg.Wait()
	TableCopyWg.Wait()
	SchemaDumpWg.Wait()
	SchemaCopyWg.Wait()

	// One final status line.

	status()

	t := time.Now()
	elapsed := t.Sub(StartTime)

	log.WithFields(log.Fields{
		"elapsed": elapsed,
	}).Info("Complete")

	//	writeMetaDataFile(start, t);

}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func status() {
	log.Info(fmt.Sprintf("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", TotalFiles, FilesDumpCompleted, FilesCopyCompleted))
	log.Info(fmt.Sprintf("BytesDumped: %d, Copied (success): %d, TmpSize: %d", BytesDumped, BytesCopied, (BytesDumped-BytesCopied)))

}

func publishStatus() {

	for {
		status()
		time.Sleep(2 * time.Second)
	}

}

/*
func writeMetaDataFile(start time, finish time) {
	// TODO: write meta data file
}
*/

func configCheckAndSetTiDBSnapshot(db *sql.DB) {

	log.SetLevel(log.InfoLevel)
	//	log.SetLevel(log.DebugLevel)

	AwsS3Bucket = getenv("TIDUMP_AWS_S3_BUCKET", "backups.tocker.ca")
	AwsS3Region = getenv("TIDUMP_AWS_S3_REGION", "us-east-1")
	FileTargetSize = 100 * 1024                      // uint64(getenv("AWS_S3_FILE_TARGET_SIZE", string(100 * 1024))) // 100KiB
	BulkInsertLimit = 1024                           // uint64(getenv("BULK_INSERT_LIMIT", string(1024))) // 1KiB
	TmpDirMax = 5 * 1024 * 1024 * 1024               // 5 GiB
	TmpDir = getenv("TIDUMP_TMPDIR", "/tmp/tidump/") // TODO: use mktemp

	os.Mkdir(TmpDir, 0700)

	/*
	 Check that the minimum version is TiDB 2.1.
	 information_schema.tables was not accurate
	 until RC2.
	*/

	db.Exec("SET group_concat_max_len = 1024 * 1024")

	// TODO: get the hostname, and assign it to the S3 bucket prefix.
	AwsS3BucketPrefix = getenv("AWS_S3_BUCKET_PREFIX", "blah")

	query := "SET tidb_snapshot = NOW() - INTERVAL 1 SECOND"

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

func discoverRowsPerFile(avgRowLength int, fileTargetSize uint64) int {
	return int(math.Abs(math.Floor(float64(fileTargetSize) / float64(avgRowLength))))
}

func prepareDumpTable(db *sql.DB, schema string, table string, primaryKey string, avgRowLength int, dataLength uint64, insertableCols string) {

	if dataLength < FileTargetSize {
		TableDumpWg.Add(1)
		go dumpTableData(db, schema, table, primaryKey, insertableCols, -1, -1) // small table
		TotalFiles += 1
	} else {

		/*
		 We need to figure out based on the reported avgRowLength,
		 how many rows per file.  We can then quer the max+min values,
		 and add some unncessary off by one handling.
		*/

		rowsPerFile := discoverRowsPerFile(avgRowLength, FileTargetSize)
		min, max := discoverTableMinMax(db, schema, table, primaryKey)
		TotalFiles += int(math.Ceil(float64(max-min) / float64(rowsPerFile)))

		for i := min; i < max; i += rowsPerFile {

			start := i
			end := i + rowsPerFile - 1

			if i == min {
				start = -1
			}

			if end > max {
				end = -1
			}

			log.Debug(fmt.Sprintf("Table: %s.%s.  Start: %d End: %d\n", schema, table, start, end))
			TableDumpWg.Add(1)
			go dumpTableData(db, schema, table, primaryKey, insertableCols, start, end)

		}

	}

}

func dumpCreateTable(db *sql.DB, schema string, table string) {

	defer SchemaDumpWg.Done()

	var fakeTable string
	var createTable string

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	file := fmt.Sprintf("%s%s.%s-schema.sql", TmpDir, schema, table)

	err := db.QueryRow(query).Scan(&fakeTable, &createTable)
	log.Debug(query)
	check(err)

	f, err := os.Create(file)
	check(err)

	_, err = f.WriteString(fmt.Sprintf("%s;\n", createTable))
	//	log.Debug(fmt.Sprintf("wrote %d bytes\n", n))
	check(err)

	f.Close()
	SchemaCopyWg.Add(1)
	go copyFileToS3(file, "schema")

}

func dumpTableData(db *sql.DB, schema string, table string, primaryKey string, insertableCols string, start int, end int) {

	defer TableDumpWg.Done()

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

	file := fmt.Sprintf("%s%s.%s%s.sql", TmpDir, schema, table, prefix)

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

		if uint64(buffer.Len()+len(values)) > BulkInsertLimit {
			buffer.WriteString(";\n")
			n, err := buffer.WriteTo(f)
	        atomic.AddInt64(&BytesDumped, n)


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
		n, err := buffer.WriteTo(f)
        atomic.AddInt64(&BytesDumped, n)
		check(err)
		buffer.Reset()
	}

	f.Close()
	FilesDumpCompleted += 1
	TableCopyWg.Add(1)
	go copyFileToS3(file, "table")

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

func copyFileToS3(filename string, copyType string) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(fmt.Sprintf("Could not open file for upload", filename))
	}
	defer file.Close()

	//select Region to use.
	conf := aws.Config{Region: aws.String(AwsS3Region)}
	sess := session.New(&conf)
	svc := s3manager.NewUploader(sess)

	log.Debug("Uploading file to S3...")

	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(AwsS3Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", AwsS3BucketPrefix, filepath.Base(filename))),
		Body:   file,
	})
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}

	log.Debug(fmt.Sprintf("Successfully uploaded %s to %s\n", filename, result.Location))

	if copyType != "schema" {
		FilesCopyCompleted += 1
		fi, _ := file.Stat()
        atomic.AddInt64(&BytesCopied, fi.Size())
		TableCopyWg.Done()
	} else {
		SchemaCopyWg.Done()
	}

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
