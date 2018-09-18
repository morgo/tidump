package main

import (
	"bytes"
	"database/sql"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
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
 * Change the mysql connection to be a pool (currently workers use their own connection, and goroutines could overload source.)
 * Design structs to hold backup

ROADMAP/FEATURE IDEAS:
* Compress files before uploading to S3
* Add regular expression to match databases

LIMITATIONS:
* Does not backup users.  Waiting on TIDB #7733.
* Not efficient at finding Primary Key.  Waiting on TiDB #7714.
* Does not do reads as low priority.  Prefer to use TiDB #7524
* Files may not be equal in size (may be fixed in TiDB #7714)
* Server does not expose version in easily parsable format (Need to File)
*/

var StartTime = time.Now()
var MySQLConnectionString, MySQLNow, AwsS3Bucket, AwsS3BucketPrefix, AwsS3Region, TmpDir string
var FileTargetSize, BulkInsertLimit, BytesDumped, BytesCopied, TmpDirMax int64
var FilesDumpCompleted, FilesCopyCompleted, TotalFiles int64

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

	preflightChecks(db)
	setTiDBSnapshot(db)

	go publishStatus() // every 2 seconds

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
 (SELECT table_schema, table_name, column_name as likely_primary_key FROM information_schema.key_column_usage WHERE constraint_name='PRIMARY' AND TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA') ) pk
 ON t.table_schema = pk.table_schema AND t.table_name=pk.table_name
LEFT JOIN 
 (SELECT table_schema, table_name, GROUP_CONCAT(COLUMN_NAME)as insertable FROM information_schema.COLUMNS WHERE extra NOT LIKE '%%GENERATED%%' AND TABLE_SCHEMA NOT IN ('mysql', 'INFORMATION_SCHEMA', 'PERFORMANCE_SCHEMA') GROUP BY table_schema, table_name) c
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
		var dataLength int64
		var likelyPrimaryKey string
		var insertableColumns string

		err = tables.Scan(&schema, &table, &avgRowLength, &dataLength, &likelyPrimaryKey, &insertableColumns)
		check(err)

		primaryKey := discoverPrimaryKey(db, schema, table, likelyPrimaryKey)

		go dumpCreateTable(db, schema, table)
		SchemaDumpWg.Add(1)

		prepareDumpTable(db, schema, table, primaryKey, avgRowLength, dataLength, insertableColumns)

	}

	dumpPermissions(db) // does nothing

	/*
	 The work is handled in goroutines.
	 The dump routines write to the tmpdir, and then
	 trigger a gorouting for copying to S3.
	*/

	TableDumpWg.Wait()
	TableCopyWg.Wait()
	SchemaDumpWg.Wait()
	SchemaCopyWg.Wait()

	os.RemoveAll(TmpDir) // delete temporary directory
	status()             // print status before exiting

	t := time.Now()
	elapsed := t.Sub(StartTime)

	log.WithFields(log.Fields{
		"StartTime":  StartTime,
		"FinishTime": t,
		"elapsed":    elapsed,
	}).Info("Complete")

}

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func status() {

	freeSpace := TmpDirMax - (BytesDumped - BytesCopied)

	log.Info(fmt.Sprintf("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", TotalFiles, FilesDumpCompleted, FilesCopyCompleted))
	log.Info(fmt.Sprintf("BytesDumped: %d, Copied (success): %d, TmpSize: %d", BytesDumped, BytesCopied, (BytesDumped - BytesCopied)))

	if freeSpace <= FileTargetSize {
		log.Warning(fmt.Sprintf("Low free space: %d bytes", freeSpace))
	}

}

func publishStatus() {

	for {
		status()
		time.Sleep(2 * time.Second)
	}

}

func preflightChecks(db *sql.DB) {

	log.SetLevel(log.InfoLevel)

	AwsS3Bucket = getenv("TIDUMP_AWS_S3_BUCKET", "backups.tocker.ca")
	AwsS3Region = getenv("TIDUMP_AWS_S3_REGION", "us-east-1")

	/*
	 These could be made configurable,
	 but it's not known if there is a strong
	 use case to do so.
	*/

	FileTargetSize = 100 * 1024 * 1024 // 100MiB, same as a region
	BulkInsertLimit = 16 * 1024 * 1024 // 16MiB, less than max_allowed_packet
	TmpDirMax = 5 * 1024 * 1024 * 1024 // 5GiB, assume small AMI local disk

	if TmpDirMax < FileTargetSize*40 {
		log.Warning("It is recommended to set a TmpDirMax 40x the size of FileTargetSize")
		log.Warning("The tmpdir could block on all incomplete files.")
	}

	db.Exec("SET group_concat_max_len = 1024 * 1024")
	var hostname string

	query := "SELECT @@hostname, NOW()"
	err := db.QueryRow(query).Scan(&hostname, &MySQLNow)
	log.Debug(query)
	check(err)

	AwsS3BucketPrefix = getenv("AWS_S3_BUCKET_PREFIX", fmt.Sprintf("tidump-%s/%s", hostname, StartTime.Format("2006-01-02")))
	log.Info(fmt.Sprintf("Uploading to %s/%s", AwsS3Bucket, AwsS3BucketPrefix))

	/*
	 Make a directory to write temporary dump files.
	 it will fill up to TmpDirMax (5GiB)
	*/

	TmpDir, err = ioutil.TempDir("", "tidump")
	log.Info(fmt.Sprintf("Writing temporary files to: %s", TmpDir))

	if err != nil {
		log.Fatal(fmt.Sprintf("Could not create tempdir: %s", err))
	}

}

func setTiDBSnapshot(db *sql.DB) {

	query := fmt.Sprintf("SET tidb_snapshot = '%s'", MySQLNow)
	time.Sleep(time.Second) // finish this second first
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

	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s.%s LIMIT 1", schema, table)
	_, err := db.Query(query)
	log.Debug(query)

	if err != nil {
		columnName = likelyPrimaryKey
	} else {
		columnName = "_tidb_rowid"
	}

	return

}

func discoverTableMinMax(db *sql.DB, schema string, table string, primaryKey string) (min int, max int) {

	query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", primaryKey, primaryKey, schema, table)
	err := db.QueryRow(query).Scan(&min, &max)
	log.Debug(query)
	check(err)

	return

}

func discoverRowsPerFile(avgRowLength int, fileTargetSize int64) int {
	return int(math.Abs(math.Floor(float64(fileTargetSize) / float64(avgRowLength))))
}

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

func dumpCreateTable(db *sql.DB, schema string, table string) {

	defer SchemaDumpWg.Done()

	atomic.AddInt64(&TotalFiles, 1)

	var fakeTable string
	var createTable string

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	file := fmt.Sprintf("%s/%s.%s-schema.sql", TmpDir, schema, table)

	err := db.QueryRow(query).Scan(&fakeTable, &createTable)
	log.Debug(query)
	check(err)

	createTable = fmt.Sprintf("%s;\n", createTable)

	if canSafelyWriteToTmpdir(int64(len(createTable))) {

		f, err := os.Create(file)
		log.Debug(fmt.Sprintf("Creating file %s", file))
		check(err)

		n, err := f.WriteString(createTable)
		check(err)

		atomic.AddInt64(&BytesDumped, int64(n))

		f.Close()
		SchemaCopyWg.Add(1)
		atomic.AddInt64(&FilesDumpCompleted, 1)
		go copyFileToS3(file, "schema")

	}

}

/*
 I am waiting for the server to support SHOW CREATE USER,
 so semantically this can be:
 SELECT user,host FROM mysql.user;
 SHOW CREATE USER user.host;
 SHOW GRANTS FOR user.host;

 Support SHOW CREATE USER as in MySQL 5.7
 https://github.com/pingcap/tidb/issues/7733
*/

func dumpPermissions(db *sql.DB) bool {

	return true

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

/*
 Check to see it's safe to write nBytes to
 the tmpdir and not exceed TmpDirMax.
 This is not thread-safe, so it's possible size
 could be exceeded.
*/

func canSafelyWriteToTmpdir(nBytes int64) bool {

	for {

		freeSpace := TmpDirMax - (BytesDumped - BytesCopied)

		if nBytes > freeSpace {
			runtime.Gosched()           // Give prority to other gorountines, this ones blocked.
			time.Sleep(5 * time.Second) // Waiting on S3 copy.
			// the status thread will warn no free space.
			continue
		} else {
			log.Debug(fmt.Sprintf("Free Space: %d, Requested: %d", freeSpace, nBytes))
			break
		}

	}

	return true

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

	os.Remove(filename) // Still open, it will free space on close

	atomic.AddInt64(&FilesCopyCompleted, 1)
	fi, _ := file.Stat()
	atomic.AddInt64(&BytesCopied, fi.Size())

	if copyType != "schema" {
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
