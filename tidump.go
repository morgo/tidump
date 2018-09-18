package main

import (
	"database/sql"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

/*
 TODO:
 * Switch to ngaut/log
 * Change the mysql connection to be a pool (currently workers use their own connection, and goroutines could overload source.)
 * Design structs to hold backup
 * Fix races

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

	preflightChecks(db)
	setTiDBSnapshot(db)

	go publishStatus() // every 2 seconds

	query := findAllTables()
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

	cleanupTmpDir()
	status() // print status before exiting

	t := time.Now()
	elapsed := t.Sub(StartTime)

	log.WithFields(log.Fields{
		"StartTime":  StartTime,
		"FinishTime": t,
		"elapsed":    elapsed,
	}).Info("Complete")

}
