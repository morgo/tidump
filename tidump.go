package main

import (
	"database/sql"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	_ "github.com/go-sql-driver/mysql"
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

	cleanupTmpDir()
	status()             // print status before exiting

	t := time.Now()
	elapsed := t.Sub(StartTime)

	log.WithFields(log.Fields{
		"StartTime":  StartTime,
		"FinishTime": t,
		"elapsed":    elapsed,
	}).Info("Complete")

}



func Map(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}


func check(e error) {
	if e != nil {
		log.Fatal(e)
		panic(e)
	}
}
