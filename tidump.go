package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

/*
 TODO:
 * Add configuration system / help command.
 * There is a bug in counting local dump bytes.  It should be compressed size.
 * Fix races with a read write lock.
 * Change the mysql connection to be a pool (currently workers use their own connection, and goroutines could overload source.)

LIMITATIONS:
* Does not backup users.  Waiting on TIDB #7733.
* Not efficient at finding Primary Key.  Waiting on TiDB #7714.
* Does not do reads as low priority.  Prefer to use TiDB #7524
* Files may not be equal in size (may be fixed in TiDB #7714)
* Server does not expose version in easily parsable format (Need to File)
*/

var StartTime = time.Now()
var MySQLConnectionString, MySQLNow, MySQLRegex, AwsS3Bucket, AwsS3BucketPrefix, AwsS3Region, TmpDir string
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

	go publishStatus() // every few seconds

	dumpUsers(db) // currently does nothing

	query := findAllTables(MySQLRegex)
	tables, err := db.Query(query)
	log.Debug(query)

	if err != nil {
		log.Fatal("Check MYSQL_CONNECTION is configured correctly.")
	}

	for tables.Next() {

		var schema, table, likelyPrimaryKey, insertableColumns string
		var avgRowLength int
		var dataLength int64

		err = tables.Scan(&schema, &table, &avgRowLength, &dataLength, &likelyPrimaryKey, &insertableColumns)
		if err != nil {
			log.Fatal("Check MYSQL_CONNECTION is configured correctly.")
		}

		primaryKey := discoverPrimaryKey(db, schema, table, likelyPrimaryKey)

		if primaryKey == "_tidb_rowid" {
			insertableColumns = fmt.Sprintf("%s,%s", insertableColumns, primaryKey)
		}

		prepareDumpSchema(schema, table)
		prepareDumpTable(schema, table, avgRowLength, dataLength, primaryKey, insertableColumns)

	}

	/*
	 The work is handled in goroutines.
	 The dump routines write to the tmpdir, and then
	 trigger a goroutine for copying to S3.
	*/

	TableDumpWg.Wait()
	TableCopyWg.Wait()
	SchemaDumpWg.Wait()
	SchemaCopyWg.Wait()

	cleanupTmpDir()
	status() // print status before exiting

	t := time.Now()
	log.Infof("Completed in %s seconds.", t.Sub(StartTime))

}
