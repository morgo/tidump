package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

type dumper struct {
	BytesDumped        int64
	BytesCopied        int64
	FilesDumpCompleted int64
	FilesCopyCompleted int64
	TotalFiles         int64
	MySQLNow           string // not configurable yet.
	hostname           string
	TableDumpWg        *sync.WaitGroup
	SchemaDumpWg       *sync.WaitGroup
	s3Wg               *sync.WaitGroup
	cfg                *Config
	db                 *sql.DB // sql connection
	s3Semaphore        chan struct{}
}

func NewDumper(cfg *Config) (*dumper, error) {

	db, err := sql.Open("mysql", cfg.MySQLConnection)
	if err != nil {
		log.Fatal("Could not connect to MySQL at %s.", cfg.MySQLConnection)
	}

	db.SetMaxOpenConns(cfg.MySQLPoolSize)

	dumper := &dumper{
		cfg:          cfg,
		TableDumpWg:  new(sync.WaitGroup),
		SchemaDumpWg: new(sync.WaitGroup),
		s3Wg:         new(sync.WaitGroup),
		s3Semaphore:  make(chan struct{}, cfg.AwsS3PoolSize),
		db:           db,
	}

	return dumper, nil

}

func (d *dumper) Dump() {

	d.preflightChecks()

	go d.publishStatus() // every few seconds

	d.dumpUsers() // currently does nothing

	tx := d.newTx()
	tx.Exec("SET group_concat_max_len = 1024 * 1024")

	query := d.findAllTables(d.cfg.MySQLRegex)
	tables, err := tx.Query(query)

	if err != nil {
		log.Fatalf("Check MySQL connection is configured correctly: %s", err)
	}

	for tables.Next() {

		dt, _ := d.newDumpTable()

		err = tables.Scan(&dt.schema, &dt.table, &dt.avgRowLength, &dt.dataLength, &dt.likelyPrimaryKey, &dt.insertableColumns)
		if err != nil {
			log.Fatal("Check MySQL connection is configured correctly.")
		}

		dt.dump()

	}

	tx.Commit() // return to pool.

	/*
	 The work is handled in goroutines.
	 The dump routines write to the tmpdir, and then
	 trigger a goroutine for copying to S3.
	*/

	d.TableDumpWg.Wait()
	d.SchemaDumpWg.Wait()
	d.s3Wg.Wait()

	d.cleanupTmpDir()
	d.db.Close()
	d.status() // print status before exiting

	return

}

func (d *dumper) status() {

	freeSpace := d.cfg.TmpDirMax - (d.BytesDumped - d.BytesCopied)

	log.Infof("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", d.TotalFiles, d.FilesDumpCompleted, d.FilesCopyCompleted)
	log.Infof("BytesDumped: %d, Copied (success): %d, TmpSize: %d", d.BytesDumped, d.BytesCopied, (d.BytesDumped - d.BytesCopied))
	log.Debugf("Goroutines in existence: %d", runtime.NumGoroutine())

	if freeSpace <= d.cfg.FileTargetSize {
		log.Warningf("Low free space: %d bytes", freeSpace)
	}

}

func (d *dumper) publishStatus() {

	for {
		d.status()
		time.Sleep(5 * time.Second)
	}

}

// Some of this could be moved to config.

func (d *dumper) preflightChecks() {

	time.Sleep(time.Second) // finish this second first
	query := "SELECT @@hostname, NOW()-INTERVAL 1 SECOND"

	tx := d.newTx()
	err := tx.QueryRow(query).Scan(&d.hostname, &d.MySQLNow)
	tx.Commit()

	if err != nil {
		log.Fatalf("Could not get server time for tidb_snapshot: %s", err)
	}

	// @TODO: don't use startime but tidb_snapshot time!
	d.cfg.AwsS3BucketPrefix = fmt.Sprintf("tidump-%s/%s", d.hostname, startTime.Format("2006-01-02"))
	log.Infof("Uploading to %s/%s", d.cfg.AwsS3Bucket, d.cfg.AwsS3BucketPrefix)

	/*
	 Make a directory to write temporary dump files.
	 it will fill up to TmpDirMax (5GiB)
	*/

	d.cfg.TmpDir, err = ioutil.TempDir("", "tidump")
	log.Infof("Writing temporary files to: %s", d.cfg.TmpDir)

	if err != nil {
		log.Fatalf("Could not create tempdir: %s", err)
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

func (d *dumper) dumpUsers() bool {
	return true
}

/*
 This makes sure they have the tidb_snapshot set.
 Note: without a transaction, go to not guarantee
 the set statement will apply to the next connection.
*/

func (d *dumper) newTx() *sql.Tx {

	tx, err := d.db.Begin()

	if err != nil {
		log.Fatalf("Could not begin new transaction: %s", err)
	}

	query := fmt.Sprintf("SET tidb_snapshot = '%s', tidb_force_priority = 'low_priority'", d.MySQLNow)
	_, err = tx.Exec(query)

	if err != nil {
		log.Fatalf("Could not set tidb_snapshot: %s", err)
	}

	return tx

}

/*
 This query can be improved by adding more meta data to the server:
 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (d *dumper) findAllTables(regex string) (sql string) {

	sql = `SELECT
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

	if len(regex) > 0 {
		sql = fmt.Sprintf("%s AND concat(t.table_schema, '.', t.table_name) RLIKE '%s'", sql, regex)
	}

	return

}

/*
 Check to see it's safe to write nBytes to
 the tmpdir and not exceed TmpDirMax.
 This is not thread-safe, so it's possible size
 could be exceeded.
*/

func (d *dumper) canSafelyWriteToTmpdir(nBytes int64) bool {

	for {

		freeSpace := d.cfg.TmpDirMax - (d.BytesDumped - d.BytesCopied)

		if nBytes > freeSpace {
			runtime.Gosched()           // Give prority to other gorountines, this ones blocked.
			time.Sleep(5 * time.Second) // Waiting on S3 copy.
			continue                    // the status thread will warn low/no free space.
		} else {
			log.Debugf("Free Space: %d, Requested: %d", freeSpace, nBytes)
			break
		}

	}

	return true

}

func (d *dumper) cleanupTmpDir() {
	os.RemoveAll(d.cfg.TmpDir) // delete temporary directory
}
