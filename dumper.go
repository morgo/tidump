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

type Dumper struct {
	BytesDumped        int64
	BytesCopied        int64
	FilesDumpCompleted int64
	FilesCopyCompleted int64
	TotalFiles         int64
	MySQLNow           string // not configurable yet.
	hostname           string
	TableDumpWg        *sync.WaitGroup
	TableCopyWg        *sync.WaitGroup
	SchemaCopyWg       *sync.WaitGroup
	SchemaDumpWg       *sync.WaitGroup
	cfg                *Config
	db                 *sql.DB // master sql connection
}

func NewDumper(cfg *Config) (*Dumper, error) {

	db, err := sql.Open("mysql", cfg.MySQLConnection)
	if err != nil {
		log.Fatal("Could not connect to MySQL at %s.", cfg.MySQLConnection)
	}

	dumper := &Dumper{
		cfg:          cfg,
		TableDumpWg:  new(sync.WaitGroup),
		TableCopyWg:  new(sync.WaitGroup),
		SchemaCopyWg: new(sync.WaitGroup),
		SchemaDumpWg: new(sync.WaitGroup),
		db:           db,
	}

	return dumper, nil

}

func (d *Dumper) dump() {

	d.preflightChecks()
	d.setTiDBSnapshot(d.db)

	go d.publishStatus() // every few seconds

	d.dumpUsers() // currently does nothing

	query := d.findAllTables(d.cfg.MySQLRegex)
	tables, err := d.db.Query(query)
	log.Debug(query)

	if err != nil {
		log.Fatal("Check MySQL connection is configured correctly.")
	}

	for tables.Next() {

		dt, _ := d.NewDumpTable()

		err = tables.Scan(&dt.schema, &dt.table, &dt.avgRowLength, &dt.dataLength, &dt.likelyPrimaryKey, &dt.insertableColumns)
		if err != nil {
			log.Fatal("Check MySQL connection is configured correctly.")
		}

		dt.dump()

	}

	/*
	 The work is handled in goroutines.
	 The dump routines write to the tmpdir, and then
	 trigger a goroutine for copying to S3.
	*/

	d.TableDumpWg.Wait()
	d.TableCopyWg.Wait()
	d.SchemaDumpWg.Wait()
	d.SchemaCopyWg.Wait()

	d.cleanupTmpDir()
	d.status() // print status before exiting

	return

}

func (d *Dumper) status() {

	freeSpace := d.cfg.TmpDirMax - (d.BytesDumped - d.BytesCopied)

	log.Infof("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", d.TotalFiles, d.FilesDumpCompleted, d.FilesCopyCompleted)
	log.Infof("BytesDumped: %d, Copied (success): %d, TmpSize: %d", d.BytesDumped, d.BytesCopied, (d.BytesDumped - d.BytesCopied))

	if freeSpace <= d.cfg.FileTargetSize {
		log.Warningf("Low free space: %d bytes", freeSpace)
	}

}

func (d *Dumper) publishStatus() {

	for {
		d.status()
		time.Sleep(5 * time.Second)
	}

}

// Some of this could be moved to config.

func (d *Dumper) preflightChecks() {

	d.db.Exec("SET group_concat_max_len = 1024 * 1024")

	time.Sleep(time.Second) // finish this second first
	query := "SELECT @@hostname, NOW()-INTERVAL 1 SECOND"
	err := d.db.QueryRow(query).Scan(&d.hostname, &d.MySQLNow)
	log.Debug(query)

	if err != nil {
		log.Fatalf("Could not get server time for tidb_snapshot: %s", err)
	}

	// hack
	d.cfg.AwsS3BucketPrefix = fmt.Sprintf("tidump-%s/%s", d.hostname, StartTime.Format("2006-01-02"))
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

func (d *Dumper) dumpUsers() bool {
	return true
}

/*
 This is for worker threads
*/

func (d *Dumper) newDbConnection() *sql.DB {

	db, err := sql.Open("mysql", d.cfg.MySQLConnection)

	if err != nil {
		log.Fatalf("Could not create new connection to MySQL: %s", err)
	}

	d.setTiDBSnapshot(db) // set all threads to same place
	return db

}

/*
 Set the tidb_snapshot before doing anything else.
 In future this might be configurable.
*/

func (d *Dumper) setTiDBSnapshot(db *sql.DB) {

	query := fmt.Sprintf("SET tidb_snapshot = '%s', tidb_force_priority = 'low_priority'", d.MySQLNow)
	_, err := db.Exec(query)
	log.Debug(query)

	if err != nil {
		log.Fatalf("Could not set tidb_snapshot: %s", err)
	}

}

/*
 This query can be improved by adding more meta data to the server:
 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (d *Dumper) findAllTables(regex string) (sql string) {

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

func (d *Dumper) canSafelyWriteToTmpdir(nBytes int64) bool {

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

func (d *Dumper) cleanupTmpDir() {
	os.RemoveAll(d.cfg.TmpDir) // delete temporary directory
}
