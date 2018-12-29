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
	"go.uber.org/zap"
)

type dumper struct {
	bytesDumped        int64 // uncompressed bytes dumped from TiDB
	bytesWritten       int64 // compressed bytes written (will be less)
	bytesCopied        int64 // actual bytes copied to S3
	filesDumpCompleted int64
	filesCopyCompleted int64
	totalFiles         int64
	dumpWg             *sync.WaitGroup
	s3Wg               *sync.WaitGroup
	s3Semaphore        chan struct{}
	cfg                *Config
	db                 *sql.DB // sql connection
}

func NewDumper(cfg *Config) (*dumper, error) {

	db, err := sql.Open("mysql", cfg.MySQLConnection)
	if err != nil {
		zap.S().Fatalf("Could not connect to MySQL at %s.", cfg.MySQLConnection)
	}

	db.SetMaxOpenConns(cfg.MySQLPoolSize)

	dumper := &dumper{
		cfg:         cfg,
		dumpWg:      new(sync.WaitGroup),
		s3Wg:        new(sync.WaitGroup),
		s3Semaphore: make(chan struct{}, cfg.AwsS3PoolSize),
		db:          db,
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
	rows, err := tx.Query(query)

	if err != nil {
		zap.S().Fatalf("Check MySQL connection is configured correctly: %s", err)
	}

	for rows.Next() {

		dt, _ := d.newDumpTable()

		err = rows.Scan(&dt.schema, &dt.table, &dt.avgRowLength, &dt.dataLength, &dt.likelyPrimaryKey, &dt.insertableColumns)
		if err != nil {
			zap.S().Fatal("Check MySQL connection is configured correctly.")
		}

		dt.dump()

	}

	rows.Close()
	tx.Commit() // return to pool.

	/*
	 The work is handled in goroutines.
	 The dump routines write to the tmpdir, and then
	 trigger a goroutine for copying to S3.
	*/

	d.dumpWg.Wait()
	d.s3Wg.Wait()

	d.cleanupTmpDir()
	d.db.Close()
	d.status() // print status before exiting
	return

}

func (d *dumper) status() {

	freeSpace := d.cfg.TmpDirMax - (d.bytesWritten - d.bytesCopied)

	zap.S().Infof("Dumped: %d/%d Copied %d/%d", d.filesDumpCompleted, d.totalFiles, d.filesCopyCompleted, d.totalFiles)
	zap.S().Infof("Bytes Dumped: %s, Bytes Written (gz): %s Copied to S3: %s", byteCountBinary(d.bytesDumped), byteCountBinary(d.bytesWritten), byteCountBinary(d.bytesCopied))
	zap.S().Infof("tmpsize: %s", byteCountBinary(d.bytesWritten-d.bytesCopied))
	zap.S().Debugf("Goroutines in existence: %d", runtime.NumGoroutine())

	if freeSpace <= d.cfg.FileTargetSize {
		zap.S().Warnf("Low free space: %d bytes", freeSpace)
	}

}

func (d *dumper) publishStatus() {

	for {
		d.status()
		time.Sleep(5 * time.Second)
	}

}

// Some of this could be moved to config.

func (d *dumper) preflightChecks() (err error) {

	if len(d.cfg.AwsS3Bucket) == 0 {
		zap.S().Fatal("Please specify an S3 bucket.  For example: tidump -s3-bucket backups.tocker.ca")
	}

	tx := d.newTx()
	defer tx.Commit()

	/* Auto create a tidb snapshot */

	if len(d.cfg.TidbSnapshot) == 0 {
		query := "SHOW MASTER STATUS"
		var file, dodb, ignoredb, gtid string
		if err = tx.QueryRow(query).Scan(&file, &d.cfg.TidbSnapshot, &dodb, &ignoredb, &gtid); err != nil {
			zap.S().Fatalf("Could not get server time for tidb_snapshot: %s", err)
		}
	}

	/* Auto create a S3 prefix */

	if len(d.cfg.AwsS3BucketPrefix) == 0 {

		var hostname, ts string

		query := "SELECT @@hostname"
		if err = tx.QueryRow(query).Scan(&hostname); err != nil {
			zap.S().Fatalf("Could not get server hostname: %s", err)
		}
		query = fmt.Sprintf("SELECT TIDB_PARSE_TSO(%s)", d.cfg.TidbSnapshot)
		if err = tx.QueryRow(query).Scan(&ts); err != nil {
			if t, err := time.Parse("2006-01-02 15:04:05", ts); err != nil {
				zap.S().Fatalf("Could not parse time: %s", err)
			} else {
				d.cfg.AwsS3BucketPrefix = fmt.Sprintf("tidump-%s/%s", hostname, t.Format("2006-01-02"))
				zap.S().Infof("Uploading to s3://%s/%s", d.cfg.AwsS3Bucket, d.cfg.AwsS3BucketPrefix)
			}
		}
	}

	/*
	 Make a directory to write temporary dump files.
	 it will fill up to TmpDirMax (5GiB)
	*/

	d.cfg.TmpDir, err = ioutil.TempDir("", "tidump")
	zap.S().Infof("Writing temporary files to: %s", d.cfg.TmpDir)

	if err != nil {
		zap.S().Fatalf("Could not create tempdir: %s", err)
	}

	if !d.s3isWritable() {
		zap.S().Fatal("Could not write to S3 Location: ", d.cfg.AwsS3Bucket)
	}

	return

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
	if tx, err := d.db.Begin(); err != nil {
		zap.S().Fatal("Could not begin new transaction: %s", err)
	} else {
		query := fmt.Sprintf("SET tidb_snapshot = '%s', tidb_force_priority = 'low_priority'", d.cfg.TidbSnapshot)
		if _, err = tx.Exec(query); err != nil {
			// skip temporarily: https://github.com/pingcap/tidb/issues/8887
			// zap.S().Fatalf("Could not set tidb_snapshot: %s", err)
		}
		return tx
	}
	return nil
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

		freeSpace := d.cfg.TmpDirMax - d.bytesWritten

		if nBytes > freeSpace {
			runtime.Gosched()           // Give prority to other gorountines, this ones blocked.
			time.Sleep(5 * time.Second) // Waiting on S3 copy.
			continue                    // the status thread will warn low/no free space.
		} else {
			zap.S().Debugf("Free Space: %d, Requested: %d", freeSpace, nBytes)
			break
		}

	}

	return true

}

func (d *dumper) cleanupTmpDir() {
	os.RemoveAll(d.cfg.TmpDir) // delete temporary directory
}
