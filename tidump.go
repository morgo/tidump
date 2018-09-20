package main

import (
	"database/sql"
	"fmt"
	"sync"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

/*
 TODO:
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

func (d *Dumper) MainLoop() {

	var err error

	d.preflightChecks()
	d.setTiDBSnapshot(d.db)

	go d.publishStatus() // every few seconds

	d.dumpUsers() // currently does nothing

	query := d.findAllTables(d.cfg.MySQLRegex)
	tables, err := d.db.Query(query)
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
			log.Fatal("Check MySQL connection is configured correctly.")
		}

		primaryKey := d.discoverPrimaryKey(schema, table, likelyPrimaryKey)

		if primaryKey == "_tidb_rowid" {
			insertableColumns = fmt.Sprintf("%s,%s", insertableColumns, primaryKey)
		}

		d.prepareDumpSchema(schema, table)
		d.prepareDumpTable(schema, table, avgRowLength, dataLength, primaryKey, insertableColumns)

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
