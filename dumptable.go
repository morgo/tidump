package main

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	"math"
	"os"
	"sync/atomic"
)

type dumpTable struct {
	schema            string
	table             string
	createTable       string
	likelyPrimaryKey  string
	primaryKey        string
	insertableColumns string
	avgRowLength      int
	dataLength        int64
	d                 *Dumper
	min               int64
	max               int64

	schemaFile  string // schema filename
	rowsPerFile int64
}

func (d *Dumper) NewDumpTable() (*dumpTable, error) {

	dt := &dumpTable{
		d: d,
	}

	return dt, nil

}

func (dt *dumpTable) dump() {

	dt.discoverPrimaryKey()

	dt.d.SchemaDumpWg.Add(1)
	go dt.dumpCreateTable() // async dump create table

	dt.prepareDumpFiles() // fan-out and async dump files

}

/*
 Hopefully this nonsense one day becomes obsolete.

 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (dt *dumpTable) discoverPrimaryKey() {

	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s.%s LIMIT 1", dt.schema, dt.table)
	_, err := dt.d.db.Query(query)
	log.Debug(query)

	if err != nil {
		dt.primaryKey = dt.likelyPrimaryKey
	} else {
		dt.primaryKey = "_tidb_rowid"
	}

	return

}

/*
 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (dt *dumpTable) discoverTableMinMax() {

	query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", dt.primaryKey, dt.primaryKey, dt.schema, dt.table)
	err := dt.d.db.QueryRow(query).Scan(&dt.min, &dt.max)
	log.Debug(query)

	if err != nil {
		log.Fatalf("Could not determine min/max values for table: %s.%s", dt.schema, dt.table)
	}

	return

}

/*
 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (dt *dumpTable) discoverRowsPerFile() {
	dt.rowsPerFile = int64(math.Abs(math.Floor(float64(dt.d.cfg.FileTargetSize) / float64(dt.avgRowLength))))
	return
}

func (dt *dumpTable) dumpCreateTable() {

	defer dt.d.SchemaDumpWg.Done()

	atomic.AddInt64(&dt.d.TotalFiles, 1)

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dt.schema, dt.table)
	dt.schemaFile = fmt.Sprintf("%s/%s.%s-schema.sql", dt.d.cfg.TmpDir, dt.schema, dt.table)

	var fake string
	err := dt.d.db.QueryRow(query).Scan(&fake, &dt.createTable)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not SHOW CREATE TABLE for %s.%s", dt.schema, dt.table)
	}

	dt.createTable = fmt.Sprintf("%s;\n", dt.createTable)

	if dt.d.canSafelyWriteToTmpdir(int64(len(dt.createTable))) {

		f, err := os.Create(dt.schemaFile)
		log.Debugf("Creating file %s", dt.schemaFile)

		if err != nil {
			log.Fatal("Could not create temporary file: %s", dt.schemaFile)
		}

		n, err := f.WriteString(dt.createTable)

		if err != nil {
			log.Fatal("Could not write %d bytes to temporary file: %s", n, dt.schemaFile)
		}

		atomic.AddInt64(&dt.d.BytesDumped, int64(n))

		f.Close()
		dt.d.SchemaCopyWg.Add(1)
		atomic.AddInt64(&dt.d.FilesDumpCompleted, 1)
		go dt.d.copyFileToS3(dt.schemaFile, "schema")

	}

}

/*
 This function chunk-splits the table into files based on the dataLength
 and avgRowLength reported in information_schema.  In future, a region
 based strategy will be used, so this function will likely change
 quite a lot.
*/

func (dt *dumpTable) prepareDumpFiles() {

	if dt.dataLength < dt.d.cfg.FileTargetSize {
		dt.d.TableDumpWg.Add(1)
		df, _ := dt.NewDumpFile(0, 0) // small table
		go df.dump()
		atomic.AddInt64(&dt.d.TotalFiles, 1)
	} else {

		dt.discoverRowsPerFile()
		dt.discoverTableMinMax()

		for i := dt.min; i < dt.max; i += dt.rowsPerFile {

			start := i
			end := i + dt.rowsPerFile - 1

			if i == dt.min {
				start = 0
			}

			if end > dt.max {
				end = 0
			}

			log.Debugf("Table: %s.%s.  Start: %d End: %d\n", dt.schema, dt.table, start, end)
			dt.d.TableDumpWg.Add(1)
			df, _ := dt.NewDumpFile(start, end)
			go df.dump()
			atomic.AddInt64(&dt.d.TotalFiles, 1)

		}
	}
}
