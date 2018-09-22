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
	d                 *dumper
	min               int64
	max               int64

	schemaFile  string // schema filename
	rowsPerFile int64
}

func (d *dumper) newDumpTable() (*dumpTable, error) {

	dt := &dumpTable{
		d: d,
	}

	return dt, nil

}

func (dt *dumpTable) dump() {

	dt.discoverPrimaryKey()
	dt.discoverRowsPerFile()
	dt.discoverTableMinMax()

	dt.d.dumpWg.Add(1)
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

	tx := dt.d.newTx()
	_, err := tx.Query(query)

	if err != nil {
		dt.primaryKey = dt.likelyPrimaryKey
	} else {
		dt.primaryKey = "_tidb_rowid"
	}

	tx.Commit()

	return

}

/*
 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (dt *dumpTable) discoverTableMinMax() {

	query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", dt.primaryKey, dt.primaryKey, dt.schema, dt.table)
	tx := dt.d.newTx()
	err := tx.QueryRow(query).Scan(&dt.min, &dt.max)
	tx.Commit()

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

	defer dt.d.dumpWg.Done()

	atomic.AddInt64(&dt.d.totalFiles, 1)

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dt.schema, dt.table)
	dt.schemaFile = fmt.Sprintf("%s/%s.%s-schema.sql", dt.d.cfg.TmpDir, dt.schema, dt.table)

	var fake string
	tx := dt.d.newTx()
	err := tx.QueryRow(query).Scan(&fake, &dt.createTable)
	tx.Commit()

	if err != nil {
		log.Fatal("Could not SHOW CREATE TABLE for %s.%s", dt.schema, dt.table)
	}

	dt.createTable = fmt.Sprintf("%s;\n", dt.createTable)

	if dt.d.canSafelyWriteToTmpdir(int64(len(dt.createTable))) {

		f, err := os.Create(dt.schemaFile)
		defer f.Close()

		if err != nil {
			log.Fatal("Could not create temporary file: %s", dt.schemaFile)
		}

		n, err := f.WriteString(dt.createTable)

		if err != nil {
			log.Fatal("Could not write %d bytes to temporary file: %s", n, dt.schemaFile)
		}

		atomic.AddInt64(&dt.d.bytesDumped, int64(n))
		atomic.AddInt64(&dt.d.bytesWritten, int64(n)) // it was uncompresssed
		dt.d.copyFileToS3(dt.schemaFile)

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
		df, _ := dt.NewDumpFile(0, 0) // small table
		go df.dump()
	} else {

		for i := dt.min; i < dt.max; i += dt.rowsPerFile {

			start := i
			end := i + dt.rowsPerFile - 1

			if i == dt.min {
				start = 0
			}

			if end > dt.max {
				end = 0
			}

			df, _ := dt.NewDumpFile(start, end)
			go df.dump()

		}
	}
}
