package main

import (
	"fmt"
	"math"
	"os"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/zap"
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

func (d *dumper) newDumpTable() *dumpTable {
	return &dumpTable{
		d: d,
	}
}

func (dt *dumpTable) dump() {

	dt.discoverPrimaryKey()
	dt.discoverRowsPerFile()
	dt.discoverTableMinMax()
	dt.dumpCreateTable()  // async dump create table
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
	rows, err := tx.Query(query)

	if err != nil {
		dt.primaryKey = dt.likelyPrimaryKey
	} else {
		dt.primaryKey = "_tidb_rowid"
		rows.Close()
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
		dt.min = 0 // Table likely has
		dt.max = 0 // zero rows
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

func (dt *dumpTable) dumpCreateTable() error {

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", dt.schema, dt.table)
	dt.schemaFile = fmt.Sprintf("%s/%s.%s-schema.sql", dt.d.cfg.TmpDir, dt.schema, dt.table)

	var fake string
	tx := dt.d.newTx()
	err := tx.QueryRow(query).Scan(&fake, &dt.createTable)
	tx.Commit()

	if err != nil {
		zap.S().Fatalf("Could not SHOW CREATE TABLE for %s.%s: %s", dt.schema, dt.table, err)
	}

	dt.createTable = fmt.Sprintf("%s;\n", dt.createTable)

	if err := dt.d.canSafelyWriteToTmpdir(int64(len(dt.createTable))); err != nil {
		return err
	}

	f, err := os.Create(dt.schemaFile)
	defer f.Close()

	if err != nil {
		zap.S().Fatalf("Could not create temporary file: %s", dt.schemaFile)
		return err
	}
	if n, err := f.WriteString(dt.createTable); err != nil {
		zap.S().Warnf("Could not write %d bytes to temporary file: %s", n, dt.schemaFile)
		return err
	} else {
		atomic.AddInt64(&dt.d.bytesDumped, int64(n))
		atomic.AddInt64(&dt.d.bytesWritten, int64(n)) // it was uncompresssed

		if err := dt.d.doCopyFileToS3(dt.schemaFile); err != nil {
			return err
		}
		return nil
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
		df, _ := NewDumpFileSummary(dt, 0, 0) // small table
		dt.d.dumpFileQueue = append(dt.d.dumpFileQueue, df)
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

			df, _ := NewDumpFileSummary(dt, start, end)
			dt.d.dumpFileQueue = append(dt.d.dumpFileQueue, df)
		}
	}
}
