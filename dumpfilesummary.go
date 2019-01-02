package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"os"

	"go.uber.org/zap"
)

type dumpFileSummary struct {
	sql    string
	file   string
	start  int64
	end    int64
	schema string
	table  string
}

/*
 DumpFileSummary is used to queue a file into the slice
 containing incomplete work.  It should not point to
 any file handles, as otherwise there can be a memory leak
*/

func NewDumpFileSummary(dt *dumpTable, start int64, end int64) (df *dumpFileSummary, err error) {

	df = &dumpFileSummary{
		start: start,
		end:   end,
	}

	startSql := "1=1"
	endSql := "1=1"

	if df.start != 0 {
		startSql = fmt.Sprintf("%s > %d", dt.primaryKey, df.start)
	}

	if df.end != 0 {
		endSql = fmt.Sprintf("%s < %d", dt.primaryKey, df.end)
	}

	df.sql = fmt.Sprintf("SELECT LOW_PRIORITY %s FROM `%s`.`%s` WHERE %s AND %s", dt.insertableColumns, dt.schema, dt.table, startSql, endSql)
	df.file = fmt.Sprintf("%s/%s.%s.%d.sql.gz", dt.d.cfg.TmpDir, dt.schema, dt.table, df.start)
	df.schema = dt.schema
	df.table = dt.table
	return

}

/*
 Convert and unqueue a dumpFileSummary back to a
 dumpFile and dump it.
*/

func (dfs dumpFileSummary) dump(d *dumper) (err error) {

	df := &dumpFile{
		start:  dfs.start,
		end:    dfs.end,
		sql:    dfs.sql,
		file:   dfs.file,
		d:      d,
		schema: dfs.schema,
		table:  dfs.table,
	}

	if df.fi, err = os.Create(df.file); err != nil {
		zap.S().Fatalf("Error in creating file: %s", df.file)
		return err
	}

	df.gf = gzip.NewWriter(df.fi)
	df.fw = bufio.NewWriter(df.gf)
	df.buffer = new(bytes.Buffer)
	df.zlen = new(int64)

	return df.dump()

}
