package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/ngaut/log"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

type dumpFile struct {
	schema string
	table  string
	sql    string
	file   string
	fi     *os.File
	gf     *gzip.Writer
	fw     *bufio.Writer
	buffer *bytes.Buffer
	d      *Dumper
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

func (d *Dumper) createDumpFile(schema string, table string, primaryKey string, insertableCols string, start int64, end int64) (df dumpFile) {

	df.schema = schema
	df.table = table
	df.d = d // hack

	startSql := "1=1"
	endSql := "1=1"

	if start != 0 {
		startSql = fmt.Sprintf("%s > %d", primaryKey, start)
	}

	if end != 0 {
		endSql = fmt.Sprintf("%s < %d", primaryKey, end)
	}

	df.sql = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s AND %s", insertableCols, schema, table, startSql, endSql)
	df.file = fmt.Sprintf("%s/%s.%s.%d.sql.gz", d.cfg.TmpDir, schema, table, start)

	var err error

	df.fi, err = os.Create(df.file)

	if err != nil {
		log.Fatalf("Error in creating file: %s", df.file)
	}

	df.gf = gzip.NewWriter(df.fi)
	df.fw = bufio.NewWriter(df.gf)
	df.buffer = new(bytes.Buffer)

	return

}

func (df dumpFile) close() {
	df.fw.Flush()
	// Close the gzip first.
	df.gf.Close()
	df.fi.Close()
}

func (df dumpFile) write(s string) (int, error) {

	// TODO: Get the zlib buffer len?
	return df.buffer.WriteString(s)
	// TODO: return zlib new length - old length?

}

func (df dumpFile) bufferLen() int {
	return df.buffer.Len()
}

func (df dumpFile) flush() {

	uncompressedLen := int64(df.bufferLen())

	if df.d.canSafelyWriteToTmpdir(uncompressedLen) {

		n, err := df.buffer.WriteTo(df.fw)
		atomic.AddInt64(&df.d.BytesDumped, n)

		if err != nil {
			log.Fatal("Could not write to gz file: %s", df.file)
		}

		df.buffer.Reset()
	}

}
