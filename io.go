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

/*
 Check to see it's safe to write nBytes to
 the tmpdir and not exceed TmpDirMax.
 This is not thread-safe, so it's possible size
 could be exceeded.
*/

func canSafelyWriteToTmpdir(nBytes int64) bool {

	for {

		freeSpace := TmpDirMax - (BytesDumped - BytesCopied)

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

func cleanupTmpDir() {
	os.RemoveAll(TmpDir) // delete temporary directory
}

type dumpFile struct {
	schema string
	table  string
	sql    string
	file   string
	fi     *os.File
	gf     *gzip.Writer
	fw     *bufio.Writer
	buffer *bytes.Buffer
}

func createDumpFile(schema string, table string, primaryKey string, insertableCols string, start int64, end int64) (d dumpFile) {

	d.schema = schema
	d.table = table

	startSql := "1=1"
	endSql := "1=1"

	if start != 0 {
		startSql = fmt.Sprintf("%s > %d", primaryKey, start)
	}

	if end != 0 {
		endSql = fmt.Sprintf("%s < %d", primaryKey, end)
	}

	d.sql = fmt.Sprintf("SELECT %s FROM `%s`.`%s` WHERE %s AND %s", insertableCols, schema, table, startSql, endSql)
	d.file = fmt.Sprintf("%s/%s.%s.%d.sql.gz", TmpDir, schema, table, start)

	var err error

	d.fi, err = os.Create(d.file)

	if err != nil {
		log.Fatalf("Error in creating file: %s", d.file)
	}

	d.gf = gzip.NewWriter(d.fi)
	d.fw = bufio.NewWriter(d.gf)
	d.buffer = new(bytes.Buffer)

	return

}

func (d dumpFile) close() {
	d.fw.Flush()
	// Close the gzip first.
	d.gf.Close()
	d.fi.Close()
}

func (d dumpFile) write(s string) (int, error) {

	// TODO: Get the zlib buffer len?
	return d.buffer.WriteString(s)
	// TODO: return zlib new length - old length?

}

func (d dumpFile) bufferLen() int {
	return d.buffer.Len()
}

func (d dumpFile) flush() {

	uncompressedLen := int64(d.bufferLen())

	if canSafelyWriteToTmpdir(uncompressedLen) {

		n, err := d.buffer.WriteTo(d.fw)
		atomic.AddInt64(&BytesDumped, n)

		if err != nil {
			log.Fatal("Could not write to gz file: %s", d.file)
		}

		d.buffer.Reset()
	}

}
