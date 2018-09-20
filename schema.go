package main

import (
	"fmt"
	"os"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

func (d *Dumper) prepareDumpSchema(schema string, table string) {

	go d.dumpCreateTable(schema, table)
	d.SchemaDumpWg.Add(1)

}

func (d *Dumper) dumpCreateTable(schema string, table string) {

	defer d.SchemaDumpWg.Done()

	db := d.newDbConnection()
	defer db.Close()

	atomic.AddInt64(&d.TotalFiles, 1)

	var fakeTable, createTable string

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	file := fmt.Sprintf("%s/%s.%s-schema.sql", d.cfg.TmpDir, schema, table)

	err := db.QueryRow(query).Scan(&fakeTable, &createTable)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not SHOW CREATE TABLE for %s.%s", schema, table)
	}

	createTable = fmt.Sprintf("%s;\n", createTable)

	if d.canSafelyWriteToTmpdir(int64(len(createTable))) {

		f, err := os.Create(file)
		log.Debugf("Creating file %s", file)

		if err != nil {
			log.Fatal("Could not create temporary file: %s", file)
		}

		n, err := f.WriteString(createTable)

		if err != nil {
			log.Fatal("Could not write %d bytes to temporary file: %s", n, file)
		}

		atomic.AddInt64(&d.BytesDumped, int64(n))

		f.Close()
		d.SchemaCopyWg.Add(1)
		atomic.AddInt64(&d.FilesDumpCompleted, 1)
		go d.copyFileToS3(file, "schema")

	}

}
