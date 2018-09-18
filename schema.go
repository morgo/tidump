package main

import (
	"database/sql"
	"fmt"
	"os"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

func dumpCreateTable(db *sql.DB, schema string, table string) {

	defer SchemaDumpWg.Done()

	atomic.AddInt64(&TotalFiles, 1)

	var fakeTable string
	var createTable string

	query := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", schema, table)
	file := fmt.Sprintf("%s/%s.%s-schema.sql", TmpDir, schema, table)

	err := db.QueryRow(query).Scan(&fakeTable, &createTable)
	log.Debug(query)
	check(err)

	createTable = fmt.Sprintf("%s;\n", createTable)

	if canSafelyWriteToTmpdir(int64(len(createTable))) {

		f, err := os.Create(file)
		log.Debug(fmt.Sprintf("Creating file %s", file))
		check(err)

		n, err := f.WriteString(createTable)
		check(err)

		atomic.AddInt64(&BytesDumped, int64(n))

		f.Close()
		SchemaCopyWg.Add(1)
		atomic.AddInt64(&FilesDumpCompleted, 1)
		go copyFileToS3(file, "schema")

	}

}
