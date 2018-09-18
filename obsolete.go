package main

import (
	"database/sql"
	"fmt"
	"math"

	_ "github.com/go-sql-driver/mysql"
	log "github.com/sirupsen/logrus"
)

/*
 Hopefully this nonsense one day becomes obsolete.

 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func discoverPrimaryKey(db *sql.DB, schema string, table string, likelyPrimaryKey string) (columnName string) {

	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s.%s LIMIT 1", schema, table)
	_, err := db.Query(query)
	log.Debug(query)

	if err != nil {
		columnName = likelyPrimaryKey
	} else {
		columnName = "_tidb_rowid"
	}

	return

}

func discoverTableMinMax(db *sql.DB, schema string, table string, primaryKey string) (min int, max int) {

	query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", primaryKey, primaryKey, schema, table)
	err := db.QueryRow(query).Scan(&min, &max)
	log.Debug(query)
	check(err)

	return

}

func discoverRowsPerFile(avgRowLength int, fileTargetSize int64) int {
	return int(math.Abs(math.Floor(float64(fileTargetSize) / float64(avgRowLength))))
}
