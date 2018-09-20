package main

import (
	"fmt"
	"math"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
)

/*
 Hopefully this nonsense one day becomes obsolete.

 Create information_schema.TIDB_TABLE_PRIMARY_KEY
 https://github.com/pingcap/tidb/issues/7714
*/

func (d *Dumper) discoverPrimaryKey(schema string, table string, likelyPrimaryKey string) (columnName string) {

	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s.%s LIMIT 1", schema, table)
	_, err := d.db.Query(query)
	log.Debug(query)

	if err != nil {
		columnName = likelyPrimaryKey
	} else {
		columnName = "_tidb_rowid"
	}

	return

}

func (d *Dumper) discoverTableMinMax(schema string, table string, primaryKey string) (min int64, max int64) {

	db := d.newDbConnection() // won't be required soon
	defer db.Close()          // i_s should return min/max

	query := fmt.Sprintf("SELECT MIN(%s) as min, MAX(%s) max FROM `%s`.`%s`", primaryKey, primaryKey, schema, table)
	err := db.QueryRow(query).Scan(&min, &max)
	log.Debug(query)

	if err != nil {
		log.Fatalf("Could not determine min/max values for table: %s.%s", schema, table)
	}

	return

}

func (d *Dumper) discoverRowsPerFile(avgRowLength int, fileTargetSize int64) int64 {
	return int64(math.Abs(math.Floor(float64(fileTargetSize) / float64(avgRowLength))))
}
