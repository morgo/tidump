package main

import (
	"fmt"
	"github.com/ngaut/log"
	"io/ioutil"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func getenv(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func (d *Dumper) preflightChecks() {

	d.db.Exec("SET group_concat_max_len = 1024 * 1024")

	time.Sleep(time.Second) // finish this second first
	query := "SELECT @@hostname, NOW()-INTERVAL 1 SECOND"
	err := d.db.QueryRow(query).Scan(&d.hostname, &d.MySQLNow)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not get server time for tidb_snapsot")
	}

	// hack
	d.cfg.AwsS3BucketPrefix = fmt.Sprintf("tidump-%s/%s", d.hostname, StartTime.Format("2006-01-02"))
	log.Infof("Uploading to %s/%s", d.cfg.AwsS3Bucket, d.cfg.AwsS3BucketPrefix)

	/*
	 Make a directory to write temporary dump files.
	 it will fill up to TmpDirMax (5GiB)
	*/

	d.cfg.TmpDir, err = ioutil.TempDir("", "tidump")
	log.Infof("Writing temporary files to: %s", d.cfg.TmpDir)

	if err != nil {
		log.Fatalf("Could not create tempdir: %s", err)
	}

}
