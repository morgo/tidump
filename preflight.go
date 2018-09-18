package main

import (
	"database/sql"
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

func preflightChecks(db *sql.DB) {

	//	log.SetLevel(log.InfoLevel)

	AwsS3Bucket = getenv("TIDUMP_AWS_S3_BUCKET", "backups.tocker.ca")
	AwsS3Region = getenv("TIDUMP_AWS_S3_REGION", "us-east-1")
	MySQLRegex = getenv("TIDUMP_MYSQL_REGEX", "")

	/*
	 These could be made configurable,
	 but it's not known if there is a strong
	 use case to do so.
	*/

	FileTargetSize = 100 * 1024 * 1024 // 100MiB, same as a region
	BulkInsertLimit = 16 * 1024 * 1024 // 16MiB, less than max_allowed_packet
	TmpDirMax = 5 * 1024 * 1024 * 1024 // 5GiB, assume small AMI local disk

	if TmpDirMax < FileTargetSize*40 {
		log.Warning("It is recommended to set a TmpDirMax 40x the size of FileTargetSize")
		log.Warning("The tmpdir could block on all incomplete files.")
	}

	db.Exec("SET group_concat_max_len = 1024 * 1024")
	var hostname string

	time.Sleep(time.Second) // finish this second first
	query := "SELECT @@hostname, NOW()-INTERVAL 1 SECOND"
	err := db.QueryRow(query).Scan(&hostname, &MySQLNow)
	log.Debug(query)

	if err != nil {
		log.Fatal("Could not get server time for tidb_snapsot")
	}

	AwsS3BucketPrefix = getenv("AWS_S3_BUCKET_PREFIX", fmt.Sprintf("tidump-%s/%s", hostname, StartTime.Format("2006-01-02")))
	log.Infof("Uploading to %s/%s", AwsS3Bucket, AwsS3BucketPrefix)

	/*
	 Make a directory to write temporary dump files.
	 it will fill up to TmpDirMax (5GiB)
	*/

	TmpDir, err = ioutil.TempDir("", "tidump")
	log.Infof("Writing temporary files to: %s", TmpDir)

	if err != nil {
		log.Fatalf("Could not create tempdir: %s", err)
	}

}
