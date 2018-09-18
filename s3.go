package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func copyFileToS3(filename string, copyType string) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(fmt.Sprintf("Could not open file for upload", filename))
	}
	defer file.Close()

	conf := aws.Config{Region: aws.String(AwsS3Region)}
	sess := session.New(&conf)
	svc := s3manager.NewUploader(sess)

	log.Debug("Uploading file to S3...")

	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(AwsS3Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", AwsS3BucketPrefix, filepath.Base(filename))),
		Body:   file,
	})
	if err != nil {
		fmt.Println("error", err)
		os.Exit(1)
	}

	log.Debug(fmt.Sprintf("Successfully uploaded %s to %s\n", filename, result.Location))

	os.Remove(filename) // Still open, it will free space on close

	atomic.AddInt64(&FilesCopyCompleted, 1)
	fi, _ := file.Stat()
	atomic.AddInt64(&BytesCopied, fi.Size())

	if copyType != "schema" {
		TableCopyWg.Done()
	} else {
		SchemaCopyWg.Done()
	}

}
