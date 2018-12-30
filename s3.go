package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"

	"go.uber.org/zap"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

/*
 The s3 library already uses goroutines to parallelize the copy.
 Lets reduce to single threaded to not thread thrash,
 and make sure progress is made in whole units.
*/

func (d *dumper) copyFileToS3(filename string) {

	atomic.AddInt64(&d.filesDumpCompleted, 1) // creating the file finished
	d.s3Wg.Add(1)
	go d.doCopyFileToS3(filename, true)

}

func (d *dumper) s3isWritable() bool {

	filename := fmt.Sprintf("%s/metadata.json", d.cfg.TmpDir)
	f, err := os.Create(filename)
	defer f.Close()

	if err != nil {
		zap.S().Fatalf("Could not create temporary file: %s", err)
	}

	if n, err := f.WriteString("{}"); err != nil {
		zap.S().Fatalf("Could not write %d bytes to temporary file: %s", n, filename)
	}

	d.doCopyFileToS3(filename, false)
	return true

}

func (d *dumper) doCopyFileToS3(filename string, counts bool) {

	file, err := os.Open(filename)
	if err != nil {
		zap.S().Fatalf("Could not open file for upload: %s", filename)
	}

	defer file.Close()
	defer os.Remove(filename)

	conf := aws.Config{Region: aws.String(d.cfg.AwsS3Region)}
	sess := session.New(&conf)
	svc := s3manager.NewUploader(sess)

	zap.S().Debugf("Uploading file to S3: %s", filename)

	/*
	 Reduce concurrent uploads so that *some* files make it completely.
	 This makes resume more viable as a feature.
	 Note that the AWS S3 Library does use goroutines itself, and
	 will add some level of concurrency below this.
	*/

	d.s3Semaphore <- struct{}{}

	result, err := svc.Upload(&s3manager.UploadInput{
		Bucket: aws.String(d.cfg.AwsS3Bucket),
		Key:    aws.String(fmt.Sprintf("%s/%s", d.cfg.AwsS3BucketPrefix, filepath.Base(filename))),
		Body:   file,
	})

	<-d.s3Semaphore // Unlock

	if err != nil {
		zap.S().Warn(err)
		zap.S().Fatal(`This program does not accept credentials for AWS resources.
If you are using on EC2, please assign a role to the instance with S3 permissions.  If you are not on EC2, install the aws cli tools and run 'aws configure'.`)
	}

	zap.S().Debugf("Successfully uploaded %s to %s", filename, result.Location)

	if counts {
		atomic.AddInt64(&d.filesCopyCompleted, 1)
		fi, _ := file.Stat()
		atomic.AddInt64(&d.bytesCopied, fi.Size())
		d.s3Wg.Done()
	}

}
