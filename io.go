package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
	"runtime"
	"os"

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
			// the status thread will warn no free space.
			continue
		} else {
			log.Debug(fmt.Sprintf("Free Space: %d, Requested: %d", freeSpace, nBytes))
			break
		}

	}

	return true

}

func cleanupTmpDir() {
	os.RemoveAll(TmpDir) // delete temporary directory
}
