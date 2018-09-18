package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"time"
)

func status() {

	freeSpace := TmpDirMax - (BytesDumped - BytesCopied)

	log.Info(fmt.Sprintf("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", TotalFiles, FilesDumpCompleted, FilesCopyCompleted))
	log.Info(fmt.Sprintf("BytesDumped: %d, Copied (success): %d, TmpSize: %d", BytesDumped, BytesCopied, (BytesDumped - BytesCopied)))

	if freeSpace <= FileTargetSize {
		log.Warning(fmt.Sprintf("Low free space: %d bytes", freeSpace))
	}

}

func publishStatus() {

	for {
		status()
		time.Sleep(2 * time.Second)
	}

}
