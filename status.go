package main

import (
	"github.com/ngaut/log"
	"time"
)

func status() {

	freeSpace := TmpDirMax - (BytesDumped - BytesCopied)

	log.Infof("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", TotalFiles, FilesDumpCompleted, FilesCopyCompleted)
	log.Infof("BytesDumped: %d, Copied (success): %d, TmpSize: %d", BytesDumped, BytesCopied, (BytesDumped - BytesCopied))

	if freeSpace <= FileTargetSize {
		log.Warningf("Low free space: %d bytes", freeSpace)
	}

}

func publishStatus() {

	for {
		status()
		time.Sleep(5 * time.Second)
	}

}
