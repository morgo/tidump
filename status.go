package main

import (
	"github.com/ngaut/log"
	"time"
)

func (d *Dumper) status() {

	freeSpace := d.cfg.TmpDirMax - (d.BytesDumped - d.BytesCopied)

	log.Infof("TotalFiles: %d, FilesDumpCompleted: %d, FilesCopyCompleted: %d", d.TotalFiles, d.FilesDumpCompleted, d.FilesCopyCompleted)
	log.Infof("BytesDumped: %d, Copied (success): %d, TmpSize: %d", d.BytesDumped, d.BytesCopied, (d.BytesDumped - d.BytesCopied))

	if freeSpace <= d.cfg.FileTargetSize {
		log.Warningf("Low free space: %d bytes", freeSpace)
	}

}

func (d *Dumper) publishStatus() {

	for {
		d.status()
		time.Sleep(5 * time.Second)
	}

}
