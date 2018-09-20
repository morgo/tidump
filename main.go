package main

import (
	"flag"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"os"
	"strings"
	"time"
)

var StartTime = time.Now()

func main() {

	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	log.SetLevelByString(strings.ToLower(cfg.LogLevel))
	if len(cfg.LogFile) > 0 {
		log.SetOutputByName(cfg.LogFile)
		log.SetHighlighting(false)
	}

	d, _ := NewDumper(cfg) // start main loop.
	d.MainLoop()

	t := time.Now()
	log.Infof("Completed in %s seconds.", t.Sub(StartTime))

}
