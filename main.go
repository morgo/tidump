/*
 TODO:
 * There is a bug in counting local dump bytes.  It should be compressed size.
 * Fix races with a read write lock.
 * Change the mysql connection to be a pool (currently workers use their own connection, and goroutines could overload source.)
 * Change the S3 copy to be a pool of N threads.
 * Add a "resume" function.

LIMITATIONS:
* Does not backup users.  Waiting on TIDB #7733.
* Not efficient at finding Primary Key.  Waiting on TiDB #7714.
* Files may not be equal in size (may be fixed in TIDB #7714)
* Server does not expose version in easily parsable format (TIDB #7736)
*/

package main

import (
	"flag"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"os"
	"strings"
	"time"
)

// Structure
// Dumper -> DumpTable ->  DumpFile
// Dumper -> DumpTable

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

	d, _ := NewDumper(cfg)
	d.dump() // start main loop.

	t := time.Now()
	log.Infof("Completed in %s seconds.", t.Sub(StartTime))

}
