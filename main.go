/*
 TODO:
 * Fix races with a read write lock.
 * Check S3 first if backup pre-exists.  If it does and was not complete,
   auto-resume by reading a "metadata.json" file.  Which contains:
   - the tidb-snapshot
   - min/max/primary key/rows-per-file of tables included in backup.


LIMITATIONS:
* Does not backup users.  Waiting on TIDB #7733.
* Not efficient at finding Primary Key.  Waiting on TiDB #7714.
* Files may not be equal in size (may be fixed in TIDB #7714)
* Server does not expose version in easily parsable format (TIDB #7736)
*/

package main

import (
	"flag"
	"os"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"
)

// Structure
// Dumper (d) -> DumpTable (dt) ->  DumpFile (df)

var startTime = time.Now()

func main() {
	log, _ := zap.NewDevelopmentConfig().Build()
	log.Sugar()
	zap.ReplaceGlobals(log)

	cfg := NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		zap.S().Error("parse cmd flags err %s\n", err)
		os.Exit(2)
	}

	level := zap.AtomicLevel{}
	level.UnmarshalText([]byte(cfg.LogLevel))

	d, _ := NewDumper(cfg)
	d.Dump() // start main loop.

	t := time.Now()
	zap.S().Info("Completed in %s seconds.", t.Sub(startTime))

}
