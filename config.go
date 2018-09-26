package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/pingcap/errors"
	"github.com/ngaut/log"
)

func NewConfig() *Config {

	cfg := &Config{}
	cfg.FlagSet = flag.NewFlagSet("tidump", flag.ContinueOnError)
	fs := cfg.FlagSet

	fs.StringVar(&cfg.AwsS3Bucket, "s3-bucket", "", "Name of S3 bucket to upload backups to.")
	fs.StringVar(&cfg.AwsS3Region, "s3-region", "us-east-1", "S3 Region")
	fs.StringVar(&cfg.AwsS3BucketPrefix, "s3-bucket-prefix", "", "Prefix to use when uploading files.")
	fs.IntVar(&cfg.AwsS3PoolSize, "s3-pool-size", 4, "Number of s3 files to concurrently copy to S3.")

	fs.StringVar(&cfg.MySQLConnection, "mysql-connection", "root@tcp(localhost:4000)/", "A regular expression to filter which schemas and tables to include.")
	fs.StringVar(&cfg.MySQLRegex, "mysql-regex", ".*", "A regular expression to filter which schemas and tables to include.")
	fs.IntVar(&cfg.MySQLPoolSize, "mysql-pool-size", 4, "Number of connections to MySQL.")
	fs.StringVar(&cfg.TidbSnapshot, "tidb-snapshot", "", "Set the backup to a point in time.")

	fs.StringVar(&cfg.LogLevel, "L", "info", "Loader log level: debug, info, warn, error, fatal")
	//log file

	fs.Int64Var(&cfg.FileTargetSize, "file-target-size", (100 * 1024 * 1024), "Target size of files")
	fs.Int64Var(&cfg.BulkInsertLimit, "bulk-insert-limit", (16 * 1024 * 1024), "Bulk insert limit")
	fs.Int64Var(&cfg.TmpDirMax, "tmpdir-max", (5 * 1024 * 1024 * 1024), "Max size of tmpdir (goal)")

	fs.StringVar(&cfg.ConfigFile, "c", "", "config file")
	fs.BoolVar(&cfg.printVersion, "V", false, "prints version and exit")

	return cfg
}

type Config struct {
	*flag.FlagSet     `json:"-"`
	AwsS3Bucket       string `toml:"s3-bucket" json:"s3-bucket"`
	AwsS3Region       string `toml:"s3-region" json:"s3-region"`
	AwsS3BucketPrefix string `toml:"s3-bucket-prefix" json:"s3-bucket-prefix"`
	AwsS3PoolSize     int    `toml:"s3-pool-size" json:"s3-pool-size"`
	MySQLConnection   string `toml:"mysql-connection" json:"mysql-connection"`
	MySQLRegex        string `toml:"mysql-regex" json:"mysql-regex"`
	MySQLPoolSize     int    `toml:"mysql-pool-size" json:"mysql-pool-size"`
	TidbSnapshot      string `toml:"tidb-snapshot" json:"tidb-snapshot"`
	LogLevel          string `toml:"log-level" json:"log-level"`
	LogFile           string `toml:"log-file" json:"log-file"`
	TmpDir            string `toml:"tmpdir" json:"tmpdir"` // does nothing yet
	FileTargetSize    int64  `toml:"file-target-size" json:"file-target-size"`
	BulkInsertLimit   int64  `toml:"bulk-insert-limit" json:"bulk-insert-limit"`
	TmpDirMax         int64  `toml:"tmpdir-max" json:"tmpdir-max"`
	ConfigFile        string `json:"config-file"`
	printVersion      bool
}

func (c *Config) String() string {
	bytes, err := json.Marshal(c)
	if err != nil {
		log.Errorf("[loader] marshal config to json error %v", err)
	}
	return string(bytes)
}

// Parse parses flag definitions from the argument list.
func (c *Config) Parse(arguments []string) error {
	// Parse first to get config file.
	err := c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	/*
		if TmpDirMax < FileTargetSize*40 {
			log.Warning("It is recommended to set a TmpDirMax 40x the size of FileTargetSize")
			log.Warning("The tmpdir could block on all incomplete files.")
		}
	*/

	if c.printVersion {
		fmt.Println("Version 0.000000001")
		return flag.ErrHelp
	}

	// Load config file if specified.
	if c.ConfigFile != "" {
		err = c.configFromFile(c.ConfigFile)
		if err != nil {
			return errors.Trace(err)
		}
	}

	// Parse again to replace with command line options.
	err = c.FlagSet.Parse(arguments)
	if err != nil {
		return errors.Trace(err)
	}

	if len(c.FlagSet.Args()) != 0 {
		return errors.Errorf("'%s' is an invalid flag", c.FlagSet.Arg(0))
	}

	return nil
}

// configFromFile loads config from file.
func (c *Config) configFromFile(path string) error {
	_, err := toml.DecodeFile(path, c)
	return errors.Trace(err)
}
