package main

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"
)

/*
 I am waiting for the server to support SHOW CREATE USER,
 so semantically this can be:
 SELECT user,host FROM mysql.user;
 SHOW CREATE USER user.host;
 SHOW GRANTS FOR user.host;

 Support SHOW CREATE USER as in MySQL 5.7
 https://github.com/pingcap/tidb/issues/7733
*/

func dumpUsers(db *sql.DB) bool {

	return true

}
