package freetds

import (
	"strconv"
	"strings"
)

const (
	DBVERSION_UNKNOWN byte = iota
	DBVERSION_46
	DBVERSION_100
	// DBVERSION_42 TDS 4.2 Sybase and Microsoft
	//The version in use at the time of the Sybase/Microsoft split.
	DBVERSION_42
	// DBVERSION_70 TDS 7.0 Microsoft
	//Introduced for SQL Server 7.0. Includes support for the extended datatypes in SQL Server 7.0 (such as char/varchar fields of more than 255 characters). It also includes support for Unicode.
	DBVERSION_70
	// DBVERSION_71 TDS 7.1 Microsoft, was 8.0 [2]
	//Introduced for SQL Server 2000. Includes support for big integer (64-bit int) and “variant” datatypes.
	DBVERSION_71
	// DBVERSION_72 TDS 7.2 Microsoft, was 9.0
	//Introduced for SQL Server 2005. Includes support for varchar(max), varbinary(max), xml datatypes and MARS.
	DBVERSION_72
	// DBVERSION_73 TDS 7.3 Microsoft
	//Introduced for SQL Server 2008. Includes support for extended date/time, table as parameters.
	DBVERSION_73
	// DBVERSION_74 TDS 7.4 Microsoft
	//Introduced for SQL Server 2012. Includes support for session recovery.
	DBVERSION_74
)

//https://www.freetds.org/userguide/tdshistory.html
var tdsVersion = map[string]byte{

	"4.2":  DBVERSION_42,
	"5.0":  DBVERSION_46,
	"7.0":  DBVERSION_70,
	"7.1":  DBVERSION_71,
	"7.2":  DBVERSION_72,
	"7.3":  DBVERSION_73,
	"7.4":  DBVERSION_74,
	"auto": DBVERSION_UNKNOWN,
}

type credentials struct {
	user, pwd, host, database, mirrorHost, compatibility, appname, options string
	maxPoolSize, lockTimeout                                               int
	version                                                                byte
}

// NewCredentials fills credentials stusct from connection string
func NewCredentials(connStr string) *credentials {
	parts := strings.Split(connStr, ";")
	crd := &credentials{maxPoolSize: 100, appname: "gofreetds", version: DBVERSION_UNKNOWN}
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) == 2 {
			key := strings.ToLower(strings.Trim(kv[0], " "))
			value := kv[1]
			switch key {
			case "server", "host":
				crd.host = value
			case "database", "dbname":
				crd.database = value
			case "user id", "user_id", "user", "UID":
				crd.user = value
			case "password", "pwd":
				crd.pwd = value
			case "failover partner", "failover_partner", "mirror", "mirror_host", "mirror host":
				crd.mirrorHost = value
			case "max pool size", "max_pool_size":
				if i, err := strconv.Atoi(value); err == nil {
					crd.maxPoolSize = i
				}
			case "compatibility_mode", "compatibility mode", "compatibility":
				crd.compatibility = strings.ToLower(value)
			case "lock timeout", "lock_timeout":
				if i, err := strconv.Atoi(value); err == nil {
					crd.lockTimeout = i
				}
			case "app", "appname":
				crd.appname = value
			case "ver", "version":
				if v, ok := tdsVersion[value]; ok {
					crd.version = v
				}
			case "options":
				crd.options = "set " + strings.ReplaceAll(value, ",", "\nset ")
			}
		}
	}
	return crd
}
