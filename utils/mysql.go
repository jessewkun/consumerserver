package utils

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type MysqlConfig struct {
	Host     string `yaml:"host"`
	Network  string `yaml:"network"`
	Db       string `yaml:"db"`
	User     string `yaml:"user"`
	Port     int    `yaml:"port"`
	Password string `yaml:"password"`
	Table    string `yaml:"table"`
}

func NewMysql(mc MysqlConfig) *sql.DB {
	dsn := fmt.Sprintf("%s:%s@%s(%s:%d)/%s", mc.User, mc.Password, mc.Network, mc.Host, mc.Port, mc.Db)
	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		Log.Error(`init mysql error ` + err.Error())
		panic(err)
	}
	logstr := fmt.Sprintf("NewMysql %s@%s:%s", mc.User, mc.Host, mc.Db)
	Log.Info(logstr)
	return conn
}
