package main

import (
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
)

// sync user info from mysqlserver, and insert new user into proxysql

var (
	ignoreUsers = []string{
		"mysql.session",
		"mysql.sys",
		"healthchecker",
		"root",
	}
	proxyAdmin        = "admin"
	defaultHostGroup  = 1
	defaultSyncPeriod = 5
)

type mysqlUser struct {
	User     string `gorm:"column:User" json:"user"`
	PassWord string `gorm:"column:authentication_string" json:"password"`
}

type Proxy struct {
	DB *gorm.DB `gorm:"-" json:"-"`
}
type proxyUser struct {
	UserName         string `gorm:"column:username" json:"username"`
	PassWord         string `gorm:"column:password" json:"password"`
	DefaultHostGroup int    `gorm:"column:default_hostgroup" json:"default_hostgroup"`
}

func (p *Proxy) proxyUsers() ([]proxyUser, error) {
	var proxyUsers []Proxy
	if err := p.DB.Table("mysql_users").Select("username", "password", "default_hostgroup").Where("password !=''").Scan(&proxyUsers).Error; err != nil {
		glog.Error(err)
		return nil, err
	}
	return proxyUsers, nil
}

func mysqlServerUsers() ([]mysqlUser, error) {
	serverName := os.Getenv("DAASSERVICE")
	rootPasswd := os.Getenv("MYSQL_ROOT_PASSWORD")
	c := mysql.Config{
		User:   "root",
		Passwd: os.Getenv("MYSQL_ROOT_PASSWORD"),
		Net:    "tcp",
		Addr: net.JoinHostPort(
			GetEnv("DAASSERVICE", "mysql"),
			"3306"),
		Loc:                  time.Now().Local().Location(),
		DBName:               "mysql",
		Params:               map[string]string{"charset": "utf8"},
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	db, err := gorm.Open("mysql", c.MySQLDSN())
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	defer db.Close()
	var raw []mysqlUser
	if err := db.Table("user").Select("User,authentication_string").Where("authentication_string !=''").Not("User", ignoreUsers).Scan(&users).Error; err != nil {
		glog.Error(err)
		return nil, err
	}
	needUpdate := make([]mysqlUser, 0)
	for _, u := range raw {
		if strings.HasPrefix(u.User, "mysql_innodb_cluster") {
			continue
		}
		needUpdate = append(needUpdate, u)
	}
	return needUpdate, nil
}

func newPorxy() *Proxy {
	c := mysql.Config{
		User:                 proxyAdmin,
		Passwd:               proxyAdmin,
		Net:                  "tcp",
		Addr:                 net.JoinHostPort("127.0.0.1", "6032"),
		Loc:                  time.Now().Local().Location(),
		DBName:               "mysql",
		Params:               map[string]string{"charset": "utf8"},
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	db, err := gorm.Open("mysql", c.MySQLDSN())
	if err != nil {
		panic(err)
	}
	return &Proxy{DB: db}

}
func main() {
	proxy := newPorxy()

}
