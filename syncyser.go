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
		"clustercheckuser",
	}
	proxyAdmin        = "admin"
	defaultHostGroup  = 1
	defaultSyncPeriod = 5
)

var (
	Skip   int = 1
	Update int
	Insert int
)

type mysqlUser struct {
	User     string `gorm:"column:User" json:"user"`
	PassWord string `gorm:"column:authentication_string" json:"password"`
}

type Proxy struct {
	DB *gorm.DB `gorm:"-" json:"-"`
}
type proxyUser struct {
	UserName string `gorm:"column:username" json:"username"`
	PassWord string `gorm:"column:password" json:"password"`
}

func (p *Proxy) TableName() string {
	return "mysql_users"
}

func (p *Proxy) loadToRuntimeSaveToDisk() {
	p.DB.Raw("LOAD MYSQL USERS TO RUNTIME;")
	p.DB.Raw("SAVE MYSQL USERS TO DISK;")
}

func (p *Proxy) insertOrUpdate(pu proxyUser) {
	// err := p.DB.Table(p.TableName()).Where("username", pu.UserName).Update(pu).Error
	err := p.DB.Table(p.TableName()).Raw(`update myql_users set password="*06C0BF5B64ECE2F648B5F048A71903906BA08E5C" where username='testinsert';`).Error
	glog.Info(err)
}
func (p *Proxy) proxyUsers() ([]proxyUser, error) {
	var proxyUsers []proxyUser
	if err := p.DB.Table(p.TableName()).Select("username", "password").Where("password !=''").Not("username", []string{"root"}).Scan(&proxyUsers).Error; err != nil {
		glog.Error(err)
		return nil, err
	}
	return proxyUsers, nil
}

func mysqlServerUsers() (map[string]string, error) {
	c := mysql.Config{
		User:   "root",
		Passwd: os.Getenv("MYSQL_ROOT_PASSWORD"),
		Net:    "tcp",
		Addr: net.JoinHostPort(
			os.Getenv("DAASSERVICE"),
			"3306"),
		Loc:                  time.Now().Local().Location(),
		DBName:               "mysql",
		Params:               map[string]string{"charset": "utf8"},
		ParseTime:            true,
		AllowNativePasswords: true,
	}
	db, err := gorm.Open("mysql", c.FormatDSN())
	if err != nil {
		glog.Error(err)
		return nil, err
	}
	defer db.Close()
	var raw []mysqlUser
	if err := db.Table("user").Select("User,authentication_string").Where("authentication_string !=''").Not("User", ignoreUsers).Scan(&raw).Error; err != nil {
		glog.Error(err)
		return nil, err
	}
	needUpdate := make(map[string]string)
	for _, u := range raw {
		if strings.HasPrefix(u.User, "mysql_innodb_cluster") {
			continue
		}
		needUpdate[u.User] = u.PassWord
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
	db, err := gorm.Open("mysql", c.FormatDSN())
	if err != nil {
		panic(err)
	}
	return &Proxy{DB: db}

}
func main() {
	proxy := newPorxy()
	ticker := time.NewTicker(time.Second * time.Duration(defaultSyncPeriod))
	for _ = range ticker.C {

		mySQLUsers, err := mysqlServerUsers()
		if err != nil {
			glog.Error(err)
			continue
		}
		if len(mySQLUsers) == 0 {
			glog.Infof("users in %s should be updated is 0 ", os.Getenv("DAASSERVICE"))
			continue

		}
		// users, err := proxy.proxyUsers()
		// if err != nil {
		// 	glog.Error(err)
		// 	continue
		// }
		// proxyUserMap := make(map[string]string)
		// for _, u := range users {
		// 	proxyUserMap[u.UserName] = u.PassWord
		// }

		for u, p := range mySQLUsers {
			glog.Infof("userName: %s,  password %s", u, p)
			proxy.insertOrUpdate(proxyUser{UserName: u, PassWord: p})
			// proxyUserMap[u] = p
			// if pp, exist := proxyUserMap[u]; exist {
			// 	if pp == mp {
			// 		continue
			// 	}
			// }
		}
		proxy.loadToRuntimeSaveToDisk()

	}

}
