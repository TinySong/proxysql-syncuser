package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/golang/glog"
	"github.com/jinzhu/gorm"
	utilexec "k8s.io/utils/exec"
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

const defaultTimeout = 10 * time.Second

var (
	skip   int = 1
	update int
	insert int
)

type mysqlUser struct {
	User     string `gorm:"column:User" json:"user"`
	PassWord string `gorm:"column:authentication_string" json:"password"`
}

type Proxy struct {
	DB *gorm.DB
}
type proxyUser struct {
	UserName string `gorm:"column:username" json:"username"`
	PassWord string `gorm:"column:password" json:"password"`
}

func (p *Proxy) TableName() string {
	return "mysql_users"
}

func (p *Proxy) loadToRuntimeSaveToDisk() {
	if err := p.DB.Exec("LOAD MYSQL USERS TO RUNTIME;").Error; err != nil {
		glog.Error(err)
	}

	if err := p.DB.Exec("SAVE MYSQL USERS TO DISK;").Error; err != nil {
		glog.Error(err)
	}
}

func (p *Proxy) Update(pu proxyUser) {
	if err := p.DB.Exec(fmt.Sprintf(`update mysql_users set password='%s' where username='%s';`, pu.PassWord, pu.UserName)).Error; err != nil {
		glog.Error(err)
	}

}

func (p *Proxy) Insert(pu proxyUser) {
	sql := fmt.Sprintf(`Insert into mysql_users(username, password,default_hostgroup) VALUES('%s','%s',1);`, pu.UserName, pu.PassWord)
	err := p.DB.Exec(sql).Error
	if err != nil {
		glog.Error(err)
	}
}
func (p *Proxy) proxyUsers() ([]proxyUser, error) {
	var proxyUsers []proxyUser
	if err := p.DB.Raw(`select username,password from mysql_users where password !=''`).Scan(&proxyUsers).Error; err != nil {
		glog.Error(err)
		return nil, err
	}
	return proxyUsers, nil
}

func (p *Proxy) CheckAlive() {
	if p.DB.DB().Ping() != nil {
		p = newPorxy()
	}
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
		Loc:                  time.UTC,
		DBName:               "",
		Params:               map[string]string{"charset": "utf8"},
		ParseTime:            true,
		AllowNativePasswords: true,
	}

	ctx, _ := context.WithCancel(context.Background())
	for !isDatabaseRunning(ctx) {
		glog.Infof("proxysql not yet running. Waiting 2 seconds ...")
		time.Sleep(time.Second * time.Duration(2))
	}

	glog.Info("create proxysql client")
	db, err := gorm.Open("mysql", c.FormatDSN())
	if err != nil {
		panic(err)
	}
	return &Proxy{DB: db}
}

func main() {
	ticker := time.NewTicker(time.Second * time.Duration(defaultSyncPeriod))
	proxy := newPorxy()
	for _ = range ticker.C {
		mySQLUsers, err := mysqlServerUsers()
		if err != nil {
			glog.Error(err)
			continue
		}
		if len(mySQLUsers) == 0 {
			glog.Warningf("users in %s should be updated is 0 ", os.Getenv("DAASSERVICE"))
			continue

		}
		users, err := proxy.proxyUsers()
		if err != nil {
			glog.Error(err)
			continue
		}
		proxyUserMap := make(map[string]string)
		for _, u := range users {
			proxyUserMap[u.UserName] = u.PassWord
		}

		for u, p := range mySQLUsers {
			glog.V(3).Infof("mysqluser: %s", u)
			if pp, exists := proxyUserMap[u]; exists {
				if pp == p {
					continue
				}
				proxy.Update(proxyUser{UserName: u, PassWord: p})
			}
			proxy.Insert(proxyUser{UserName: u, PassWord: p})

		}
		proxy.loadToRuntimeSaveToDisk()

	}

}

func isDatabaseRunning(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, defaultTimeout)
	defer cancel()
	err := utilexec.New().CommandContext(ctx,
		"mysqladmin",
		"--protocol", "tcp",
		"-u", "root",
		os.ExpandEnv("-p$MYSQL_ROOT_PASSWORD"),
		"status",
	).Run()
	if err != nil {
		glog.Error(err)
	}
	glog.Info("isDatabaseRunning is alive")
	return err == nil
}
