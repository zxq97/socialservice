package conf

import (
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/go-redis/redis"
	"github.com/jinzhu/gorm"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"time"
	"upper.io/db.v3/lib/sqlbuilder"
	"upper.io/db.v3/mysql"
)

const (
	ApiConfPath     = "/home/work/zzlove/conf/zzlove/api.yaml"
	ArticleConfPath = "/home/work/zzlove/conf/zzlove/article.yaml"
	ASyncConfPath   = "/home/work/zzlove/conf/zzlove/async.yaml"
	CommentConfPath = "/home/work/zzlove/conf/zzlove/comment.yaml"
	OnlineConfPath  = "/home/work/zzlove/conf/zzlove/online.yaml"
	RemindConfPath  = "/home/work/zzlove/conf/zzlove/remind.yaml"
	SocialConfPath  = "/home/work/zzlove/conf/zzlove/social.yaml"
	UserConfPath    = "/home/work/zzlove/conf/zzlove/user.yaml"

	MysqlAddr = "%v:%v@tcp(%v:%v)/%v?charset=utf8mb4&parseTime=True"
)

type MysqlConf struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DB       string `yaml:"db"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

type RedisConf struct {
	Addr string `yaml:"addr"`
	DB   int    `yaml:"db"`
}

type RedisClusterConf struct {
	Addr []string `yaml:"addr"`
}

type MCConf struct {
	Addr []string `yaml:"addr"`
}

type GrpcConf struct {
	Addr string `yaml:"addr"`
	Name string `yaml:"name"`
}

type EtcdConf struct {
	Addr []string `yaml:"addr"`
}

type KafkaConf struct {
	Addr []string `yaml:"addr"`
}

type Conf struct {
	Mysql        MysqlConf        `yaml:"mysql"`
	Slave        MysqlConf        `yaml:"slave"`
	RedisCluster RedisClusterConf `yaml:"cluster"`
	MC           MCConf           `yaml:"mc"`
	Grpc         GrpcConf         `yaml:"grpc"`
	Etcd         EtcdConf         `yaml:"etcd"`
	Kafka        KafkaConf        `yaml:"kafka"`
}

func LoadYaml(path string) (*Conf, error) {
	conf := new(Conf)
	y, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(y, conf)
	return conf, err
}

func GetMC(addr []string) *memcache.Client {
	return memcache.New(addr...)
}

func GetRedisCluster(addr []string) redis.Cmdable {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: addr,
	})
}

func GetMysql(addr string) (sqlbuilder.Database, error) {
	dsn, err := mysql.ParseURL(addr)
	if err != nil {
		return nil, err
	}
	r, err := mysql.Open(dsn)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func GetGorm(addr string) (*gorm.DB, error) {
	db, err := gorm.Open("mysql", addr)
	if err != nil {
		return nil, err
	}
	db.DB().SetConnMaxLifetime(time.Minute * 3)
	db.DB().SetMaxIdleConns(10)
	db.DB().SetMaxOpenConns(50)
	return db, nil
}
