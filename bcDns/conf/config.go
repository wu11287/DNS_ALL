package conf

import (
	"github.com/spf13/viper"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Config struct {
	CAPath string
	CAPort int64

	//system info
	Port     string
	HostName string

	ProposalBufferSize int
	ProposalTimeout    time.Duration

	LeaderMsgBufferSize int
	PowDifficult        int
	Byzantine           bool
	Mode                string

	Test     bool
	Delay    time.Duration
	SetDelay bool

	ConfigInterval time.Duration
}

var (
	path        string
	BCDnsConfig Config
)

func init() {
	if val, ok := os.LookupEnv("BCDNSConfFile"); ok {
		path = val
	} else {
		log.Fatal("System's config is not set")
	}
	dir, file := filepath.Split(path)
	viper.SetConfigName(file)
	viper.AddConfigPath(dir)
	viper.SetConfigType("json")
	err := viper.ReadInConfig()
	if err != nil {
		//TODO
		log.Fatal("Read system config failed", err)
	}

	BCDnsConfig.Port = viper.GetString("PORT")
	BCDnsConfig.HostName = viper.GetString("HOSTNAME")
	BCDnsConfig.Byzantine = viper.GetBool("Byzantine")
	BCDnsConfig.Mode = viper.GetString("MODE")
	if strings.Compare("yes", viper.GetString("TEST")) == 0 {
		BCDnsConfig.Test = true
	} else {
		BCDnsConfig.Test = false
	}
	d := viper.GetInt("DELAY")
	if d == 0 {
		BCDnsConfig.SetDelay = false
	} else {
		BCDnsConfig.SetDelay = true
		BCDnsConfig.Delay = time.Duration(d) * time.Millisecond
	}
	BCDnsConfig.ProposalBufferSize = 10000
	BCDnsConfig.ProposalTimeout = 30 * time.Second
	c := viper.GetInt("CONFIGINTERVAL")
	BCDnsConfig.ConfigInterval = time.Duration(c) * time.Second
}
