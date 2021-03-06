package client

import (
	"flag"
	"io/ioutil"
	"time"

	"gopkg.in/yaml.v2"
)

type zkconfig struct {
	Servers []string `yaml:"servers"`
	Timeout int      `yaml:"sessionTimeout"`
}
type clientConfig struct {
	Zk  zkconfig `yaml:"zk"`
	Ack int      `yaml:"ack"`
}

var c clientConfig

var zkServers []string = []string{
	"127.0.0.1:2181",
}
var zkSessionTimeout = time.Second * 5
var ack int64 = int64(0)

func initCfg(toRootPath string) {
	cfgPath := toRootPath + flag.Lookup("cfg").Value.String()
	initCfgWithDirectPath(cfgPath)
}

func initCfgWithDirectPath(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(content, &c)
	if err != nil {
		panic(err)
	}
	zkServers = c.Zk.Servers
	zkSessionTimeout = time.Millisecond * time.Duration(c.Zk.Timeout)
	if ack == 0 || ack == 1 || ack == -1 {
		ack = int64(c.Ack)
	}
}
