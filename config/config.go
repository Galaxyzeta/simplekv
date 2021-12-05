package config

import (
	"flag"
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v2"
)

var ConfigFileDir = ""

type config struct {
	BlockSize int64  `yaml:"blocksize"`
	DBDir     string `yaml:"dbdir"`
	Port      int    `yaml:"port"`
}

var c config

var BlockSize = int64(64)
var DBDir = "tmp"
var Port = 9999

var ErrRecordNotFound = fmt.Errorf("record not found")
var ErrInvalidParam = fmt.Errorf("invalid param")
var ErrFileNotFound = fmt.Errorf("file not found")
var ErrRecordExpired = fmt.Errorf("record expired")
var ErrNoRelatedExpire = fmt.Errorf("expire not set")

var ErrInvalidInput = fmt.Errorf("invalid input")
var ErrUnknownCmd = fmt.Errorf("unknown command")

func InitCfg(toRootPath string) {
	cfgPath := toRootPath + flag.Lookup("cfg").Value.String()
	InitCfgWithDirectPath(cfgPath)
}

func InitCfgWithDirectPath(path string) {
	content, err := ioutil.ReadFile(path)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(content, &c)
	if err != nil {
		panic(err)
	}

	BlockSize = c.BlockSize
	DBDir = c.DBDir
	Port = c.Port
}
