package config

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v2"
)

var ConfigFileDir = ""

type dataconfig struct {
	Blocksize int64  `yaml:"blocksize"`
	Dir       string `yaml:"dir"`
}
type netconfig struct {
	Dataport int `yaml:"dataport"`
	Ctrlport int `yaml:"ctrlport"`
}
type zkconfig struct {
	Servers             []string `yaml:"servers"`
	Timeout             int      `yaml:"sessionTimeout"`
	NodeNotExistBackoff int      `yaml:"nodeNotExistBackoff"`
}
type retryConfig struct {
	Backoff int `yaml:"backoff"`
	Count   int `yaml:"count"`
}
type clusterConfig struct {
	NodeName         string            `yaml:"nodeName"`
	Node2HostportMap map[string]string `yaml:"node2Hostports"`
}
type replicationConfig struct {
	Isr               isrConfig     `yaml:"isr"`
	LogFetchInterval  int64         `yaml:"logFetchInterval"`
	LogDelayerTimeout time.Duration `yaml:"logDelayerTimeout"`
}
type isrConfig struct {
	MaxCatchUpTime int `yaml:"maxCatchUpTime"`
	MaxNoFetchTime int `yaml:"maxNoFetchTime"`
	MaxDelayCount  int `yaml:"maxDelayCount"`
}
type commitConfig struct {
	InitQueueSize int `yaml:"initQueueSize"`
	RequiredAcks  int `yaml:"requiredAcks"`
	Timeout       int `yaml:"timeout"`
}
type electionConfig struct {
	LeaderTimeout int `yaml:"leaderTimeout"`
}
type logConfig struct {
	Output string `yaml:"output"`
}
type config struct {
	Data        dataconfig        `yaml:"data"`
	Net         netconfig         `yaml:"net"`
	Zk          zkconfig          `yaml:"zk"`
	Retry       retryConfig       `yaml:"retry"`
	Cluster     clusterConfig     `yaml:"cluster"`
	Commit      commitConfig      `yaml:"commit"`
	Log         logConfig         `yaml:"log"`
	Election    electionConfig    `yaml:"election"`
	Replication replicationConfig `yaml:"replication"`
}

var c config

var DataBlockSize = int64(64)
var DataDir = "tmp"
var NetDataPort = 9999
var NetControlPort = 9998
var ZkServers []string = []string{
	"127.0.0.1:2181",
}
var ZkSessionTimeout time.Duration = time.Minute * 30
var ZkNodeNotExistBackoff = time.Second
var RetryBackoff time.Duration = time.Millisecond * 50
var RetryCount = 5
var CommitInitQueueSize = 1024
var CommitMaxAck = 0 // 0: don't need ack; 1: at least one ack; 1+: more ack.
var CommitTimeout = time.Second * 5
var ClusterNodeName = "default"
var ClusterNode2HostportMap = map[string]string{}
var ElectionLeaderTimeout = time.Second
var ReplicationLogFetchInterval = time.Millisecond * 50
var ReplicationLogDelayerTimeout = time.Millisecond * 500
var ReplicationIsrMaxDelayCount = 20
var ReplicationIsrMaxCatchUpTime = time.Millisecond * 500
var ReplicationIsrMaxNoFetchTime = time.Millisecond * 250
var LogOutput = "stdout"

var ErrRecordNotFound = fmt.Errorf("record not found")
var ErrInvalidParam = fmt.Errorf("invalid param")
var ErrFileNotFound = fmt.Errorf("file not found")
var ErrRecordExpired = fmt.Errorf("record expired")
var ErrNoRelatedExpire = fmt.Errorf("expire not set")
var ErrBrokenData = fmt.Errorf("broken data")
var ErrTimeout = fmt.Errorf("timeout")
var ErrNotLeader = fmt.Errorf("not leader")
var ErrInvalidInput = fmt.Errorf("invalid input")
var ErrUnknownCmd = fmt.Errorf("unknown command")
var ErrNoLeaderFound = fmt.Errorf("cannot find leader")

var LogOutputWriter = os.Stdout

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

	DataBlockSize = c.Data.Blocksize
	DataDir = c.Data.Dir
	NetDataPort = c.Net.Dataport
	NetControlPort = c.Net.Ctrlport
	ZkServers = c.Zk.Servers
	ZkSessionTimeout = time.Millisecond * time.Duration(c.Zk.Timeout)
	ZkNodeNotExistBackoff = time.Millisecond * time.Duration(c.Zk.NodeNotExistBackoff)
	RetryBackoff = time.Millisecond * time.Duration(c.Retry.Backoff)
	RetryCount = c.Retry.Count
	CommitInitQueueSize = c.Commit.Timeout
	CommitMaxAck = c.Commit.RequiredAcks
	CommitTimeout = time.Millisecond * time.Duration(c.Commit.Timeout)
	ClusterNodeName = c.Cluster.NodeName
	ClusterNode2HostportMap = c.Cluster.Node2HostportMap
	ElectionLeaderTimeout = time.Millisecond * time.Duration(c.Election.LeaderTimeout)
	ReplicationLogFetchInterval = time.Millisecond * time.Duration(c.Replication.LogFetchInterval)
	ReplicationLogDelayerTimeout = time.Millisecond * time.Duration(c.Replication.LogDelayerTimeout)
	ReplicationIsrMaxDelayCount = c.Replication.Isr.MaxDelayCount
	ReplicationIsrMaxCatchUpTime = time.Millisecond * time.Duration(c.Replication.Isr.MaxCatchUpTime)
	ReplicationIsrMaxNoFetchTime = time.Millisecond * time.Duration(c.Replication.Isr.MaxNoFetchTime)
	LogOutput = c.Log.Output

	if LogOutput != "stdout" {
		log.Println("all logs will be append to file: ", LogOutput)
		// check whether directory exist, if not make a new directory
		tmp := LogOutput
		if n := strings.LastIndex(LogOutput, "/"); n != -1 {
			tmp = tmp[:n]
		}
		if err := os.MkdirAll(tmp, 0644); err != nil {
			panic(err)
		}
		fp, err := os.OpenFile(LogOutput, os.O_CREATE|os.O_APPEND|os.O_SYNC, os.ModePerm) // TODO remove SYNC if you're not testing
		if err != nil {
			panic(err)
		}
		LogOutputWriter = fp
	} else {
		log.Println("all logs will be append to stdout")
	}
}

func IsCriticalErr(err error) bool {
	return err == ErrBrokenData
}
