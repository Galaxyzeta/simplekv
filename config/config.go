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
	Blocksize   int64  `yaml:"blocksize"`
	Dir         string `yaml:"dir"`
	LruCapacity int64  `yaml:"lruCapacity"`
}
type netconfig struct {
	Dataport int `yaml:"dataport"`
	Ctrlport int `yaml:"ctrlport"`
}
type zkconfig struct {
	Servers []string `yaml:"servers"`
	Timeout int      `yaml:"sessionTimeout"`
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
	UpdateInterval int `yaml:"updateInterval"`
	MinIsrRequired int `yaml:"minIsrRequired"`
}
type commitConfig struct {
	InitQueueSize int `yaml:"initQueueSize"`
	Timeout       int `yaml:"timeout"`
}
type electionConfig struct {
	LeaderTimeout int `yaml:"leaderTimeout"`
}
type logConfig struct {
	Output string `yaml:"output"`
}
type debugConfig struct {
	PprofPort string `yaml:"pprof"`
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
	Debug       debugConfig       `yaml:"debug"`
}

var c config = config{
	Data: dataconfig{
		Blocksize:   64,
		Dir:         "tmp",
		LruCapacity: 4096,
	},
	Net: netconfig{
		Dataport: 9999,
		Ctrlport: 9998,
	},
	Zk: zkconfig{
		Servers: []string{
			"127.0.0.1:2181",
		},
		Timeout: 3000,
	},
	Retry: retryConfig{
		Backoff: 50,
		Count:   5,
	},
	Cluster: clusterConfig{
		NodeName:         "",
		Node2HostportMap: map[string]string{},
	},
	Commit: commitConfig{
		InitQueueSize: 1024,
		Timeout:       1000,
	},
	Log: logConfig{
		Output: "stdout",
	},
	Election: electionConfig{
		LeaderTimeout: 1000,
	},
	Replication: replicationConfig{
		Isr: isrConfig{
			MaxCatchUpTime: 500,
			MaxNoFetchTime: 250,
			MaxDelayCount:  20,
			UpdateInterval: 1000,
			MinIsrRequired: 0,
		},
		LogFetchInterval:  50,
		LogDelayerTimeout: 500,
	},
	Debug: debugConfig{
		PprofPort: "6060s",
	},
}

var DataBlockSize = int64(64)
var DataDir = "tmp"
var DataLruCapacity = 4096
var NetDataPort = 9999
var NetControlPort = 9998
var ZkServers []string = []string{
	"127.0.0.1:2181",
}
var ZkSessionTimeout time.Duration = time.Minute * 30
var RetryBackoff time.Duration = time.Millisecond * 50
var RetryCount = 5
var CommitInitQueueSize = 1024
var CommitTimeout = time.Second * 1
var ClusterNodeName = "default"
var ClusterNode2HostportMap = map[string]string{}
var ElectionLeaderTimeout = time.Second
var ReplicationLogFetchInterval = time.Millisecond * 50
var ReplicationLogDelayerTimeout = time.Millisecond * 500
var ReplicationIsrMaxDelayCount = 999999
var ReplicationIsrMaxCatchUpTime = time.Millisecond * 500
var ReplicationIsrMaxNoFetchTime = time.Millisecond * 250
var ReplicationIsrUpdateInterval = time.Second
var ReplicationIsrMinRequired = 1
var LogOutput = "stdout"
var DebugPprofPort = "6060"

var ErrRecordNotFound = fmt.Errorf("record not found")
var ErrInternalRecordExpired = fmt.Errorf("record expired") // Used internally
var ErrInvalidParam = fmt.Errorf("invalid param")
var ErrFileNotFound = fmt.Errorf("file not found")
var ErrNoRelatedExpire = fmt.Errorf("expire not set")
var ErrBrokenData = fmt.Errorf("broken data")
var ErrTimeout = fmt.Errorf("timeout")
var ErrNotLeader = fmt.Errorf("not leader")
var ErrInvalidInput = fmt.Errorf("invalid input")
var ErrUnknownCmd = fmt.Errorf("unknown command")
var ErrNoLeaderFound = fmt.Errorf("cannot find leader")
var ErrDataPlaneNotReady = fmt.Errorf("dataplane not ready")
var ErrNotEnoughIsr = fmt.Errorf("not enough Isr")
var ErrInvalidRequiredIsr = fmt.Errorf("invalid requiredIsr param")
var ErrEntryCancel = fmt.Errorf("entry canceled")
var ErrStaleLeaderEpoch = fmt.Errorf("stale leader epoch")

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
	DataLruCapacity = int(c.Data.LruCapacity)
	NetDataPort = c.Net.Dataport
	NetControlPort = c.Net.Ctrlport
	ZkServers = c.Zk.Servers
	ZkSessionTimeout = time.Millisecond * time.Duration(c.Zk.Timeout)
	RetryBackoff = time.Millisecond * time.Duration(c.Retry.Backoff)
	RetryCount = c.Retry.Count
	CommitInitQueueSize = c.Commit.InitQueueSize
	CommitTimeout = time.Millisecond * time.Duration(c.Commit.Timeout)
	ClusterNodeName = c.Cluster.NodeName
	ClusterNode2HostportMap = c.Cluster.Node2HostportMap
	ElectionLeaderTimeout = time.Millisecond * time.Duration(c.Election.LeaderTimeout)
	ReplicationLogFetchInterval = time.Millisecond * time.Duration(c.Replication.LogFetchInterval)
	ReplicationLogDelayerTimeout = time.Millisecond * time.Duration(c.Replication.LogDelayerTimeout)
	ReplicationIsrMaxDelayCount = c.Replication.Isr.MaxDelayCount
	ReplicationIsrMaxCatchUpTime = time.Millisecond * time.Duration(c.Replication.Isr.MaxCatchUpTime)
	ReplicationIsrMaxNoFetchTime = time.Millisecond * time.Duration(c.Replication.Isr.MaxNoFetchTime)
	ReplicationIsrUpdateInterval = time.Millisecond * time.Duration(c.Replication.Isr.UpdateInterval)
	ReplicationIsrMinRequired = c.Replication.Isr.MinIsrRequired
	LogOutput = c.Log.Output
	DebugPprofPort = c.Debug.PprofPort

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
