package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/galaxyzeta/simplekv"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/go-zookeeper/zk"
)

const retryBackoff = time.Second

var _currentLeaderHostport atomic.Value

func init() {
	_currentLeaderHostport.Store("")
}

func currentLeaderHostport() string {
	return _currentLeaderHostport.Load().(string)
}
func setCurrentLeaderDataHostport(hostport string) {
	_currentLeaderHostport.Store(hostport)
}

func monitorCurrentLeader() {
	for {
		data, _, ch, err := zkClient.GetW(simplekv.ZkPathLeaderNode)
		if err != nil {
			if err == zk.ErrNoNode {
				setCurrentLeaderDataHostport("")
				condHasLeader.Broadcast()
				fmt.Printf("warn: leader node not exist, retry...\n")
			} else {
				fmt.Printf("err: while reading from zk, an error occurred: %s\n", err.Error())
			}
			time.Sleep(retryBackoff)
			continue
		}
		zkLeader := util.ParseZkLeader(string(data))
		fmt.Printf("info: setting currentLeader to %s\n", zkLeader.Format())
		setCurrentLeaderDataHostport(zkLeader.DataHostport)
		condHasLeader.Broadcast()
		<-ch
	}
}
