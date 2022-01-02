package main

import (
	"github.com/go-zookeeper/zk"
)

var zkClient *zk.Conn
var zkPermAll = zk.WorldACL(zk.PermAll)

func zkMustInit() {
	var err error
	zkClient, _, err = zk.Connect(zkServers, zkSessionTimeout)
	if err != nil {
		panic(err)
	}
	// ping test
	_, _, err = zkClient.Exists("/test")
	if err != nil {
		panic("cannot connect to zk")
	}
}
