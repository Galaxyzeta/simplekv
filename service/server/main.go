package main

import (
	"flag"
	"fmt"
	"net"

	"github.com/galaxyzeta/simplekv"
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/service/proto"
	"google.golang.org/grpc"
)

func init() {
	flag.StringVar(&config.ConfigFileDir, "cfg", "config/default.yaml", "config file")
}

func main() {
	flag.Parse()
	config.InitCfg("")

	simplekv.MustLoad()

	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", config.Port))
	if err != nil {
		panic(err)
	}
	server := grpc.NewServer()
	proto.RegisterSimpleKVServer(server, &SimplekvService{})
	err = server.Serve(lis)
	if err != nil {
		panic(err)
	}
}
