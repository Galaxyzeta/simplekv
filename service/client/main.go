package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/service/proto"
	"github.com/galaxyzeta/simplekv/util"
	"google.golang.org/grpc"
)

type clientHandler func(ctx context.Context, params ...string) (string, error)

var handlerRegistry = map[string]clientHandler{
	"get":    Get,
	"set":    Set,
	"del":    Del,
	"expire": Expire,
	"ttl":    TTL,
}

var grpcClient proto.SimpleKVClient

func main() {

	// TODO implement routing method

	addr := "localhost:9999" // TODO read from config files

	conn, err := grpc.Dial(addr, grpc.WithInsecure()) // TODO security
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	grpcClient = proto.NewSimpleKVClient(conn)

	sc := bufio.NewScanner(os.Stdin) // use scanner to read line by line.
	for {
		fmt.Printf("[%s] > ", addr)
		sc.Scan()
		input := sc.Text()
		if err != nil {
			panic(err)
		}
		fmt.Println(handleInput(input))
	}
}

func handleInput(input string) string {
	args := strings.Split(input, " ")
	if len(args) == 0 {
		return config.ErrInvalidInput.Error()
	}
	cmd := util.StringStandardize(args[0])
	fn, ok := handlerRegistry[cmd]
	if !ok {
		return config.ErrUnknownCmd.Error()
	}

	var result string
	var err error
	if len(args) >= 1 {
		result, err = fn(context.Background(), args[1:]...) // TODO context
	} else {
		result, err = fn(context.Background())
	}

	if err != nil {
		return err.Error()
	}
	return result
}
