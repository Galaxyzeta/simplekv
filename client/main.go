package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
)

type clientHandler func(ctx context.Context, params ...string) (string, error)

var handlerRegistry = map[string]clientHandler{
	"get":    Get,
	"set":    Set,
	"del":    Del,
	"expire": Expire,
	"ttl":    TTL,
}
var condHasLeader = util.NewConditionBlocker(func() bool { return currentLeaderHostport() != "" })

func main() {

	flag.String("cfg", "", "configuration file")
	flag.Parse()
	initCfg("")

	zkMustInit()

	fmt.Println("info: waiting for an available leader...")
	go monitorCurrentLeader()
	sc := bufio.NewScanner(os.Stdin) // use scanner to read line by line.
	for {
		condHasLeader.LoopWaitUntilTrue()
		fmt.Printf("[%s] > ", currentLeaderHostport())
		sc.Scan()
		input := sc.Text()
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
