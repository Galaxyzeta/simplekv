package main

import (
	"flag"

	"github.com/galaxyzeta/simplekv/client"
)

func main() {
	flag.String("cfg", "conf/client/cli.yaml", "client config")
	flag.Parse()
	var cfg = flag.Lookup("cfg")
	client.RunRepl(cfg.Value.String())
}
