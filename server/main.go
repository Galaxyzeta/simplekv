package main

import (
	"os"

	"github.com/galaxyzeta/simplekv"
	"github.com/galaxyzeta/simplekv/config"
)

func main() {
	if config.DeleteEverythingWhenReboot {
		os.RemoveAll("tmp") // TODO should remove this
	}
	simplekv.Run(nil)
}
