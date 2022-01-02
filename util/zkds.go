package util

import (
	"fmt"
	"strings"
)

type ZkLeader struct {
	Name         string
	CtrlHostport string
	DataHostport string
}

func MakeZkLeader(name, hostport, dataHostport string) ZkLeader {
	return ZkLeader{
		Name:         name,
		CtrlHostport: hostport,
		DataHostport: dataHostport,
	}
}

func ParseZkLeader(dat string) ZkLeader {
	strList := strings.Split(dat, ",")
	return MakeZkLeader(strList[0], strList[1], strList[2])
}

func FormatZkLeader(name, ctrlHostport, dataHostport string) string {
	return MakeZkLeader(name, ctrlHostport, dataHostport).Format()
}

func (ds ZkLeader) Format() string {
	return fmt.Sprintf("%s,%s,%s", ds.Name, ds.CtrlHostport, ds.DataHostport)
}
