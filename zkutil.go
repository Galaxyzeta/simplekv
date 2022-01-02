package simplekv

import (
	"strings"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/go-zookeeper/zk"
)

var zkClient *zk.Conn
var zkPermAll = zk.WorldACL(zk.PermAll)

var zkUtilLogger *util.Logger

const ZkPathIsr = "/simplekv/var/isr"
const ZkPathControllerNode = "/simplekv/election/controller"
const ZkPathLeaderNode = "/simplekv/election/leader"
const ZkNodeConnectionPath = "/simplekv/nodes"

func zkMustInit() {
	var err error
	zkUtilLogger = util.NewLogger("[ZkUtil]", config.LogOutputWriter)
	zkClient, _, err = zk.Connect(config.ZkServers, config.ZkSessionTimeout)
	if err != nil {
		panic(err)
	}

	// Create all unexisting parent zknodes.
	// Note: if we don't add / to zkNodeConnectionPath, the subfolder won't be created.
	var toExtract = []string{
		ZkPathControllerNode, ZkPathLeaderNode, ZkPathIsr + "/", ZkNodeConnectionPath + "/",
	}
	set := map[string]struct{}{}
	for _, eachPath := range toExtract {
		for i := 1; i < len(eachPath); i++ {
			char := eachPath[i]
			if char == '/' {
				set[eachPath[0:i]] = struct{}{}
			}
		}
	}
	for eachPath := range set {
		_, err := zkClient.Create(eachPath, nil, 0, zkPermAll)
		zkUtilLogger.Infof("Trying to create znode %s", eachPath)
		if err != nil && err != zk.ErrNodeExists {
			zkUtilLogger.Errorf("Panic: %s", err.Error())
			panic(err)
		}
	}
}

func zkSetIsr(isrSet map[string]struct{}) error {
	str := strings.Join(util.StringSet2List(isrSet), ",")
	ok, _, err := zkClient.Exists(ZkPathIsr)
	if err != nil {
		return err
	}
	if !ok {
		_, err = zkClient.Create(ZkPathIsr, []byte(str), 0, zk.WorldACL(zk.PermAll))
	} else {
		_, err = zkClient.Set(ZkPathIsr, []byte(str), -1)
	}
	return err
}

func zkGetIsr() (isrSet []string, err error) {
	data, _, err := zkClient.Get(ZkPathIsr)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(data), ","), nil
}

func zkGetControllerName() (string, error) {
	// TODO when there's a data storage protocol change on the controllerNode, this method should be re-write.
	data, _, err := zkClient.Get(ZkPathControllerNode)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func zkGetLeader() (util.ZkLeader, error) {
	// TODO when there's a data storage protocol change on the leaderNode, this method should be re-write.
	data, _, err := zkClient.Get(ZkPathLeaderNode)
	if err != nil {
		return util.ZkLeader{}, err
	}
	return util.ParseZkLeader(string(data)), nil
}
