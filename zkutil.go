package simplekv

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/go-zookeeper/zk"
)

var zkClient *zk.Conn
var zkPermAll = zk.WorldACL(zk.PermAll)

var zkUtilLogger *util.Logger

const ZKPathRoot = "/simplekv"
const ZkPathIsr = ZKPathRoot + "/var/isr"
const ZkPathLeaderNode = ZKPathRoot + "/election/leader"
const ZKPathLeaderEpoch = ZKPathRoot + "/election/leader-epoch"
const ZkPathNodeConnection = ZKPathRoot + "/nodes"

func zkRetryBackoff() {
	time.Sleep(config.RetryBackoff)
}

// Watch change in children.
func zkMonitorChildren(path string, onChildrenChanged func()) {
	go func() {
		for {
			_, _, ch, err := zkClient.ChildrenW(path)
			if err != nil {
				zkUtilLogger.Errorf("ChildrenW failed: %s", err.Error())
				zkRetryBackoff()
			}
			ev := <-ch
			if ev.Type == zk.EventNodeChildrenChanged {
				onChildrenChanged()
			}
		}
	}()
}

// Watch path, if node exists before and is lost for now, will execute onNodeLost() and keep watching.
// If node not exist, will execute onNodeNotExist() and return.
func zKMonitorNodeLost(path string, onNodeLost func()) {
	go func() {
		for {
			exist, _, ch, err := zkClient.ExistsW(path)
			if err != nil {
				zkUtilLogger.Errorf("ExistW failed: %s", err.Error())
				zkRetryBackoff()
			}
			if !exist {
				zkUtilLogger.Infof("Waiting for node to come back: %s", path)
				zkRetryBackoff()
			}
			if ev := <-ch; ev.Type == zk.EventNodeDeleted {
				zkUtilLogger.Infof("Detected node deleted: %s, executing function onNodeLost: %s", path, onNodeLost)
				onNodeLost()
			}
		}
	}()
}

func zkMustInit() {
	var err error
	zkUtilLogger = util.NewLogger("[ZkUtil]", config.LogOutputWriter)
	zkClient, _, err = zk.Connect(config.ZkServers, config.ZkSessionTimeout)
	if err != nil {
		panic(err)
	}

	// Create all unexisting parent zknodes.
	// Note: if we don't add / to zkNodeConnectionPath, the subfolder won't be created.
	// TODO too tricky here.
	var toExtract = []string{
		ZkPathLeaderNode, ZkPathIsr, ZkPathNodeConnection + "/",
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
	// Create from the shortest path to the longest path to prevent parent path not exist bug.
	pathList := util.StringSet2List(set)
	sort.Slice(pathList, func(i, j int) bool {
		return pathList[i] < pathList[j]
	})
	for _, eachPath := range pathList {
		_, err := zkClient.Create(eachPath, nil, 0, zkPermAll)
		zkUtilLogger.Infof("Trying to create znode %s", eachPath)
		if err != nil && err != zk.ErrNodeExists {
			zkUtilLogger.Errorf("Panic: %s", err.Error())
			panic(err)
		}
	}
}

func zkMustShutdown() {
	if zkClient != nil {
		zkClient.Close()
	}
}

// Get leaderEpoch from Zk.
// Need to check node not exist error.
// Will panic if leaderEpoch is not a number.
func zkGetLeaderEpoch() (int64, error) {
	data, _, err := zkClient.Get(ZKPathLeaderEpoch)
	if err != nil {
		return 0, err
	}
	intval, err := strconv.Atoi(string(data))
	if err != nil {
		zkUtilLogger.Fatalf("leaderEpoch is not a number, this is fatal")
	}
	return int64(intval), nil
}

// This method should not be used directly.
func zkSetLeaderEpoch(leaderEpoch int64) error {
	data := strconv.Itoa(int(leaderEpoch))
	_, err := zkClient.Set(ZKPathLeaderEpoch, []byte(data), -1)
	return err
}

// Contain distributed lock operation. Use this function with caution.
func zkIncreLeaderEpochInfiniteRetry() int64 {
	var val int64
	var err error
	t := time.Now()
	zkAtomicOperationInfiniteRetry("/simplekv/lock-incre-leader-epoch", func() error {
		val, err = zkGetLeaderEpoch()
		if zkNodeNotExistErr(err) {
			if err = zkCreateOrSetPermanent(ZKPathLeaderEpoch, "0"); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		if err0 := zkSetLeaderEpoch(val + 1); err0 != nil {
			return err0
		}
		return nil
	})
	zkUtilLogger.Debugf("Time cost on this operation: %d ms", time.Since(t).Milliseconds())
	return val + 1
}

// zkSetIsr set / creates ISR in zk.
func zkSetIsr(isrList []string) error {
	str := strings.Join(isrList, ",")
	return zkCreateOrSetPermanent(ZkPathIsr, str)
}

// zkDeleteIsr deletes isr node in zk.
// Use this with caution.
func zkDeleteIsr() error {
	return zkClient.Delete(ZkPathIsr, -1)
}

func zkDeleteRootDir() error {
	return zkDeleteRecursively(ZKPathRoot)
}

func zkDeleteRecursively(root string) error {
	children, _, err := zkClient.Children(root)
	if err != nil {
		return err
	}
	for _, child := range children {
		if err = zkDeleteRecursively(root + "/" + child); err != nil {
			return err
		}
	}
	return zkClient.Delete(root, -1)
}

func zkAtomicOperationInfiniteRetry(lock string, fn func() error) {
	for {
		_, err := zkClient.Create(lock, nil, zk.FlagEphemeral, zkPermAll)
		if zkNodeExistErr(err) {
			zkUtilLogger.Debugf("Lock %s has been acquired by someone. Wait and retry.", lock)
			zkRetryBackoff()
			continue
		} else if err != nil {
			zkUtilLogger.Debugf("Create lock %s err: %s", lock, err.Error())
			zkRetryBackoff()
			continue
		}
		break
	}
	// Critical zone
	for err := fn(); err != nil; {
		zkUtilLogger.Errorf("While executing function in critical zone, err: %s", err.Error())
	}
	// Critical zone end
	for {
		err := zkClient.Delete(lock, -1)
		if zkNodeNotExistErr(err) {
			zkUtilLogger.Warnf("Lock has been deleted while executing critical code !")
			break
		} else if err != nil {
			zkUtilLogger.Errorf("Delete lock %s err: %s", lock, err.Error())
			continue
		}
		break
	}
}

// Set the value. If node not exist, will create it first.
func zkCreateOrSetPermanent(path string, value string) error {
	data := []byte(value)
	_, err := zkClient.Set(path, data, -1)
	if zkNodeNotExistErr(err) {
		_, err = zkClient.Create(path, data, 0, zkPermAll)
	}
	return err
}

func zkGetIsr() (isrList []string, err error) {
	data, _, err := zkClient.Get(ZkPathIsr)
	strData := string(data)
	if len(strings.TrimSpace(strData)) == 0 {
		return
	}
	if err != nil {
		return nil, err
	}
	return strings.Split(string(strData), ","), nil
}

func zkExistsIsr() (bool, error) {
	exists, _, err := zkClient.Exists(ZkPathIsr)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func zkGetLeader() (util.ZkLeader, error) {
	// TODO when there's a data storage protocol change on the leaderNode, this method should be re-write.
	data, _, err := zkClient.Get(ZkPathLeaderNode)
	if err != nil {
		return util.ZkLeader{}, err
	}
	return util.ParseZkLeader(string(data)), nil
}

// Try to set leader if leader node not exist.
// Returns:
// <true, nil> if the set was successful.
// <false, nil> if node exists.
// <false, err> if network error.
func zkTrySetLeader(metadata util.ZkLeader) (bool, error) {
	_, err := zkClient.Create(ZkPathLeaderNode, []byte(metadata.Format()), zk.FlagEphemeral, zkPermAll)
	if err != nil {
		if zkNodeExistErr(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func zkRegisterNode(nodeName string) error {
	return util.RetryWithMaxCount(func() (bool, error) {
		var nodePath = fmt.Sprintf("%s/%s", ZkPathNodeConnection, nodeName)
		_, err := zkClient.Create(nodePath, []byte(ctrlInstance.getSelfHostport()), zk.FlagEphemeral, zkPermAll)
		if err != nil && err != zk.ErrNodeExists {
			return true, err
		}
		zkUtilLogger.Infof("Registering %s to znode OK", nodePath)
		return false, nil
	}, config.RetryCount)
}

func zkGetLivingNodes() ([]string, error) {
	children, _, err := zkClient.Children(ZkPathNodeConnection)
	if err != nil {
		return nil, err
	}
	return children, nil
}

func zkNodeNotExistErr(err error) bool {
	return err == zk.ErrNoNode
}

func zkNodeExistErr(err error) bool {
	return err == zk.ErrNodeExists
}
