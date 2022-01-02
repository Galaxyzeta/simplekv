package simplekv

import (
	"fmt"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/go-zookeeper/zk"
)

type nodeMonitor struct {
	logger *util.Logger
}

func newNodeMonitor() *nodeMonitor {
	return &nodeMonitor{
		logger: util.NewLogger("[NodeMonitor]", config.LogOutputWriter),
	}
}

func (nm *nodeMonitor) commonMonitorGetW(path string, onNodeNotExist func(), onNodeFound func(data string)) {
	for {
		data, _, ch, err := zkClient.GetW(path)
		if err != nil {
			if err == zk.ErrNoNode {
				onNodeNotExist()
				continue
			}
			// net err or other
			nm.logger.Errorf("Adding watch on node err: %s", err.Error())
			time.Sleep(config.RetryBackoff)
			continue
		}
		onNodeFound(string(data))
		<-ch
	}
}

func (nm *nodeMonitor) commonMonitorExistW(path string, data string, onNodeNotExist func(data string), onNodeFound func(data string)) {
	for {
		ok, _, ch, err := zkClient.ExistsW(path)
		if !ok {
			onNodeNotExist(data)
			continue
		}
		if err != nil {
			// net err or other
			nm.logger.Errorf("Adding watch on node err: %s", err.Error())
			time.Sleep(config.RetryBackoff)
			continue
		}
		onNodeFound(data)
		<-ch
	}
}

func (nm *nodeMonitor) onControllerLost() {
	nm.logger.Infof("Detect controller lost")
	ctrlInstance.cmdExecutor.enqueueAndWait_ControllerElectionCmd()
}
func (nm *nodeMonitor) onControllerFound(controllerData string) {
	nm.logger.Infof("Controller found, data = %s", controllerData)
	ctrlInstance.cmdExecutor.enqueueAndWait_SetControllerNameCmd(controllerData)
}
func (nm *nodeMonitor) onLeaderLost() {
	nm.logger.Infof("Detect leader lost")
	ctrlInstance.cmdExecutor.enqueueAndWait_LeaderElectionCmd()
}
func (nm *nodeMonitor) onLeaderFound(leaderData string) {
	nm.logger.Infof("Leader found, data = %s", leaderData)
	ctrlInstance.cmdExecutor.enqueueAndWait_SetLeaderNameCmd(leaderData)
}
func (nm *nodeMonitor) onNodeLost(nodeName string) {
	nm.logger.Infof("Detect node lost, %s", nodeName)
	// if node is self, register itself to zk
	if nodeName == ctrlInstance.nodeName {
		if err := util.RetryWithMaxCount(func() (bool, error) {
			return true, nm.registerSelf()
		}, config.RetryCount); err != nil {
			nm.logger.Errorf("Register to zookeeper failed: %s", err.Error())
			return
		}
		ctrlInstance.cmdExecutor.enqueueAndWait_SetNodeStatus(nodeName, false)
	} else if ctrlInstance.isOffline(nodeName) {
		// Sleep for a while and re-detect again
		time.Sleep(config.ZkNodeNotExistBackoff)
	} else {
		// Not offlined yet, set it to offline.
		ctrlInstance.cmdExecutor.enqueueAndWait_SetNodeStatus(nodeName, true)
	}
}
func (nm *nodeMonitor) onNodeFound(nodeName string) {
	nm.logger.Infof("Node found, nodeName = %s", nodeName)
	ctrlInstance.cmdExecutor.enqueueAndWait_SetNodeStatus(nodeName, false)
}

// monitorLeaderNode monitors leader node with a znode watch callback to erase current leader name
// when a leader lost is detected.
func (nm *nodeMonitor) monitorLeaderNode() {
	for {
		data, _, ch, err := zkClient.GetW(ZkPathLeaderNode)
		if err != nil {
			if err == zk.ErrNoNode {
				// node not exist
				nm.logger.Infof("Leader node does not exist. Current leader name set to empty string")
				ctrlInstance.setCurrentLeaderName("")
				ctrlInstance.condHasLeader.LoopWaitUntilTrue()
				continue
			}
			// net err or other
			nm.logger.Errorf("Adding watch on leader node err: %s", err.Error())
			time.Sleep(config.RetryBackoff)
			continue
		}
		// contain leader
		zkLeader := util.ParseZkLeader(string(data))
		ctrlInstance.setCurrentLeaderName(zkLeader.Name)
		<-ch
	}
}

// monitorNodeStatus observing node health status. If it is dead, add it to the offline pool.
func (nm *nodeMonitor) monitorNodeStatus(nodeName string, hostport string) {
	var nodePath = fmt.Sprintf("%s/%s", ZkNodeConnectionPath, nodeName)
	var err error
	var ch <-chan zk.Event
	var exist bool
	for {
		exist, _, ch, err = zkClient.ExistsW(nodePath)
		if err != nil {
			nm.logger.Errorf("Set watch on zknode err: ", err.Error())
			time.Sleep(config.RetryBackoff)
			continue
		}
		if !exist {
			nm.logger.Warnf("Node %s does not exist", nodePath)
			if nodeName == ctrlInstance.nodeName {
				nm.logger.Warnf("Self node not exist, trying to register myself to zookeeper")
				if err := nm.registerSelf(); err != nil {
					nm.logger.Errorf("Register self to zookeeper err: %s", err.Error())
				}
				continue
			} // else, other node was lost.
			if !ctrlInstance.isOffline(nodeName) {
				nm.logger.Warnf("Node %s is marked as offline", nodePath)
				ctrlInstance.addToOffline(nodeName)
			}
			time.Sleep(config.ZkNodeNotExistBackoff)
			continue
		}
		nm.logger.Infof("Add watch on living node %s OK", nodePath)
		ctrlInstance.removeFromOffline(nodeName) // Node is alive, try to remove it from offlineSet.
		<-ch                                     // Wait until something happens to the node, then retry the loop to check whether it is still alive or not.
		nm.logger.Infof("Detect node change on znode = %s", nodePath)
	}
}

// registerSelf register myself to zookeeper with infinite retry until succeed.
func (nm *nodeMonitor) registerSelf() error {
	return util.RetryWithMaxCount(func() (bool, error) {
		var nodePath = fmt.Sprintf("%s/%s", ZkNodeConnectionPath, ctrlInstance.nodeName)
		_, err := zkClient.Create(nodePath, []byte(ctrlInstance.getSelfHostport()), zk.FlagEphemeral, zkPermAll)
		if err != nil && err != zk.ErrNodeExists {
			return true, err
		}
		nm.logger.Infof("Registering %s to znode OK", nodePath)
		return false, nil
	}, config.RetryCount)
}

func (nm *nodeMonitor) run() {
	go nm.commonMonitorGetW(ZkPathControllerNode, nm.onControllerLost, nm.onControllerFound)
	go nm.commonMonitorGetW(ZkPathLeaderNode, nm.onLeaderLost, nm.onLeaderFound)
	for nodeName := range ctrlInstance.getNode2HostportMap() {
		go nm.commonMonitorExistW(fmt.Sprintf("%s/%s", ZkNodeConnectionPath, nodeName), nodeName, nm.onNodeLost, nm.onNodeFound)
	}
}
