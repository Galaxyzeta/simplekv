package simplekv

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/util"
)

var ctrlInstance *controlPlane
var ctrlInstanceInitOnce = sync.Once{}
var ctrlInstanceRunOnce = sync.Once{}

type roleStateEnum uint8

const (
	roleState_NoRole roleStateEnum = iota
	roleState_Leader
	roleState_Follower
)

// controlPlane coordinates distributive problems.
type controlPlane struct {
	nodeName          string            // immutable after initialization
	nodeMap           map[string]string // immutable after initialization
	hostport2nodeName map[string]string // immutable after initialization

	controllerElectionMgr *controllerElectionManager
	leaderElectionMgr     *leaderElectionManager
	rpcMgr                *controlPlaneRpcManager
	commitMgr             *commitManager
	replicationManager    *replicationManager
	nodeMonitor           *nodeMonitor
	cmdExecutor           *controlPlaneExecutor

	condHasLeader    util.ConditionBlocker
	condIsFollower   util.ConditionBlocker
	condIsController util.ConditionBlocker

	offlineNodeSet sync.Map

	_curLeaderName     atomic.Value // string
	_curControllerName atomic.Value // string
	_curRoleStatus     atomic.Value // roleStatusEnum
}

// initControlPlaneSingleton inits everything needed for message replication, leader/controller election, etc.
func initControlPlaneSingleton() {
	ctrlInstanceInitOnce.Do(func() {
		hostports := make([]string, 0, len(config.ClusterNode2HostportMap))
		hostport2NodeName := make(map[string]string, len(config.ClusterNode2HostportMap))
		for nodeName, eachHostport := range config.ClusterNode2HostportMap {
			hostports = append(hostports, eachHostport)
			hostport2NodeName[eachHostport] = nodeName
		}
		ctrlInstance = &controlPlane{
			nodeName:              config.ClusterNodeName,
			controllerElectionMgr: newControllerElectionManager(),
			commitMgr:             newCommitManager(),
			leaderElectionMgr:     newLeaderElectionManager(),
			rpcMgr:                newControlPlaneRpcManager(hostports),
			replicationManager:    newReplicaManager(config.ClusterNode2HostportMap),
			nodeMonitor:           newNodeMonitor(),
			cmdExecutor:           newControlPlaneExecutor(),

			hostport2nodeName: hostport2NodeName,

			nodeMap:          config.ClusterNode2HostportMap,
			condHasLeader:    util.NewConditionBlocker(func() bool { return ctrlInstance.hasLeader() }),
			condIsController: util.NewConditionBlocker(func() bool { return ctrlInstance.isController() }),
			condIsFollower:   util.NewConditionBlocker(func() bool { return ctrlInstance.isFollower() }),
		}
		ctrlInstance.setCurrentControllerName("")
		ctrlInstance.setCurrentLeaderName("")
	})
}

func startControlPlaneSingleton() {
	ctrlInstanceRunOnce.Do(func() { // TODO graceful shutdown.
		go ctrlInstance.nodeMonitor.run()
		go ctrlInstance.commitMgr.run()
		go ctrlInstance.cmdExecutor.run()
		// go ctrlInstance.controllerElectionMgr.run()
		// go ctrlInstance.leaderElectionMgr.runElectionHost()
		go ctrlInstance.replicationManager.run()
	})
}

func (cp *controlPlane) currentLeaderName() string {
	return cp._curLeaderName.Load().(string)
}

func (cp *controlPlane) currentRoleStatus() roleStateEnum {
	return cp._curRoleStatus.Load().(roleStateEnum)
}

func (cp *controlPlane) currentLeaderHostport() (string, bool) {
	return cp.getHostport(cp.currentLeaderName())
}

func (cp *controlPlane) currentControllerName() string {
	return cp._curControllerName.Load().(string)
}

func (cp *controlPlane) containNodeName(name string) bool {
	_, ok := cp.nodeMap[name]
	return ok
}

// setCurrentLeaderName sets the leader's name to cache and add a broadcast on certain conditions.
func (cp *controlPlane) setCurrentLeaderName(name string) {
	cp._curLeaderName.Store(name)
	cp.condHasLeader.Broadcast()
	cp.condIsFollower.Broadcast()
}

func (cp *controlPlane) setCurrentControllerName(name string) {
	cp._curControllerName.Store(name)
	cp.condIsController.Broadcast()
}

func (cp *controlPlane) hasLeader() bool {
	return cp.currentLeaderName() != ""
}

func (cp *controlPlane) hasController() bool {
	return cp.currentControllerName() != ""
}

func (cp *controlPlane) isLeader() bool {
	return cp.currentLeaderName() == cp.nodeName
}

func (cp *controlPlane) isFollower() bool {
	c := cp.currentLeaderName()
	return c != cp.nodeName && c != ""
}

func (cp *controlPlane) isController() bool {
	return cp.currentControllerName() == cp.nodeName
}

func (cp *controlPlane) addToOffline(nodeName string) {
	cp.offlineNodeSet.Store(nodeName, struct{}{})
}

func (cp *controlPlane) removeFromOffline(nodeName string) {
	cp.offlineNodeSet.Delete(nodeName)
}

func (cp *controlPlane) isOffline(nodeName string) bool {
	_, ok := cp.offlineNodeSet.Load(nodeName)
	return ok
}

func (cp *controlPlane) getHostport(nodeName string) (string, bool) {
	hostport, ok := cp.nodeMap[nodeName]
	return hostport, ok
}

func (cp *controlPlane) getNodeNameByHostport(hostport string) (string, bool) {
	hostport, ok := cp.hostport2nodeName[hostport]
	return hostport, ok
}

func (cp *controlPlane) getSelfHostport() string {
	return cp.nodeMap[cp.nodeName]
}

func (cp *controlPlane) getSelfHost() string {
	return strings.Split(cp.getSelfHostport(), ":")[0]
}

func (cp *controlPlane) getSelfDataHostport() string {
	return fmt.Sprintf("%s:%d", cp.getSelfHost(), config.NetDataPort)
}

func (cp *controlPlane) getNode2HostportMap() map[string]string {
	return cp.nodeMap
}

func (cp *controlPlane) getClusterSize() int {
	return len(cp.nodeMap)
}

func (cp *controlPlane) getOnlineHostports() (ret map[string]struct{}) {
	offlineNodenames := map[string]struct{}{}
	ret = make(map[string]struct{})
	cp.offlineNodeSet.Range(func(key, value interface{}) bool {
		offlineNodenames[key.(string)] = struct{}{}
		return true
	})
	for nodeName, hostport := range cp.nodeMap {
		if _, ok := offlineNodenames[nodeName]; !ok {
			ret[hostport] = struct{}{}
		}
	}
	return
}
