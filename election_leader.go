package simplekv

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/proto"
	"github.com/galaxyzeta/simplekv/util"
	"github.com/go-zookeeper/zk"
)

type leaderElectionManager struct {
	logger *util.Logger

	isListeningElection atomic.Value  // If there's an election happening.
	electionTimeout     time.Time     // The election will fail if time.Now() > electionTimeout
	electionDuration    time.Duration // Immutable after initialization.
	timeoutMu           sync.Mutex
}

type hostportAndOffset struct {
	hostport string
	offset   int64
}

func newLeaderElectionManager() *leaderElectionManager {
	em := &leaderElectionManager{
		logger:              util.NewLogger("[LeaderElectionManager]", config.LogOutputWriter),
		isListeningElection: atomic.Value{},
		electionDuration:    time.Second,
	}
	em.setElecting(false)
	return em
}

func (em *leaderElectionManager) isElecting() bool {
	return em.isListeningElection.Load().(bool)
}

func (em *leaderElectionManager) setElecting(val bool) {
	em.isListeningElection.Store(val)
}

// onReceiveOffsetCollectRequest will begin a electing process.
func (em *leaderElectionManager) handleReceiveOffsetCollectRequest() {
	if !em.isElecting() { // There's no election, initiate an election.
		em.setElecting(true)
		em.logger.Infof("Election initiated due to receiving offset collect request")
	}
	// Reset timeout in any condition.
	em.timeoutMu.Lock()
	defer em.timeoutMu.Unlock()
	_to := time.Now().Add(em.electionDuration)
	em.logger.Infof("Setting election timeout to %s", util.FormatYYYYMMDDhhmmss(_to))
	em.electionTimeout = _to
}

func (em *leaderElectionManager) handleReceiveRoleChangeRequest(role proto.ServerRole) bool {
	em.logger.Infof("Received role change request to change role to %s", role.String())
	if !em.isElecting() {
		em.logger.Infof("Not electing, ignore role change request")
		return false // Not electing, should ignore the role assignment command.
	}
	var isElectionTimeout = func() bool {
		em.timeoutMu.Lock()
		defer em.timeoutMu.Unlock()
		return time.Now().After(em.electionTimeout)
	}
	defer func() {
		em.logger.Infof("Election has been terminated")
		em.setElecting(false) // defer terminates the election.
	}()
	if isElectionTimeout() {
		em.logger.Infof("Election already timeout, ignoring role change request")
		return false // Not electing, should ignore the role assignment command.
	}
	if role == proto.ServerRole_leader {
		if err := em.becomeLeaderWithRetry(); err != nil {
			em.logger.Errorf("All retries to become leader have failed: %s", err.Error())
			return false
		}
	} else {
		em.becomeFollower()
	}
	return true
}

func (em *leaderElectionManager) runElectionOnce() {
	// Only controller can proceed
	if !ctrlInstance.isController() {
		em.logger.Infof("Not controller, cannot run election, quiting...")
		return
	}
	// Start electing leader. If there's a network error, retry the loop.
	// Take a snapshot of online hostports. Assume they're still online inside the loop.
	var onlineHostports = ctrlInstance.getOnlineHostports()
	// Get a copy of ISR
	var isrSet = util.StringList2Set(ctrlInstance.replicationManager.cloneIsrList(true))
	var shouldConsiderIsr = len(isrSet) == 0 // if isr is empty, do not take isr into consideration.
	// Collecting other node's offset.
	var hostportAndOffsetList = make([]hostportAndOffset, 0, len(onlineHostports))
	var hostportAndOffsetListMu = sync.Mutex{}
	var lastestError error

	em.logger.Infof("Current onlineHostports: %v", onlineHostports)
	em.logger.Infof("Current isr list: %v", isrSet)

	wg := sync.WaitGroup{}

	var appendHostportAndOffsetList = func(hostport string, offset int64) {
		hostportAndOffsetListMu.Lock()
		hostportAndOffsetList = append(hostportAndOffsetList, hostportAndOffset{
			hostport: hostport,
			offset:   offset,
		})
		hostportAndOffsetListMu.Unlock()
	}

	var rpcCollectOffset = func(hostport string) {
		defer wg.Done()
		if lastestError != nil {
			em.logger.Errorf("Encounter err while collecting offset, fail fast")
			return // fail fast
		}
		offset, err := ctrlInstance.rpcMgr.collectOffset(context.Background(), hostport)
		if err != nil {
			em.logger.Errorf("CollectOffset err: %s", err.Error())
			lastestError = err
			return
		}
		nodeName, _ := ctrlInstance.getNodeNameByHostport(hostport)
		if _, ok := isrSet[nodeName]; !ok && shouldConsiderIsr {
			em.logger.Errorf("Node<%s - %s> is not in isr, and isr is not empty", nodeName, hostport)
			return // not in isr, and isr set is not nil
		}
		em.logger.Infof("Recv node<%s>'s offset = %d", nodeName, offset)
		appendHostportAndOffsetList(hostport, offset)
	}

	em.logger.Errorf("Broadcasting offset collect request...")
	for hostport := range onlineHostports {
		if hostport == ctrlInstance.getSelfHostport() {
			appendHostportAndOffsetList(hostport, dataInstance.totalOffset())
		} else {
			wg.Add(1)
			go rpcCollectOffset(hostport)
		}
	}
	wg.Wait()

	if lastestError != nil {
		em.logger.Errorf("encounter error while requesting offset, returning...")
		time.Sleep(config.RetryBackoff)
		return
	}
	if len(hostportAndOffsetList) == 0 {
		em.logger.Errorf("no available election member, returning...")
		time.Sleep(config.RetryBackoff)
		return
	}

	// Sort offsets, and send leadership modification request to all active nodes.
	// If there's still any network error after several retry, redo the election loop again.
	sort.Slice(hostportAndOffsetList, func(i, j int) bool {
		elem1 := hostportAndOffsetList[i]
		elem2 := hostportAndOffsetList[j]
		return elem1.offset > elem2.offset || elem1.offset == elem2.offset && elem1.hostport < elem2.hostport
	})

	// First, handle leader situation.
	// Len(offset) should always > 0 here, because we've already handled Len(offset) = 0 situation above here.
	var leaderHostport = hostportAndOffsetList[0].hostport
	if leaderHostport == ctrlInstance.getSelfHostport() {
		if err := em.becomeLeaderWithRetry(); err != nil {
			em.logger.Errorf("All retry for roleChange rpc on hostport %s has failed: %s, returning...", leaderHostport, err.Error())
			time.Sleep(config.RetryBackoff)
			return
		}
	} else {
		if err := util.RetryWithMaxCount(func() (bool, error) {
			return true, ctrlInstance.rpcMgr.roleChange(context.Background(), leaderHostport, proto.ServerRole_leader)
		}, config.RetryCount); err != nil {
			em.logger.Errorf("All retry for roleChange rpc on hostport %s has failed: %s, returning...", leaderHostport, err.Error())
			time.Sleep(config.RetryBackoff)
			return
		}
	}

	// Second, handle follower situation.
	for _, tuple := range hostportAndOffsetList[1:] {
		followerHostport := tuple.hostport
		if followerHostport == ctrlInstance.getSelfHostport() {
			em.becomeFollower()
		} else {
			if err := util.RetryWithMaxCount(func() (bool, error) {
				return true, ctrlInstance.rpcMgr.roleChange(context.Background(), followerHostport, proto.ServerRole_follower)
			}, config.RetryCount); err != nil {
				em.logger.Errorf("All retry for roleChange rpc on hostport %s has failed: %s, returning...", followerHostport, err.Error())
				time.Sleep(config.RetryBackoff)
				return
			}
		}
	}
}

// runElectionHost runs an infinite loop of leader election on controller node.
// Only works when the node itself is controller, and there's no leader detected.
func (em *leaderElectionManager) runElectionHost() {
	for {
		// Wait until the node becomes controller, and the leader was found dead.
		ctrlInstance.condIsController.LoopWaitUntilTrue()
		ctrlInstance.condHasLeader.LoopWaitUntilFalse()

		// Start electing leader. If there's a network error, retry the loop.
		// Take a snapshot of online hostports. Assume they're still online inside the loop.
		var onlineHostports = ctrlInstance.getOnlineHostports()
		em.logger.Infof("Current onlineHostports: %v", onlineHostports)

		// Get a copy of ISR
		var isrSet = util.StringList2Set(ctrlInstance.replicationManager.cloneIsrList(true))
		var shouldConsiderIsr = len(isrSet) == 0 // if isr is empty, do not take isr into consideration.

		// Collecting other node's offset.
		var hostportAndOffsetList = make([]hostportAndOffset, 0, len(onlineHostports))
		var hostportAndOffsetListMu = sync.Mutex{}
		var lastestError error

		wg := sync.WaitGroup{}
		var rpcCollectOffset = func(hostport string) {
			defer wg.Done()
			if lastestError != nil {
				em.logger.Errorf("Encounter err while collecting offset, fail fast")
				return // fail fast
			}
			offset, err := ctrlInstance.rpcMgr.collectOffset(context.Background(), hostport)
			if err != nil {
				em.logger.Errorf("CollectOffset err: %s", err.Error())
				lastestError = err
				return
			}

			nodeName, _ := ctrlInstance.getNodeNameByHostport(hostport)
			if _, ok := isrSet[nodeName]; !ok && shouldConsiderIsr {
				em.logger.Errorf("Node<%s - %s> is not in isr, and isr is not empty", nodeName, hostport)
				return // not in isr, and isr set is not nil
			}

			hostportAndOffsetListMu.Lock()
			hostportAndOffsetList = append(hostportAndOffsetList, hostportAndOffset{
				hostport: hostport,
				offset:   offset,
			})
			hostportAndOffsetListMu.Unlock()
		}

		em.logger.Errorf("Broadcasting offset collect request...")
		for hostport := range onlineHostports {
			wg.Add(1)
			go rpcCollectOffset(hostport)
		}
		wg.Wait()

		if lastestError != nil {
			time.Sleep(config.RetryBackoff)
			continue // retry because there's an error
		}
		if len(hostportAndOffsetList) == 0 {
			em.logger.Errorf("hostportAndOffsetList length is 0, retry again...")
			time.Sleep(config.RetryBackoff)
			continue
		}

		// Sort offsets, and send leadership modification request to all active nodes.
		// If there's still any network error after several retry, redo the election loop again.
		sort.Slice(hostportAndOffsetList, func(i, j int) bool {
			elem1 := hostportAndOffsetList[i]
			elem2 := hostportAndOffsetList[j]
			return elem1.offset > elem2.offset || elem1.offset == elem2.offset && elem1.hostport < elem2.hostport
		})

		// First, handle leader situation.
		// Len(offset) should always > 0 here, because we've already handled Len(offset) = 0 situation above here.
		var leaderHostport = hostportAndOffsetList[0].hostport
		if err := util.RetryWithMaxCount(func() (bool, error) {
			return true, ctrlInstance.rpcMgr.roleChange(context.Background(), leaderHostport, proto.ServerRole_leader)
		}, config.RetryCount); err != nil {
			em.logger.Errorf("All retry for roleChange rpc on hostport %s has failed: ", leaderHostport, err.Error())
			time.Sleep(config.RetryBackoff)
			continue // re-exec the loop
		}

		// Second, handle follower situation.
		for _, tuple := range hostportAndOffsetList[1:] {
			followerHostport := tuple.hostport
			if err := util.RetryWithMaxCount(func() (bool, error) {
				return true, ctrlInstance.rpcMgr.roleChange(context.Background(), followerHostport, proto.ServerRole_follower)
			}, config.RetryCount); err != nil {
				em.logger.Errorf("All retry for roleChange rpc on hostport %s has failed: ", followerHostport, err.Error())
				time.Sleep(config.RetryBackoff)
				continue // re-exec the loop
			}
		}
	}
}

func (em *leaderElectionManager) becomeFollower() {
	em.logger.Infof("Node<%s-%s> has become follower", ctrlInstance.nodeName, ctrlInstance.getSelfHostport())
}

func (em *leaderElectionManager) becomeLeaderWithRetry() error {
	return util.RetryWithMaxCount(func() (bool, error) {
		ctrlHostport := ctrlInstance.getSelfHostport()
		dataHostport := ctrlInstance.getSelfDataHostport()
		if _, err := zkClient.Create(ZkPathLeaderNode, []byte(util.FormatZkLeader(ctrlInstance.nodeName, ctrlHostport, dataHostport)), zk.FlagEphemeral, zkPermAll); err != nil {
			if err == zk.ErrNodeExists {
				// This will happen when there's a fast-restart on leader. Check whether the leader is myself.
				zkLeader, err0 := zkGetLeader()
				if err0 != nil {
					return true, err0 // Neterr
				}
				if zkLeader.Name != ctrlInstance.nodeName {
					// Node is not myself. Don't think that's gonna happen.
					return false, fmt.Errorf("node is not myself, znode = %s", zkLeader.Name)
				}
				// Node is myself, jump to set leader.
			} else {
				return true, err // Neterr
			}
		}
		ctrlInstance.setCurrentLeaderName(ctrlInstance.nodeName)
		em.logger.Infof("Node<%s-%s> has become leader", ctrlInstance.nodeName, ctrlHostport)
		return false, nil
	}, config.RetryCount)
}
