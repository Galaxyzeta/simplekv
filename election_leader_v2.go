package simplekv

import (
	"fmt"
	"sort"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/util"
)

type leaderElectionManagerV2 struct {
	logger *util.Logger
}

func newLeaderElectionManagerV2() *leaderElectionManagerV2 {
	return &leaderElectionManagerV2{
		logger: util.NewLogger("[LeaderElecMgrV2]", config.LogOutputWriter),
	}
}

func (em *leaderElectionManagerV2) logErrAndBackoff(reason string) {
	em.logger.Errorf(reason)
	zkRetryBackoff()
}

func (em *leaderElectionManagerV2) logInfoAndBackoff(reason string) {
	em.logger.Infof(reason)
	zkRetryBackoff()
}

func (em *leaderElectionManagerV2) startUp() {
	em.registerNodeWithInfiniteRetry()
	ctrlInstance.cmdExecutor.enqueueLeaderElection()
}

// processElect will be invoked by event polling from cmdExecutor.
func (em *leaderElectionManagerV2) processElect() {
	for {
		leaderData, err := zkGetLeader()
		hasLeader := true
		if err != nil {
			if zkNodeNotExistErr(err) {
				hasLeader = false
			} else {
				em.logErrAndBackoff(err.Error())
				continue
			}
		}
		if hasLeader {
			if leaderData.Name == ctrlInstance.nodeName {
				var validIsrList []string
				validIsrList, err = em.fetchValidIsrList()
				em.logger.Infof("I am still leader after a short disconnection")
				em.becomeLeaderWithValidIsr(validIsrList)
				return
			} else {
				// If there's an existing leader, just set current leader name and we're finished.
				em.logger.Infof("Trying to become follower since there's already a leader, current leader is %s", leaderData.Name)
				em.becomeFollower(leaderData.Name)
				return
			}
		} else {
			// Begin leader election logic.
			// 1. Retrieve ISRs and all living brokers from zookeeper, assume they stay the same during leader election.
			//
			// 2.1 If there's no ISR entry on zookeeper, fallback to first-come-frist-win strategy.
			// 2.2 Else there's a ISR entry.
			// 2.2.1 Check if we are the first living node in ISR list, if so we win the leader election.
			// 2.2.2 If not, backoff and restart the whole loop.

			// Check whether ISR node exists, or it has zero length.
			var isrList []string
			var nodeExist bool = true
			isrList, err = zkGetIsr()
			if err != nil {
				if zkNodeNotExistErr(err) {
					nodeExist = false
				} else {
					em.logErrAndBackoff(fmt.Sprintf("zkGetIsr err: %s", err.Error()))
					continue
				}
			}
			if !nodeExist || len(isrList) == 0 {
				// 2.1 First comer wins strategy
				if em.trySetLeaderOrBackoff() {
					em.logger.Infof("I won leader election since I'm the fastest node (no ISR)")
					em.becomeLeaderWithNoISR()
					break
				}
				// If not ok, retry immediately to fetch leader and become follower.
			} else if err != nil {
				em.logErrAndBackoff(fmt.Sprintf("zkGetIsr err: %s", err.Error()))
			} else {
				// 2.2 First living ISR wins
				var validIsrList []string
				validIsrList, err = em.fetchValidIsrList()
				if err != nil {
					em.logErrAndBackoff(fmt.Sprintf("fetchValidIsrList err: %s", err.Error()))
					continue
				}
				em.logger.Infof("ValidIsrList = %s", validIsrList)
				if !util.StringListContains(validIsrList, ctrlInstance.nodeName) {
					em.logInfoAndBackoff("Not inside ISR, waiting for a valid leader")
					continue
				}
				if validIsrList[0] == ctrlInstance.nodeName {
					if em.trySetLeaderOrBackoff() {
						em.logger.Infof("I won leader election since I'm the first living node in ISR")
						em.becomeLeaderWithValidIsr(validIsrList)
						break
					}
				} else {
					em.logInfoAndBackoff("Not first living node, waiting for a valid leader")
					continue
				}
			}
		}
	}
}

// If no ISR node path is in ZK, try fill it with all living nodes.
func (em *leaderElectionManagerV2) becomeLeaderWithNoISR() {
	// Fetch living nodes.
	var livingNodes []string
	var err error
	util.RetryInfinite(func() error {
		livingNodes, err = em.fetchLivingNodesAndCheck(true)
		return err
	}, config.RetryBackoff)

	em.becomeLeaderWithValidIsr(livingNodes)
}

func (em *leaderElectionManagerV2) errorOmitWrapper(fn func() error) func() {
	return func() {
		if err := fn(); err != nil {
			em.logger.Errorf("error while calling %s: %s", fn, err)
		}
	}
}

// This method is triggered when a new leader come to power.
func (em *leaderElectionManagerV2) becomeLeaderWithValidIsr(validIsrList []string) {
	ctrlInstance.replicationManager.setNewIsrAndCache(validIsrList)

	newLeaderEpoch := zkIncreLeaderEpochInfiniteRetry()
	em.persistLeaderEpochAndLogOffset(newLeaderEpoch)

	enqueueNodeConnectionChangedWrapper := em.errorOmitWrapper(ctrlInstance.cmdExecutor.enqueueNodeConnectionChanged)
	enqueueLeaderElectionWrapper := em.errorOmitWrapper(ctrlInstance.cmdExecutor.enqueueLeaderElection)

	zkMonitorChildren(ZkPathNodeConnection, enqueueNodeConnectionChangedWrapper) // Watch on connection folder
	zKMonitorNodeLost(ZkPathLeaderNode, enqueueLeaderElectionWrapper)

	go ctrlInstance.replicationManager.isrUpdateRoutine(config.ReplicationIsrUpdateInterval) // Regularly update Isr

	ctrlInstance.setCurrentLeaderName(ctrlInstance.nodeName)
}

func (em *leaderElectionManagerV2) becomeFollower(leaderName string) {
	enqueueLeaderElectionWrapper := em.errorOmitWrapper(ctrlInstance.cmdExecutor.enqueueLeaderElection)
	zKMonitorNodeLost(ZkPathLeaderNode, enqueueLeaderElectionWrapper)
	ctrlInstance.setCurrentLeaderName(leaderName)
}

// processNodeConnectionChanged is invoked via event queue in controller.
func (em *leaderElectionManagerV2) processNodeConnectionChanged() {
	oldLivingNodes := ctrlInstance.getOnlineNodeNames()
	newLivingNodes := util.StringList2Set(em.fetchLivingNodesAndCheckWithInfiniteRetry(true))
	toRemoveNodes := util.StringSetSubtract(oldLivingNodes, newLivingNodes)
	toAddNodes := util.StringSetSubtract(newLivingNodes, oldLivingNodes)
	for nodeName := range toRemoveNodes {
		ctrlInstance.markOffline(nodeName)
	}
	for nodeName := range toAddNodes {
		ctrlInstance.markOnline(nodeName)
	}
	em.logger.Infof("OldLivingNodes = %s", oldLivingNodes)
	em.logger.Infof("NewLivingNodes = %s", newLivingNodes)
	if len(toRemoveNodes) > 0 {
		em.logger.Infof("Nodes = %s are offlined", toRemoveNodes)
	}
	if len(toAddNodes) > 0 {
		em.logger.Infof("Nodes = %s are onlined", toAddNodes)
	}
}

// Fetch living nodes and check whether there's any critical error.
func (em *leaderElectionManagerV2) fetchLivingNodesAndCheck(checkSafety bool) (livingNodes []string, err error) {
	livingNodes, err = zkGetLivingNodes()
	if err != nil {
		em.logErrAndBackoff(fmt.Sprintf("zkGetLivingNodes err: %s", err.Error()))
		return livingNodes, err
	}
	if len(livingNodes) == 0 {
		em.logger.Fatalf("no living nodes were found, this should not happen because I have already registered myself to living nodes list.")
	}
	if !util.StringListContains(livingNodes, ctrlInstance.nodeName) {
		em.logger.Fatalf("myself is not in living nodes, this should not happen since I have already registered myself to living node s list.")
	}
	return livingNodes, err
}

// Infinite retry version of fetchLivingNodesAndCheck
func (em *leaderElectionManagerV2) fetchLivingNodesAndCheckWithInfiniteRetry(checkSafety bool) (livingNodes []string) {
	var err error
	util.RetryInfinite(func() error {
		livingNodes, err = em.fetchLivingNodesAndCheck(true)
		return err
	}, config.RetryBackoff)
	return
}

func (em *leaderElectionManagerV2) fetchValidIsrList() ([]string, error) {
	// Fetch ISR list
	isrList, err := zkGetIsr()
	// Fetch living nodes and do safety check
	livingNodes, err := em.fetchLivingNodesAndCheck(true)
	if err != nil {
		em.logErrAndBackoff(fmt.Sprintf("fetchLivingNodesAndCheck failed: %s", err.Error()))
	}
	// Determine whether I'm the first living node in ISR
	livingNodesSet := util.StringList2Set(livingNodes)
	validIsrList := []string{}
	for _, each := range isrList {
		if _, ok := livingNodesSet[each]; !ok {
			continue
		} else {
			validIsrList = append(validIsrList, each)
		}
	}
	return validIsrList, nil
}

func (em *leaderElectionManagerV2) trySetLeaderOrBackoff() bool {
	var ok, err = zkTrySetLeader(util.MakeZkLeader(ctrlInstance.nodeName, ctrlInstance.getSelfHost(), ctrlInstance.getSelfDataHostport()))
	if err != nil {
		em.logErrAndBackoff(fmt.Sprintf("zkTrySetLeader err: %s", err.Error()))
		return false
	} else if ok {
		return true
	} else {
		em.logInfoAndBackoff("try set leader failed because there's already a leader exists")
		return false
	}
}

func (em *leaderElectionManagerV2) registerNodeWithInfiniteRetry() {
	util.RetryInfinite(func() error {
		err := zkRegisterNode(ctrlInstance.nodeName)
		if err != nil && !zkNodeExistErr(err) {
			return err
		}
		return nil
	}, config.RetryBackoff)
}

func (em *leaderElectionManagerV2) persistLeaderEpochAndLogOffset(leaderEpoch int64) {
	dataInstance.vars.AppendLeaderEpochAndOffsetVector(leaderEpoch, dataInstance.totalOffset())
}

func (em *leaderElectionManagerV2) onReceiveGetLeaderEpochAndLogOffsetRequest(leaderEpoch int64) (dbfile.LeaderEpochAndOffset, error) {
	// If not leader, cannot respond.
	if !ctrlInstance.isLeader() {
		em.logger.Errorf("Refuse to serve data because I'm not leader")
		return dbfile.LeaderEpochAndOffset{}, config.ErrNotLeader
	}
	// Find entry that has equal or just bigger leader epoch number by given leader epoch.
	leaderEpochAndOffset := dataInstance.vars.CloneLeaderEpochAndOffset()
	index := sort.Search(len(leaderEpochAndOffset), func(i int) bool {
		return leaderEpochAndOffset[i].LeaderEpoch >= leaderEpoch
	})
	if index == len(leaderEpochAndOffset) {
		em.logger.Warnf("No suitable leader epoch is found when handling getLeaderEpochAndOffsetRequest. Current leader epoch list is %s", leaderEpochAndOffset)
		if !dataInstance.vars.ValidateLeaderEpochAndOffset() {
			em.logger.Fatalf("leader epoch list is out of order !")
		} // Do a validate
		return dbfile.LeaderEpochAndOffset{}, fmt.Errorf("no leader epoch was found")
	}
	return leaderEpochAndOffset[index], nil
}
