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
		logger: util.NewLogger("[LeaderElecMgrV2]", config.LogOutputWriter, config.EnableDebug),
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
		if ctrlInstance.getIsShutingdown() {
			em.logger.Warnf("Exiting leader election: shutdown detected")
			break
		}
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
				// SetLeadrWithInfiniteRery: This prevents the situation when the leader
				// node recovers from a short disconnection, meanwhile detects leader node lost since the session
				// is no longer exist after the crash of previous application which wrote the node got crashed. Although
				// the leader logically stays unchanged, the node who actually wrote the /leader node has crashed, causing
				// the session probable to vanish anytime before setting current leader name again.
				em.resetLeaderWithInfiniteRetry()
				em.becomeLeaderWithValidIsr(validIsrList, false)
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
						em.becomeLeaderWithValidIsr(validIsrList, true)
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

	em.becomeLeaderWithValidIsr(livingNodes, true)
}

func (em *leaderElectionManagerV2) errorOmitWrapper(fn func() error) func() {
	return func() {
		if err := fn(); err != nil {
			em.logger.Errorf("error while calling %s: %s", fn, err)
		}
	}
}

// This method is triggered when a new leader come to power.
func (em *leaderElectionManagerV2) becomeLeaderWithValidIsr(validIsrList []string, isNewLeader bool) {
	ctrlInstance.replicationManager.setNewIsrAndCache(validIsrList)

	var currentLeaderEpoch int64
	if isNewLeader {
		newLeaderEpoch := zkIncreLeaderEpochInfiniteRetry()
		em.persistLeaderEpochAndLogOffset(newLeaderEpoch)
		em.logger.Infof("leaderEpoch updated to %d", newLeaderEpoch)
		currentLeaderEpoch = newLeaderEpoch
	} else {
		var lep int64
		var err error
		util.RetryInfinite(func() error {
			lep, err = zkGetLeaderEpoch()
			if err != nil {
				return err
			}
			return nil
		}, config.RetryBackoff)
		currentLeaderEpoch = lep
		em.logger.Infof("leaderEpoch is %d. No leaderEpoch update.", lep)
	}
	em.logger.Infof("set leaderEpoch to %d", int(currentLeaderEpoch))
	ctrlInstance.setLeaderEpoch(int(currentLeaderEpoch)) // NOTICE: must update leader epoch

	enqueueNodeConnectionChangedWrapper := em.errorOmitWrapper(ctrlInstance.cmdExecutor.enqueueNodeConnectionChanged)
	enqueueLeaderElectionWrapper := em.errorOmitWrapper(ctrlInstance.cmdExecutor.enqueueLeaderElection)

	zkMonitorChildren(ZkPathNodeConnection, enqueueNodeConnectionChangedWrapper) // Watch on connection folder
	zKMonitorNodeLost(ZkPathLeaderNode, enqueueLeaderElectionWrapper, true)

	ctrlInstance.setCurrentLeaderName(ctrlInstance.nodeName)
	ctrlInstance.replicationManager.tryBootIsrUpdateRoutine(config.ReplicationIsrUpdateInterval) // Regularly update Isr
}

func (em *leaderElectionManagerV2) becomeFollower(leaderName string) {
	enqueueLeaderElectionWrapper := em.errorOmitWrapper(ctrlInstance.cmdExecutor.enqueueLeaderElection)
	zKMonitorNodeLost(ZkPathLeaderNode, enqueueLeaderElectionWrapper, true)
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

func (em *leaderElectionManagerV2) resetLeaderWithInfiniteRetry() {
	zkForceDeleteNodeWithInfiniteRetry(ZkPathLeaderNode)
	zkSetLeaderInfiniteRetry(util.MakeZkLeader(ctrlInstance.nodeName, ctrlInstance.getSelfHost(), ctrlInstance.getSelfDataHostport()))
}

// Delete the old node if it exists, then create a new node and put it under living node directory.
func (em *leaderElectionManagerV2) registerNodeWithInfiniteRetry() {
	path := zkGetLivingNodeFullPath(ctrlInstance.nodeName)
	zkForceDeleteNodeWithInfiniteRetry(path)
	zkRegisterNodeWithInfiniteRetry(path)
	zkUtilLogger.Infof("Registering %s to znode OK", path)
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
	var ld = leaderEpochAndOffset[index]
	em.logger.Infof("returning leaderEpoch = %d, offset = %d", ld.LeaderEpoch, ld.Offset)
	return leaderEpochAndOffset[index], nil
}
