package simplekv

import (
	"context"
	"fmt"
	"io"
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/util"
)

// --- Log Fetch Delayer ---
type logFetchDelayerItem struct {
	offset          int64
	timeout         *time.Timer
	timeoutMark     atomic.Value // struct{}
	completeChannel chan struct{}
}

// wait until getting notified or timeout.
// Returns true when getting notified, if there's a waiting timeout, returns false.
func (item *logFetchDelayerItem) wait() bool {
	defer item.timeout.Stop()
	select {
	case <-item.completeChannel:
		return true
	case <-item.timeout.C:
		item.timeoutMark.Store(struct{}{})
		return false
	}
}

type logFetchDelayer struct {
	mu            sync.Mutex
	pendingOffset []*logFetchDelayerItem // ordered array
	timeout       time.Duration
}

// newLogFetchDelayer returns a delayer, and boot up a timeout-purge thread.
func newLogFetchDelayer(purgeInterval time.Duration) *logFetchDelayer {
	ret := &logFetchDelayer{
		mu:            sync.Mutex{},
		pendingOffset: []*logFetchDelayerItem{},
		timeout:       time.Millisecond * 500, // TODO add to config
	}
	go ret.purgeTimeout(purgeInterval)
	return ret
}

func (ld *logFetchDelayer) purgeTimeout(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		if ctrlInstance.getIsShutingdown() {
			ctrlInstance.logger.Warnf("logFetchDelayer exiting...")
			break
		}
		select {
		case <-ticker.C:
			ld.internalPurgeTimeout()
		default:
		}
	}
}

// internalPurgeTimeout
func (ld *logFetchDelayer) internalPurgeTimeout() {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	var i = 0
	for i := range util.Iter(len(ld.pendingOffset)) {
		item := ld.pendingOffset[i]
		if item.timeoutMark.Load() == nil {
			break
		}
	}
	ld.pendingOffset = ld.pendingOffset[i:] // Trim to the limit.
}

// enqueue adds an offset into waiting queue in order.
func (ld *logFetchDelayer) enqueue(offset int64) *logFetchDelayerItem {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	origLen := len(ld.pendingOffset)
	// Search for insertion point and insert it.
	idx := sort.Search(origLen, func(i int) bool {
		return ld.pendingOffset[i].offset >= offset
	})
	entry := logFetchDelayerItem{
		offset:          offset,
		timeout:         time.NewTimer(ld.timeout),
		completeChannel: make(chan struct{}, 1),
	}
	if idx == origLen {
		// Not found
		ld.pendingOffset = append(ld.pendingOffset, &entry)
		return &entry
	}
	ld.pendingOffset = append(ld.pendingOffset, nil)
	copy(ld.pendingOffset[idx+1:], ld.pendingOffset[idx:origLen])
	ld.pendingOffset[idx] = &entry
	return &entry
}

// notifyAndDequeue make all request that has offset less or equals to curLeo return,
// and removes them from the queue as well.
func (ld *logFetchDelayer) notifyAndDequeue(curLeo int64) {
	ld.mu.Lock()
	defer ld.mu.Unlock()
	length := len(ld.pendingOffset)
	idx := sort.Search(length, func(i int) bool {
		return ld.pendingOffset[i].offset > curLeo
	})
	// Dequeue to index.
	for i := 0; i < idx; i++ {
		ld.pendingOffset[i].completeChannel <- struct{}{}
	}
	if idx < length {
		ld.pendingOffset = ld.pendingOffset[idx:]
	} else {
		ld.pendingOffset = make([]*logFetchDelayerItem, 0, length)
	}
}

func (ld *logFetchDelayer) shutdown() {
	ld.mu.Lock()
	for _, item := range ld.pendingOffset {
		item.timeoutMark.Store(struct{}{})
	}
	ld.mu.Unlock()
	ld.internalPurgeTimeout()
}

// --- Replication Manager ---

type nodeReplicationStatus struct {
	mu              sync.Mutex
	lastCatchupTime time.Time
	lastFetchTime   time.Time
	delayedOffset   int64
	logEndOffset    int64 // a remote record of followers' log end offset. This is used to update the high-watermark value.
}

type replicationManager struct {
	logger               *util.Logger
	rwmu                 sync.RWMutex                      // mainly protect isrSet
	isrList              []string                          // leader only
	nodeStatusMap        map[string]*nodeReplicationStatus // leader only
	sigkill              chan struct{}
	isrUpdateRoutineOnce *sync.Once

	logFetchDeposit *logFetchDelayer // blocks log fetch request when there's no more log to get.
}

func newReplicaManager(node2hostport map[string]string) *replicationManager {
	rm := &replicationManager{
		logger:               util.NewLogger("[ReplicaManager]", config.LogOutputWriter, config.EnableDebug),
		rwmu:                 sync.RWMutex{},
		isrList:              make([]string, 0, len(node2hostport)),
		nodeStatusMap:        make(map[string]*nodeReplicationStatus, len(node2hostport)),
		logFetchDeposit:      newLogFetchDelayer(config.ReplicationLogDelayerTimeout * 10), // TODO take this value from config
		sigkill:              make(chan struct{}, 1),
		isrUpdateRoutineOnce: &sync.Once{},
	}
	for nodeName := range node2hostport {
		rm.nodeStatusMap[nodeName] = &nodeReplicationStatus{}
	}
	return rm
}

func (r *replicationManager) shutdown() {
	r.sigkill <- struct{}{}
	r.logFetchDeposit.shutdown()
}

func (r *replicationManager) onReceiveCollectWatermarkRequest() (int64, error) {
	if !ctrlInstance.isLeader() {
		return 0, config.ErrNotLeader
	}
	return dataInstance.vars.ReadWatermarkFromCache(), nil
}

// isIsr returns whether the nodename is in isr set.
func (r *replicationManager) isIsr(nodeName string, needLock bool) bool {
	if needLock {
		r.rwmu.RLock()
		defer r.rwmu.RUnlock()
	}
	return util.StringListContains(r.isrList, nodeName)
}

// cloneIsrSet gets an isr set copy.
// Notice, this isr set copy does not include leader itself.
func (r *replicationManager) cloneIsrList(needLock bool) []string {
	if needLock {
		r.rwmu.RLock()
		defer r.rwmu.RUnlock()
	}
	ret := make([]string, len(r.isrList))
	copy(ret, r.isrList)
	return ret
}

// Make sure to use this readonly.
func (r *replicationManager) getIsrList() []string {
	r.rwmu.RLock()
	defer r.rwmu.RUnlock()
	return r.isrList
}

// Overwrite old isrSet.
func (r *replicationManager) setNewIsrAndCache(newIsrList []string) {
	r.logger.Infof("attempting to set newIsrList to %v", newIsrList)
	util.RetryInfinite(func() error {
		return zkSetIsr(newIsrList)
	}, config.RetryBackoff)

	r.rwmu.Lock()
	defer r.rwmu.Unlock()
	r.isrList = newIsrList
	r.logger.Infof("isr set Ok")
}

// updateWatermark updates self watermark.
// This should be processed sequentially by commit a request to commitMgr first.
func (r *replicationManager) updateWatermark() {
	clonedIsrList := r.getIsrList()
	clonedIsrSet := util.StringList2Set(clonedIsrList)
	toUpdateHw := dataInstance.vars.ReadWatermarkFromCache()
	for nodeName, eachStat := range r.nodeStatusMap {
		if _, nodeIsIsr := clonedIsrSet[nodeName]; nodeIsIsr {
			if eachStat.logEndOffset < toUpdateHw {
				toUpdateHw = eachStat.logEndOffset
			}
		}
	}
	util.MustDo(func() error { return dataInstance.vars.OverwriteWatermark(toUpdateHw) })
}

// updateNodeStatus will be trigerred when received fetch request from other nodes.
// This should be processed sequentially by calling this funtion in commitManager handler.
func (r *replicationManager) updateNodeStatus(nodeName string, reqOffset int64) error {
	status, ok := r.nodeStatusMap[nodeName]
	if !ok {
		return fmt.Errorf("nodeName %s not exist", nodeName)
	}
	// Update statistics
	status.mu.Lock()
	status.lastFetchTime = time.Now()
	hwm := dataInstance.vars.ReadWatermarkFromCache()
	status.delayedOffset = hwm - reqOffset
	if status.delayedOffset <= 0 {
		status.lastCatchupTime = time.Now()
	}
	status.mu.Unlock()

	// Update high-watermark. LeadHW = min(allIsrLEO)
	toUpdateHw := int64(math.MaxInt64)
	for _, eachStat := range r.nodeStatusMap {
		if eachStat.logEndOffset < toUpdateHw {
			toUpdateHw = eachStat.logEndOffset
		}
	}
	if toUpdateHw != math.MaxInt64 {
		util.MustDo(func() error { return dataInstance.vars.OverwriteWatermark(toUpdateHw) })
	}
	return nil
}

// Isr is updated in a fixed interval by a scheduled goroutine.
func (r *replicationManager) processTryUpdateIsr() {
	// Warning: needs to maintain Isr order unchanged !
	oldIsrList := r.getIsrList()
	oldIsrSet := util.StringList2Set(oldIsrList)
	newIsrList := make([]string, len(oldIsrList))
	copy(newIsrList, oldIsrList)

	hasModified := false
	for nodeName, status := range r.nodeStatusMap {
		r.logger.Debugf("Iterating node %s...", nodeName)
		// Try modify ISR set.
		// Test whether the node has been out of sync.
		// Leader it self should always be inside Isr
		isLeaderItself := nodeName == ctrlInstance.nodeName
		if isLeaderItself {
			_, ok := oldIsrSet[nodeName]
			if !ok {
				// Leader is not inside Isr, add it !
				newIsrList = append(newIsrList, nodeName)
				hasModified = true
				r.logger.Infof("Node %s is Leader but not inside Isr, attempting to add it to isr", nodeName)
			}
			continue
		}

		// Consider follower nodes then.
		_, isNodeIsr := oldIsrSet[nodeName]
		if isNodeIsr {
			isOutOfSync := false
			if status.delayedOffset > int64(config.ReplicationIsrMaxDelayCount) {
				r.logger.Infof("Node %s is out of sync because its delayedOffset %d has reached max delay tolerance %d", nodeName, status.delayedOffset, config.ReplicationIsrMaxDelayCount)
				isOutOfSync = true
			} else if time.Since(status.lastFetchTime) > config.ReplicationIsrMaxNoFetchTime {
				r.logger.Infof("Node %s is out of sync because it didn't receive fetch request in %v since %v", nodeName, config.ReplicationIsrMaxNoFetchTime, status.lastFetchTime)
				isOutOfSync = true
			} else if time.Since(status.lastCatchupTime) > config.ReplicationIsrMaxCatchUpTime {
				r.logger.Infof("Node %s is out of sync because it didn't catch up leader in %v since %v", nodeName, config.ReplicationIsrMaxCatchUpTime, status.lastCatchupTime)
				isOutOfSync = true
			}
			if isOutOfSync {
				// Remove from ISR cache and update ZK.
				newIsrList = util.StringListDelete(newIsrList, nodeName)
				hasModified = true
			}
		} else {
			// Test whether the node can be added to ISR.
			cond1 := status.delayedOffset < int64(config.ReplicationIsrMaxDelayCount)
			cond2 := time.Since(status.lastFetchTime) < config.ReplicationIsrMaxNoFetchTime
			cond3 := time.Since(status.lastCatchupTime) < config.ReplicationIsrMaxCatchUpTime
			r.logger.Debugf("Node %s's delayedOffset = %d, require = %d", nodeName, status.delayedOffset, config.ReplicationIsrMaxDelayCount)
			r.logger.Debugf("Node %s's lastFetchTime = %d, delta = %d ms, require < %d ms", nodeName, status.lastFetchTime.Unix(), time.Since(status.lastFetchTime).Milliseconds(), config.ReplicationIsrMaxNoFetchTime.Milliseconds())
			r.logger.Debugf("Node %s's lastCatchupTime = %d, delta = %d ms, require < %d ms", nodeName, status.lastFetchTime.Unix(), time.Since(status.lastCatchupTime).Milliseconds(), config.ReplicationIsrMaxCatchUpTime.Milliseconds())
			if cond1 && cond2 && cond3 {
				r.logger.Infof("Node %s has caught up to the leader, attempting to add it back to isr", nodeName)
				// Add back to ISR.
				newIsrList = append(newIsrList, nodeName)
				hasModified = true
			}
		}
	}
	// We have to persist it to zookeeper, if any modification is detected.
	if hasModified {
		r.logger.Infof("ISR has been modified")
		r.setNewIsrAndCache(newIsrList)
		if err := ctrlInstance.commitMgr.enqueueIsrSetUpdated(); err != nil {
			r.logger.Errorf("Enqueue isrSet updated err: %s", err.Error()) // Most likely channel closed error
		}
		r.logger.Infof("Current ISR = %v", newIsrList)
	}
}

// Boot isrUpdateRoutine once.
func (r *replicationManager) tryBootIsrUpdateRoutine(interval time.Duration) {
	r.isrUpdateRoutineOnce.Do(func() {
		go r.isrUpdateRoutine(interval)
	})
}

// Do not call this directly.
func (r *replicationManager) isrUpdateRoutine(checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	for {
		select {
		case <-r.sigkill:
			r.logger.Infof("IsrUpdateRoutine is shutdown")
			return
		default:
			break
		}
		<-ticker.C
		ctrlInstance.cmdExecutor.enqueueTryAlterIsr()
	}
}

// onReceiveLogFetch will be handled when there's an log fetch request.
func (r *replicationManager) onReceiveLogFetchRequest(nodeName string, fromOffset int64, count int64) (ret [][]byte, leaderEpoch int64, err error) {

	totalOffsetSnapshot := dataInstance.totalOffset()

	r.logger.Debugf("LogFetchRequest received from %s, params: fromOffset = %d. Leader's log totalOffset = %d", nodeName, fromOffset, totalOffsetSnapshot)
	// If not leader, cannot respond.
	if !ctrlInstance.isLeader() {
		r.logger.Errorf("Refuse to serve data because I'm not leader")
		return nil, 0, config.ErrNotLeader
	}

	// Try to commit an uncommited offset.
	ctrlInstance.commitMgr.enqueueReceiveLogFetchReqEvent(nodeName, fromOffset)

	leaderEpoch = int64(ctrlInstance.getLeaderEpoch())
	// If there's no more logs to read, append to waiting pool.
	if fromOffset >= totalOffsetSnapshot {
		entry := ctrlInstance.replicationManager.logFetchDeposit.enqueue(fromOffset)
		if !entry.wait() {
			// Timeout, returns nil data.
			r.logger.Debugf("node %s's log fetch request from %d to %d was timeout", nodeName, fromOffset, fromOffset+count)
			return nil, leaderEpoch, nil
		}
		// Else, log has been updated, try to fetch log.
	}

	ret, err = dataInstance.readEntries(fromOffset, count)
	if err != nil {
		if err == io.EOF || err == config.ErrFileNotFound || err == config.ErrRecordNotFound {
			err = nil
		} else {
			r.logger.Warnf("readEntries from offset = %d with count = %d contain err: %s", fromOffset, count, err.Error())
		}
	}
	return
}

// logSyncRoutine send fetch request to leader and append logs to its local storage.
// Only work on follower.
func (r *replicationManager) logSyncRoutine() {
	var fetchAmount int64 = 1024 // TODO change this value
	ticker := time.NewTicker(config.ReplicationLogFetchInterval)
	for {
		<-ticker.C
		ctrlInstance.condIsFollower.LoopWaitUntilTrue()
		ctrlInstance.condHasLeader.LoopWaitUntilTrue()
		r.logger.Debugf("Initiating log fetch process")
		leaderHostport, ok := ctrlInstance.currentLeaderHostport()
		if !ok {
			r.logger.Errorf("Failed to find leader hostport, retry for another round...")
			continue
		}
		logs, leaderHw, err := ctrlInstance.rpcMgr.fetchLog(context.Background(), leaderHostport, dataInstance.totalOffset(), fetchAmount)
		if err != nil {
			if config.IsCriticalErr(err) {
				panic(fmt.Sprintf("Critical error detected: %s", err.Error()))
			}
			r.logger.Errorf("FetchLog err: %s, wait till next round", err.Error())
			continue
			// Do nothing and wait for the next round
		}
		for _, eachLog := range logs {
			// Validate log integrity
			e := dbfile.Decode(eachLog)

			// Write to disk
			f := dataInstance.getNextActiveFile(eachLog)
			infileBeforeWriteOffset := f.Offset()
			if err := f.Write(eachLog); err != nil {
				panic(fmt.Sprintf("Persist log err: %s. This can be critical.", err.Error()))
			}
			r.logger.Debugf("Log persist ok, KV = <%s, %s>", e.Key, e.Value)

			// Should appy the log to memory
			applyEntry(e, infileBeforeWriteOffset, f)
		}
		// Update high-watermark, hw = min(LogEndOffset, leaderHw)
		updateHw := dataInstance.totalOffset()
		if updateHw > leaderHw {
			updateHw = leaderHw
		}
		r.logger.Debugf("Trying to update my HW to %d", updateHw)
		if err = dataInstance.vars.OverwriteWatermark(updateHw); err != nil {
			r.logger.Errorf("Overwrite high-watermark failed: %s. This can be critical.", err)
		}
		r.logger.Debugf("Trying to update my HW to %d success", updateHw)
	}
}

// initIsr pulls isr from zookeeper with retry. If failed too many times, will panic.
func (r *replicationManager) initIsr() {
	if err := util.RetryWithMaxCount(func() (bool, error) {
		isr, err0 := zkGetIsr()
		if zkNodeNotExistErr(err0) {
			isr = make([]string, 0)
		} else if err0 != nil {
			r.logger.Errorf("zkGetIsr failed: %s", err0)
			return true, err0
		}
		r.isrList = isr
		return false, nil
	}, config.RetryCount); err != nil {
		panic(err)
	}
}

func (r *replicationManager) run() {
	// Cache ISR from zookeeper with retry.
	r.initIsr()
	// Begin fetching and applying logs. This will only work on follower node.
	go r.logSyncRoutine()
}
