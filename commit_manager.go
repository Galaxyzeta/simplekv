package simplekv

import (
	"fmt"
	"time"

	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
	"github.com/galaxyzeta/simplekv/util"
)

// +---------------------+
// |   Data Structures   |
// +---------------------+

// commitManagerEventType indicates the type difference between each
// awaiting event.
type commitManagerEventType uint8

const (
	commitManagerEvent_AddWaitingEntry commitManagerEventType = iota
	commitManagerEvent_RecvLogFetch
	commitManagerEvent_UpdateHw
)

// commitManagerEvent represents the job that is waiting to get executed.
type commitManagerEvent struct {
	evType  commitManagerEventType
	payload interface{}
}

// commitEntry is a single unit in a waiting queue.
type commitEntry struct {
	offset        int64               // immutable after initialization.
	ackedNodeName map[string]struct{} // can only be r/w by one thread.
	completeCh    chan struct{}
	timestamp     time.Time // used for debugging only
	timeout       *time.Timer
	nextEntry     *commitEntry // point to the next entry in the queue.
}

// waitUntilComplete waits for an entry to be fully acked before timeout.
// Returns false if timeout, otherwise returns true.
func (e *commitEntry) waitUntilComplete() error {
	defer e.timeout.Stop()
	select {
	case <-e.completeCh:
		return nil
	case <-e.timeout.C:
		return config.ErrTimeout
	}
}

// complete the operation reprensted by the entry by putting a struct{} signal to the channel.
func (e *commitEntry) complete() {
	e.completeCh <- struct{}{}
}

// tryComplete evaluates whether the entry completion conditon can be
// satisfied, if so, complete it.
// Returns whether the entry has completed.
func (e *commitEntry) tryComplete() bool {
	if e.canBeComplete() {
		e.complete()
		return true
	}
	return false
}

// canBeComplete checks whether the entry has satisfied given complete condition.
func (e *commitEntry) canBeComplete() bool {
	return len(e.ackedNodeName) >= config.CommitMaxAck-1
}

// commitAck represents an ack that was recevied from log fetch request.
type commitAck struct {
	offset   int64
	nodeName string
}

// String stringifys an commitAck in form of [nodeName - offset].
func (e *commitAck) String() string {
	return fmt.Sprintf("[%s - %d]", e.nodeName, e.offset)
}

// updateWatermarkRequest is an empty struct that works as a
// signal to trigger an update on self high-watermark.
type updateWatermarkRequest struct{}

// +---------------------+
// |   Crucial Structs   |
// +---------------------+

// commitManager handles all uncommitted requests.
type commitManager struct {
	eventChannel chan commitManagerEvent
	hashQueue
	logger *util.Logger
}

// hashQueue consists of a queue and a hashmap that each key points to an entry in the queue.
type hashQueue struct {
	queue []*commitEntry
	hash  map[int64]*commitEntry
}

func newCommitManager() *commitManager {
	return &commitManager{
		eventChannel: make(chan commitManagerEvent),
		hashQueue:    hashQueue{queue: make([]*commitEntry, 0, config.CommitInitQueueSize), hash: make(map[int64]*commitEntry, config.CommitInitQueueSize)},
		logger:       util.NewLogger("[CommitManager]", config.LogOutputWriter),
	}
}

// addToWaitingQueue enqueues an offset to the queue, return entry as future completable object.
func (cm *commitManager) addToWaitingQueue(offset int64, entry dbfile.Entry, timeout time.Duration) *commitEntry {
	e := &commitEntry{
		offset:        offset,
		ackedNodeName: make(map[string]struct{}, ctrlInstance.getClusterSize()),
		completeCh:    make(chan struct{}, 1),
		timeout:       time.NewTimer(timeout),
	}
	// @Debugging
	if config.EnableTimeEstimate {
		e.timestamp = time.Now()
	}
	cm.eventChannel <- commitManagerEvent{
		evType:  commitManagerEvent_AddWaitingEntry,
		payload: e,
	}
	return e
}

func (cm *commitManager) enqueueReceiveLogFetchReqEvent(nodeName string, offset int64) {
	cm.eventChannel <- commitManagerEvent{
		evType: commitManagerEvent_RecvLogFetch,
		payload: commitAck{
			offset:   offset,
			nodeName: nodeName,
		},
	}
}

// try to complete the first element in the queue, if it is completed, dequeue it and perform check again.
func (cm *commitManager) tryCompleteFromWaitingQueue() {
	for {
		if len(cm.queue) == 0 {
			return
		}
		first := cm.queue[0]
		if first.tryComplete() {
			// @Debugging
			if config.EnableTimeEstimate {
				cm.logger.Debugf("commit wait time delta = %d us", time.Since(first.timestamp).Microseconds())
			}
			cm.logger.Infof("offset = %d is considered commited", first.offset)
			cm.queue = cm.queue[1:]
			delete(cm.hash, first.offset)
			continue // do another round of check
		}
		return // cannot complete the first element, quit checking
	}
}

// --- handlers ---

// handleLogFetchAck will be called when a log fetch request arrives.
// All offsets less than ack.offset are acked.
func (cm *commitManager) handleLogFetchAck(ack commitAck) {
	ctrlInstance.replicationManager.updateNodeStatus(ack.nodeName, ack.offset)
	// The elements in the queue are always in ascend order.
	// Keep acking until meeting bigger offset.
	cm.logger.Debugf("Received log fetch ack, offset = %d", ack.offset)
	var shouldTryComplete = false
	var requiredCommits = config.CommitMaxAck - 1
	for _, entry := range cm.queue {
		if entry.offset >= ack.offset {
			break
		}
		entry.ackedNodeName[ack.nodeName] = struct{}{}
		if len(entry.ackedNodeName) >= requiredCommits {
			shouldTryComplete = true
		}
	}
	// Try to complete the first queueing element.
	if shouldTryComplete {
		cm.tryCompleteFromWaitingQueue()
	}
	cm.logger.Debugf("Handle log fetch OK")
}

// hanldeAddWaitingEntry will be called when a writing request comes from client.
func (cm *commitManager) handleAddWaitingEntry(e *commitEntry) {
	cm.queue = append(cm.queue, e)
	lenAfterAppend := len(cm.queue)
	if lenAfterAppend > 1 {
		cm.queue[lenAfterAppend-1].nextEntry = e
	}
	cm.hash[e.offset] = e
}

// handleUpdateWatermarkRequest after a writing future is fulfilled.
func (cm *commitManager) handleUpdateWatermarkRequest() {
	ctrlInstance.replicationManager.updateWatermark()
}

// --- run ---

// run will boot up event dispatcher and process commit events continuously.
func (cm *commitManager) run() {
	cm.logger.Infof("running...")
	for {
		// Serialize the operations by introducing a channel, thus no need to add mutex on any elements.
		// Because of temporal relationships, add operation of a certain offset must occur before receiving an ack.
		switch ev := <-cm.eventChannel; ev.evType {
		case commitManagerEvent_AddWaitingEntry:
			cm.handleAddWaitingEntry(ev.payload.(*commitEntry))
		case commitManagerEvent_RecvLogFetch:
			cm.handleLogFetchAck(ev.payload.(commitAck))
		case commitManagerEvent_UpdateHw:
			cm.handleUpdateWatermarkRequest()
		}
	}
}
