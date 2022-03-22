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

type commitAckType int8

const (
	commitAck_None   = 0
	commitAck_One    = 1
	commitAck_AllIsr = -1
)

// commitManagerEventType indicates the type difference between each
// awaiting event.
type commitManagerEventType uint8

const (
	commitManagerEvent_AddWaitingEntry commitManagerEventType = iota
	commitManagerEvent_RecvLogFetch
	commitManagerEvent_IsrSetUpdated
	commitManagerEvent_Shutdown
)

// commitManagerEvent represents the job that is waiting to get executed.
type commitManagerEvent struct {
	evType  commitManagerEventType
	payload interface{}
}

type commitEntryStateType uint8

const (
	commitEntryStateType_Normal commitEntryStateType = iota
	commitEntryStateType_Timeout
	commitEntryStateType_Canceled
)

// commitEntry is a single unit in a waiting queue.
// use this internally.
type commitEntry struct {
	offset                int64 // immutable after initialization.
	entryLength           int64
	requiredAcksToRespond int                 // how many acks is needed to respond a write OK. Note this can be -1 which means require to get replicated to all ISR
	ackedNodeName         map[string]struct{} // can only be r/w by one thread.
	completeCh            chan error
	timestamp             time.Time // used for debugging only
	timeout               *time.Timer
	nextEntry             *commitEntry // point to the next entry in the queue.
	state                 commitEntryStateType
}

// waitUntilComplete waits for an entry to be fully acked before timeout.
// Returns false if timeout, otherwise returns true.
func (e *commitEntry) waitUntilComplete() error {
	defer e.timeout.Stop()
	select {
	case e := <-e.completeCh:
		return e
	case <-e.timeout.C:
		return config.ErrTimeout
	}
}

// complete the operation reprensted by the entry by putting a struct{} signal to the channel.
func (e *commitEntry) complete() {
	e.completeCh <- nil
}

func (e *commitEntry) tryCancel() {
	if e.state == commitEntryStateType_Normal {
		e.completeCh <- config.ErrEntryCancel
	}
}

// If the entry has been expired or canceled, will return true.
// Else if the entry meets
func (e *commitEntry) tryComplete(isrSet map[string]struct{}) bool {
	if e.state == commitEntryStateType_Normal {
		if e.canBeComplete(isrSet) {
			e.complete()
			return true
		}
		return false
	}
	return true
}

// canBeComplete checks whether the entry has satisfied given complete condition.
func (e *commitEntry) canBeComplete(isrSet map[string]struct{}) bool {
	if e.requiredAcksToRespond < 0 {
		validCnt := 0
		for node := range e.ackedNodeName {
			_, ok := isrSet[node]
			if ok {
				validCnt += 1
			}
		}
		return validCnt >= len(isrSet)
	} else {
		return len(e.ackedNodeName) >= e.requiredAcksToRespond
	}
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

// +---------------------+
// |   Crucial Structs   |
// +---------------------+

// commitManager handles all uncommitted requests.
type commitManager struct {
	eventChannel chan commitManagerEvent
	hashQueue
	logger       *util.Logger
	cachedIsrSet map[string]struct{} // The isr cache that the commitManager thought to be currently. Updated through event queue.
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

// Shutdown commit manager until all events were complete.
func (cm *commitManager) processShutdown(waitCh chan struct{}) {

	waitCh <- struct{}{}
}

func (cm *commitManager) isValidRequiredAck(ack commitAckType) bool {
	return ack == -1 || ack == 0 || ack == 1
}

// addToWaitingQueue enqueues an offset to the queue, return entry as future completable object.
func (cm *commitManager) addToWaitingQueue(offset int64, entryLength int64, entry dbfile.Entry, timeout time.Duration, requiredAcksToRespond int) (*commitEntry, error) {
	e := &commitEntry{
		offset:                offset,
		entryLength:           entryLength,
		requiredAcksToRespond: requiredAcksToRespond,
		ackedNodeName:         make(map[string]struct{}, ctrlInstance.getClusterSize()),
		completeCh:            make(chan error, 1),
		timeout:               time.NewTimer(timeout),
		state:                 commitEntryStateType_Normal,
	}
	// @Debugging
	if config.EnableTimeEstimate {
		e.timestamp = time.Now()
	}
	return e, cm.internalEnqueueEvent(commitManagerEvent{
		evType:  commitManagerEvent_AddWaitingEntry,
		payload: e,
	})
}

func (cm *commitManager) enqueueReceiveLogFetchReqEvent(nodeName string, offset int64) error {
	return cm.internalEnqueueEvent(commitManagerEvent{
		evType: commitManagerEvent_RecvLogFetch,
		payload: commitAck{
			offset:   offset,
			nodeName: nodeName,
		},
	})
}

func (cm *commitManager) enqueueIsrSetUpdated() error {
	return cm.internalEnqueueEvent(commitManagerEvent{
		evType: commitManagerEvent_IsrSetUpdated,
	})
}

// add a shutdown event.
// Returns:
// - chan struct{}: used to wait for the remaining event to get proessed.
// - error: most likely to be channel closed error.
func (cm *commitManager) enqueueShutdownEvent() (chan struct{}, error) {
	waitCh := make(chan struct{}, 1)
	return waitCh, cm.internalEnqueueEvent(commitManagerEvent{
		evType:  commitManagerEvent_Shutdown,
		payload: waitCh,
	})
}

func (cm *commitManager) internalEnqueueEvent(ev commitManagerEvent) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w", err)
		}
	}()
	cm.eventChannel <- ev
	return
}

// try to complete the first element in the queue, if it is completed, dequeue it and perform check again.
// WARN: this method should only get invoked internally.
func (cm *commitManager) internalTryCompleteFromWaitingQueue() {
	for {
		if len(cm.queue) == 0 {
			return
		}
		first := cm.queue[0]
		// If the first element is canceled / timeout already, dequeue it directly.
		needDeq := false
		if first.state == commitEntryStateType_Canceled || first.state == commitEntryStateType_Timeout {
			ctrlInstance.stats.recordLogCommitFailed()
			needDeq = true
		}
		if first.tryComplete(cm.cachedIsrSet) {
			// @Debugging
			if config.EnableTimeEstimate {
				t := time.Since(first.timestamp)
				cm.logger.Debugf("commit wait time delta = %d ms", t.Milliseconds())
				ctrlInstance.stats.recordLogCommit(t)
				needDeq = true
			}
			cm.logger.Infof("offset = %d is considered commited", first.offset)
		}
		if needDeq {
			cm.queue = cm.queue[1:]
			delete(cm.hash, first.offset)
			continue // do another round of check
		}
		return // cannot complete the first element, quit checking
	}
}

// --- handlers ---

// processLogFetchAck will be called when a log fetch request arrives.
// All offsets less than ack.offset are acked.
func (cm *commitManager) processLogFetchAck(ack commitAck) {
	ctrlInstance.replicationManager.updateNodeStatus(ack.nodeName, ack.offset)
	// The elements in the queue are always in ascend order.
	// Keep acking until meeting bigger offset.
	cm.logger.Debugf("Received log fetch ack, offset = %d", ack.offset)
	var shouldTryComplete = false
	for _, entry := range cm.queue {
		if entry.offset >= ack.offset {
			cm.logger.Debugf("entry.offset = %d", entry.offset)
			break
		}
		entry.ackedNodeName[ack.nodeName] = struct{}{}
		if len(entry.ackedNodeName) >= entry.requiredAcksToRespond-1 {
			shouldTryComplete = true
		}
	}
	// Try to complete the first queueing element.
	if shouldTryComplete {
		cm.internalTryCompleteFromWaitingQueue()
	}
	cm.logger.Debugf("Handle log fetch OK")
}

// handleAddWaitingEntry will be called when a writing request comes from client.
func (cm *commitManager) processAddWaitingEntry(e *commitEntry) {
	cm.queue = append(cm.queue, e)
	lenAfterAppend := len(cm.queue)
	if lenAfterAppend > 1 {
		cm.queue[lenAfterAppend-1].nextEntry = e
	}
	cm.hash[e.offset] = e
	ctrlInstance.replicationManager.logFetchDeposit.notifyAndDequeue(e.entryLength + e.offset)
}

// processIsrSetUpdated is triggered through the event queue when there is an isr set expand / shrink.
func (cm *commitManager) processIsrSetUpdated() {
	cm.cachedIsrSet = util.StringList2Set(ctrlInstance.replicationManager.cloneIsrList(true))
	cm.internalTryCompleteFromWaitingQueue()
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
			cm.processAddWaitingEntry(ev.payload.(*commitEntry))
		case commitManagerEvent_RecvLogFetch:
			cm.processLogFetchAck(ev.payload.(commitAck))
		case commitManagerEvent_IsrSetUpdated:
			cm.processIsrSetUpdated()
		case commitManagerEvent_Shutdown:
			cm.processShutdown(ev.payload.(chan struct{}))
		}
	}
}
