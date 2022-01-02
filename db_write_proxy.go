package simplekv

import (
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
)

// writeProxy determines whether to persist the entry to disk directly
// or to register it to the commitManager to wait until enough acks are
// received before persisting the entry to the disk.
// Returns writePos and error.
func writeProxy(e dbfile.Entry) (offsetBeforeWrite int64, file *dbfile.File, err error) {
	// - choose file to write into
	stream := e.Encode()
	file = dataInstance.getNextActiveFile(stream)
	offsetBeforeWrite = file.Offset()
	totalOffsetAfterWrite := dataInstance.internalTotalOffset() + int64(len(stream))
	// Either no need to wait or already commited, persist it to the disk.
	if err := file.Write(stream); err != nil {
		return 0, nil, err
	}
	// Notify logs fetching requests that are currently waiting in the queue with offset after write.
	ctrlInstance.replicationManager.logFetchDeposit.notifyAndDequeue(totalOffsetAfterWrite)
	// If need more than 1 commit to proceed,
	// add it the commitManager, and wait it.
	if config.CommitMaxAck > 1 {
		dataInstance.mu.Unlock() // Release lock here, otherwise the waiting operation will block log replication when trying to get total offset.
		if err := ctrlInstance.commitMgr.addToWaitingQueue(offsetBeforeWrite, e, config.CommitTimeout).waitUntilComplete(); err != nil {
			// might be timeout error, but should ignore that.
			dataInstance.logger.Errorf("Err while waiting for %d acks: %s. Giveup waiting.", config.CommitMaxAck-1, err.Error())
		}
		dataInstance.mu.Lock()
	}
	// Update watermark.
	dataInstance.varfp.OverwriteWatermark(totalOffsetAfterWrite)
	return
}
