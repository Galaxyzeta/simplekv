package simplekv

import (
	"github.com/galaxyzeta/simplekv/config"
	"github.com/galaxyzeta/simplekv/dbfile"
)

// writeProxy determines whether to persist the entry to disk directly
// or to register it to the commitManager to wait until enough acks are
// received before returning the result.
// Returns writePos and error.
func writeProxy(e dbfile.Entry, requiredAcks int) (offsetBeforeWrite int64, file *dbfile.File, err error) {
	// Check whether requiredAcks is valid
	if !ctrlInstance.commitMgr.isValidRequiredAck(commitAckType(requiredAcks)) {
		return 0, nil, config.ErrInvalidRequiredIsr
	}
	// Choose file to write into
	stream := e.Encode()
	file = dataInstance.getNextActiveFile(stream)
	offsetBeforeWrite = file.Offset()
	streamLength := int64(len(stream))
	// Either no need to wait or already commited, persist it to the disk.
	if err = file.Write(stream); err != nil {
		return 0, nil, err
	}
	// If required Ack == ALLISR, add it to commitManager.
	if requiredAcks == commitAck_AllIsr {
		dataInstance.logger.Infof("Incoming entry = <%s, %s>", e.Key, e.Value)
		// Try to reject when there's not enough ISR
		var isrList = ctrlInstance.replicationManager.getIsrList()
		dataInstance.logger.Infof("IsrList = %s, minIsr = %d", isrList, config.ReplicationIsrMinRequired)
		if len(isrList) < config.ReplicationIsrMinRequired {
			dataInstance.logger.Infof("Rejecting entry = <%s, %s> because there's not enough ISR, current ISR = %s", e.Key, e.Value, isrList)
			return 0, nil, config.ErrNotEnoughIsr
		}
		if len(isrList) == 1 && isrList[0] == ctrlInstance.nodeName {
			// Direct commit
		} else {
			// Require wait
			dataInstance.mu.Unlock() // Release lock here, otherwise the waiting operation will block log replication when trying to get total offset.
			var entry *commitEntry
			entry, err = ctrlInstance.commitMgr.addToWaitingQueue(offsetBeforeWrite, streamLength, e, config.CommitTimeout, requiredAcks)
			// Handle channel closed error
			if err != nil {
				return 0, nil, err
			}
			if entry.waitUntilComplete(); err != nil {
				// might be timeout error, but should ignore that.
				dataInstance.mu.Lock()
				dataInstance.logger.Errorf("Entry = <%s, %s> err while waiting for %d acks: %s. Giveup waiting.", e.Key, e.Value, requiredAcks, err.Error())
				return
			} else {
				dataInstance.mu.Lock()
			}
		}
	}
	dataInstance.logger.Infof("Entry = <%s, %s> has been commited, requiredAcks = %d", e.Key, e.Value, requiredAcks)
	return
}
