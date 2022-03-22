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
	// If required Ack is bigger than 1 or need to get replicated to all ISR, add it to
	if requiredAcks != 0 {
		dataInstance.mu.Unlock() // Release lock here, otherwise the waiting operation will block log replication when trying to get total offset.
		dataInstance.logger.Infof("Incoming entry = <%s, %s>", e.Key, e.Value)

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
	dataInstance.logger.Infof("Entry = <%s, %s> has been commited", e.Key, e.Value)
	return
}
