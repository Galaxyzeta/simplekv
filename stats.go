package simplekv

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

type statsUpdateEvent struct {
	tp     statsUpdateEventType
	params []interface{}
}

type statsUpdateEventType byte

const (
	statsEvent_LogCommit       statsUpdateEventType = 1
	statsEvent_LogCommitFailed statsUpdateEventType = 2
)

const statsUpdateBatchInitSize = 512
const statsUpdateInterval = time.Millisecond * 500

type statsManager struct {
	batch              []statsUpdateEvent
	commitStats        *maxMinAvgStatsHolder
	commitFaildCounter int64
	commitStatsLock    sync.Mutex
	cacheStats         *hitAndMissStatsHolder
}

type hitAndMissStatsHolder struct {
	hitCnt  int64
	missCnt int64
}

func newHitAndMissStatsHolder() *hitAndMissStatsHolder {
	return &hitAndMissStatsHolder{}
}

func (s *hitAndMissStatsHolder) hit(cnt int64) {
	atomic.AddInt64(&s.hitCnt, cnt)
}

func (s *hitAndMissStatsHolder) miss(cnt int64) {
	atomic.AddInt64(&s.missCnt, cnt)
}

func (s *hitAndMissStatsHolder) getHitCnt() int64 {
	return atomic.LoadInt64(&s.hitCnt)
}

func (s *hitAndMissStatsHolder) getMissCnt() int64 {
	return atomic.LoadInt64(&s.missCnt)
}

func (s *hitAndMissStatsHolder) getHitRate() float64 {
	miss := s.getMissCnt()
	hit := s.getHitCnt()
	total := miss + hit
	if total == 0 {
		return 0
	}
	return float64(hit) / float64(total)
}

func (s *hitAndMissStatsHolder) toString() string {
	return fmt.Sprintf("Hit = %d | Miss = %d | HitRate = %.3f", s.getHitCnt(), s.getMissCnt(), s.getHitRate())
}

type maxMinAvgStatsHolder struct {
	avg    float64
	sum    float64
	maxval float64
	minval float64
	cnt    float64
}

func newMaxMinAvgStatsHolder() *maxMinAvgStatsHolder {
	return &maxMinAvgStatsHolder{
		maxval: -1,
		minval: math.MaxFloat64,
	}
}

func (s maxMinAvgStatsHolder) toString() string {
	return fmt.Sprintf("Avg = %f | Sum = %f | Max = %f | Min = %f | Cnt = %f", s.avg, s.sum, s.maxval, s.minval, s.cnt)
}

func (s *maxMinAvgStatsHolder) add(data float64) {
	s.sum += data
	s.cnt += 1
	if data > s.maxval {
		s.maxval = data
	} else if data < s.minval {
		s.minval = data
	}
	s.avg = s.sum / s.cnt
}

func (s *maxMinAvgStatsHolder) clone() maxMinAvgStatsHolder {
	return maxMinAvgStatsHolder{
		avg:    s.avg,
		sum:    s.sum,
		maxval: s.maxval,
		minval: s.minval,
		cnt:    s.cnt,
	}
}

func newStatsManager() *statsManager {
	return &statsManager{
		batch:       make([]statsUpdateEvent, statsUpdateBatchInitSize),
		commitStats: newMaxMinAvgStatsHolder(),
		cacheStats:  newHitAndMissStatsHolder(),
	}
}

// This method should be called inside a goroutine.
func (sm *statsManager) run() {
	ticker := time.NewTicker(statsUpdateInterval)
	for {
		select {
		case <-ticker.C:
			for _, eachEvent := range sm.batch {
				switch eachEvent.tp {
				case statsEvent_LogCommit:
					sm.processLogCommit(eachEvent)
				}
			}
			sm.batch = sm.batch[0:0] // clear the slice
		default:
			if ctrlInstance.getIsShutingdown() {
				ctrlInstance.logger.Warnf("StatManager exiting...")
				return
			}
		}
	}
}

func (sm *statsManager) processLogCommit(ev statsUpdateEvent) {
	sm.commitStats.add(ev.params[0].(float64))
}

func (sm *statsManager) recordLogCommit(commitWaitTime time.Duration) {
	record := statsUpdateEvent{
		tp: statsEvent_LogCommit,
		params: []interface{}{
			float64(commitWaitTime.Milliseconds()),
		},
	}
	sm.batch = append(sm.batch, record)
}

func (sm *statsManager) recordLogCommitFailed() {
	atomic.AddInt64(&sm.commitFaildCounter, 1)
}

// get a copy of commit stats
func (sm *statsManager) getCommitStats() maxMinAvgStatsHolder {
	sm.commitStatsLock.Lock()
	defer sm.commitStatsLock.Unlock()
	return sm.commitStats.clone()
}
