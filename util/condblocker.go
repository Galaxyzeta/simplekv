package util

import (
	"sync"
)

type ConditionBlocker struct {
	mu   *sync.Mutex
	cond *sync.Cond
	fn   func() bool
}

func NewConditionBlocker(checker func() bool) ConditionBlocker {
	cb := ConditionBlocker{
		fn: checker,
	}
	cb.mu = &sync.Mutex{}
	cb.cond = sync.NewCond(cb.mu)
	return cb
}

func (cb *ConditionBlocker) WaitOnceIfFalse() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if !cb.fn() {
		cb.cond.Wait()
	}
}

func (cb *ConditionBlocker) WaitOnceIfTrue() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.fn() {
		cb.cond.Wait()
	}
}

func (cb *ConditionBlocker) LoopWaitUntilTrue() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for !cb.fn() {
		cb.cond.Wait()
	}
}

func (cb *ConditionBlocker) LoopWaitUntilFalse() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	for cb.fn() {
		cb.cond.Wait()
	}
}

func (cb *ConditionBlocker) Broadcast() {
	cb.cond.Broadcast()
}

func (cb *ConditionBlocker) Signal() {
	cb.cond.Signal()
}
