package main

import (
	"sync"
)

type TrackableRWMutex struct {
	mu      sync.RWMutex
	stateMu sync.Mutex
	readers int
	writer  bool
	owner   TransactionId
}

func NewTrackableRWMutex() *TrackableRWMutex {
	return &TrackableRWMutex{
		mu:      sync.RWMutex{},
		stateMu: sync.Mutex{},
		readers: 0,
		writer:  false,
		owner:   EmptyTransactionId(),
	}
}

func (t *TrackableRWMutex) LockFor(txId TransactionId) {
	t.mu.Lock()
	t.stateMu.Lock()
	t.writer = true
	t.owner = txId
	t.stateMu.Unlock()
}

func (t *TrackableRWMutex) Unlock() {
	t.stateMu.Lock()
	t.writer = false
	t.owner = EmptyTransactionId()
	t.stateMu.Unlock()
	t.mu.Unlock()
}

func (t *TrackableRWMutex) RLock() {
	t.mu.RLock()
	t.stateMu.Lock()
	t.readers++
	t.stateMu.Unlock()
}

func (t *TrackableRWMutex) RUnlock() {
	t.stateMu.Lock()
	t.readers--
	t.stateMu.Unlock()
	t.mu.RUnlock()
}

func (t *TrackableRWMutex) IsBlocked(txId TransactionId) bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()

	isReadLocked := t.readers > 0
	if isReadLocked {
		return true
	}

	isWriteLocked := bool(t.writer)
	wasLockedByMe := (t.owner == txId || t.owner == EmptyTransactionId())

	return isWriteLocked && !wasLockedByMe
}
