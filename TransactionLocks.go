package main

import (
	"sync"
)

type TransactionLock struct {
	mutex *sync.Mutex
}

type TransactionLocks struct {
	lockedKeys map[Key]*sync.Mutex
}

func NewTransactionLocks() *TransactionLocks {
	return &TransactionLocks{
		lockedKeys: make(map[Key]*sync.Mutex),
	}
}

func (t *TransactionLocks) Lock(row *Row) bool {
	if _, ok := t.lockedKeys[row.Key]; ok {
		return false
	}

	row.ExclusiveLock.Lock()
	t.lockedKeys[row.Key] = row.ExclusiveLock

	return true
}

func (t *TransactionLocks) Unlock(row *Row) {
	if _, ok := t.lockedKeys[row.Key]; !ok {
		return
	}

	row.ExclusiveLock.Unlock()
	delete(t.lockedKeys, row.Key)
}

func (t *TransactionLocks) UnlockAll() {
	for key, mutex := range t.lockedKeys {
		mutex.Unlock()
		delete(t.lockedKeys, key)
	}
}
