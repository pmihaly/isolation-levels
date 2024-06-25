package main

import (
	"sync"
)

type TransactionLock struct {
	mutex *sync.Mutex
}

type TransactionLocks struct {
	readLockedKeys  map[Key]*sync.RWMutex
	writeLockedKeys map[Key]*sync.RWMutex
}

func NewTransactionLocks() *TransactionLocks {
	return &TransactionLocks{
		readLockedKeys:  make(map[Key]*sync.RWMutex),
		writeLockedKeys: make(map[Key]*sync.RWMutex),
	}
}

func (t *TransactionLocks) WLock(row *Row) bool {
	t.RUnlock(row)

	if _, ok := t.writeLockedKeys[row.Key]; ok {
		return false
	}

	row.Lock.Lock()
	t.writeLockedKeys[row.Key] = row.Lock

	return true
}

func (t *TransactionLocks) WUnlock(row *Row) {
	if _, ok := t.writeLockedKeys[row.Key]; !ok {
		return
	}

	row.Lock.Unlock()
	delete(t.writeLockedKeys, row.Key)
}

func (t *TransactionLocks) RLock(row *Row) bool {
	if _, ok := t.readLockedKeys[row.Key]; ok {
		return false
	}

	row.Lock.Lock()
	t.readLockedKeys[row.Key] = row.Lock

	return true
}

func (t *TransactionLocks) RUnlock(row *Row) {
	if _, ok := t.readLockedKeys[row.Key]; !ok {
		return
	}

	row.Lock.Unlock()
	delete(t.readLockedKeys, row.Key)
}

func (t *TransactionLocks) UnlockAll() {
	for key, mutex := range t.readLockedKeys {
		mutex.RUnlock()
		delete(t.writeLockedKeys, key)
	}

	for key, mutex := range t.writeLockedKeys {
		mutex.Unlock()
		delete(t.readLockedKeys, key)
	}
}
