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

type LockLevel int

const (
	EmptyLockLevel LockLevel = iota
	Read
	ReadWrite
)

func (t *TransactionLocks) Lock(lockType LockLevel, row *Row) bool {
	_, isReadLocked := t.readLockedKeys[row.Key]
	_, isWriteLocked := t.writeLockedKeys[row.Key]

	if isWriteLocked {
		return false
	}

	if lockType == Read {
		if isReadLocked {
			return false
		}

		row.Lock.RLock()
		t.readLockedKeys[row.Key] = row.Lock

		return true
	}

	isUpgradingLock := lockType == ReadWrite && isReadLocked
	if isUpgradingLock {
		row.Lock.RUnlock()
		delete(t.readLockedKeys, row.Key)
	}

	row.Lock.Lock()
	t.writeLockedKeys[row.Key] = row.Lock

	return true
}

func (t *TransactionLocks) Unlock(row *Row) {
	_, isReadLocked := t.readLockedKeys[row.Key]

	if isReadLocked {
		row.Lock.RUnlock()
		delete(t.readLockedKeys, row.Key)
		return
	}

	_, isWriteLocked := t.writeLockedKeys[row.Key]
	if isWriteLocked {
		row.Lock.Unlock()
		delete(t.writeLockedKeys, row.Key)
	}
}

func (t *TransactionLocks) UnlockAll() {
	for key, mutex := range t.readLockedKeys {
		mutex.RUnlock()
		delete(t.readLockedKeys, key)
	}

	for key, mutex := range t.writeLockedKeys {
		mutex.Unlock()
		delete(t.writeLockedKeys, key)
	}
}

func (t *TransactionLocks) GetLockLevels() map[Key]LockLevel {
	result := make(map[Key]LockLevel)

	for key := range t.readLockedKeys {
		result[key] = Read
	}

	for key := range t.writeLockedKeys {
		result[key] = ReadWrite
	}

	return result
}
