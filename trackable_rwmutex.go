package main

import (
	"sync"
)

type TrackableRWMutex struct {
	mu      sync.RWMutex
	stateMu sync.Mutex
	readers int
	writer  bool
}

func (t *TrackableRWMutex) Lock() {
	t.mu.Lock()
	t.stateMu.Lock()
	t.writer = true
	t.stateMu.Unlock()
}

func (t *TrackableRWMutex) Unlock() {
	t.stateMu.Lock()
	t.writer = false
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

func (t *TrackableRWMutex) IsLocked() bool {
	t.stateMu.Lock()
	defer t.stateMu.Unlock()
	return t.writer || t.readers > 0
}
