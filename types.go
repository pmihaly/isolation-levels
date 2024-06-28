package main

import (
	"sync"
)

type Key string
type Value string
type TransactionId string

func EmptyValue() Value {
	return "<empty>"
}

type Operation struct {
	Key       Key
	FromValue Value
	ToValue   Value
}

type Row struct {
	Key               Key
	Committed         Value
	LatestUncommitted Value
	UncommittedByTxId map[TransactionId]Value
	Lock              *sync.RWMutex
}

func NewRow(key Key, value Value) Row {
	return Row{
		Key:               key,
		Committed:         value,
		LatestUncommitted: EmptyValue(),
		UncommittedByTxId: make(map[TransactionId]Value),
		Lock:              &sync.RWMutex{},
	}
}

type Snapshot map[Key]Value

type Table struct {
	Data      map[Key]Row
	snapshots map[TransactionId]Snapshot
}

func NewTable() Table {
	return Table{
		Data:      make(map[Key]Row),
		snapshots: make(map[TransactionId]Snapshot),
	}
}

func (t *Table) GetCommitted(key Key, txId TransactionId) (Value, bool) {
	if value, ok := t.snapshots[txId][key]; ok {
		return value, true
	}

	if row, ok := t.Data[key]; ok {
		return row.Committed, true
	}

	return EmptyValue(), false
}

func (t *Table) SetCommitted(key Key, value Value, txId TransactionId) {
	row, ok := t.Data[key]

	if !ok {
		panic("key not found")
	}

	for snapshotTxid, snapshot := range t.snapshots {
		if snapshotTxid == txId {
			continue
		}

		if _, ok := snapshot[key]; !ok {
			t.snapshots[snapshotTxid][key] = row.Committed
		}

	}

	row.Committed = value
	row.LatestUncommitted = value
	delete(row.UncommittedByTxId, txId)

	t.Data[key] = row
}

func (t *Table) TakeSnapshot(txId TransactionId) {
	snapshot := make(Snapshot)
	t.snapshots[txId] = snapshot
}

func (t *Table) DeleteSnapshot(txId TransactionId) {
	delete(t.snapshots, txId)
}

type Transaction interface {
	Set(key Key, value Value) Transaction
	Get(key Key) Value
	Delete(key Key) Transaction
	Lock(key Key) Transaction
	Rollback() Transaction
	Commit() Transaction
}
