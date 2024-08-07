package main

import (
	"sync"
)

type Key string

func EmptyKey() Key {
	return "<empty>"
}

type TransactionId string

func EmptyTransactionId() TransactionId {
	return "<empty>"
}

type Value string

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
	Lock              *sync.RWMutex `json:"-"`
}

func NewRow(key Key, value Value) Row {
	return Row{
		Key:               key,
		Committed:         value,
		LatestUncommitted: value,
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
	GetKeysWrittenTo() []Key
	GetLocks() *TransactionLocks
}

type TransactionLevel int

const (
	readUncommitted TransactionLevel = iota
	readCommitted
	snapshotIsolation
	twoPhaseLocking
)

type EventType int

const (
	TableOperation EventType = iota
)

type OperationType int

const (
	WriteOperation OperationType = iota
	ReadOperation
	Commit
	Rollback
	Wait
)

type TableEvent struct {
	TxId          TransactionId
	TxLevel       TransactionLevel
	OperationType OperationType
	Key           Key
	To            Value
}

func NewRead(
	txId TransactionId,
	txLevel TransactionLevel,
	key Key,
) Event {
	return Event{TableEvent: &TableEvent{
		TxId:          txId,
		TxLevel:       txLevel,
		OperationType: ReadOperation,
		Key:           key,
		To:            EmptyValue(),
	}}
}

func NewWrite(
	txId TransactionId,
	txLevel TransactionLevel,
	key Key,
	to Value,
) Event {
	return Event{TableEvent: &TableEvent{
		TxId:          txId,
		TxLevel:       txLevel,
		OperationType: WriteOperation,
		Key:           key,
		To:            to,
	}}
}

func NewCommit(
	txId TransactionId,
	txLevel TransactionLevel,
) Event {
	return Event{TableEvent: &TableEvent{
		TxId:          txId,
		TxLevel:       txLevel,
		OperationType: Commit,
		Key:           EmptyKey(),
		To:            EmptyValue(),
	}}
}

func NewRollback(
	txId TransactionId,
	txLevel TransactionLevel,
) Event {
	return Event{TableEvent: &TableEvent{
		TxId:          txId,
		TxLevel:       txLevel,
		OperationType: Rollback,
		Key:           EmptyKey(),
		To:            EmptyValue(),
	}}
}

// https://go.dev/play/p/LhJ7tnMoDT4
type Event struct {
	*TableEvent
}
