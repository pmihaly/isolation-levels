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
	ExclusiveLock     *sync.Mutex
}

func NewRow(key Key, value Value) Row {
	return Row{
		Key:               key,
		Committed:         value,
		LatestUncommitted: EmptyValue(),
		UncommittedByTxId: make(map[TransactionId]Value),
		ExclusiveLock:     &sync.Mutex{},
	}
}

type Table struct {
	Data map[Key]Row
}

func NewTable() Table {
	return Table{
		Data: make(map[Key]Row),
	}
}

func (t *Table) GetCommitted(key Key, txId TransactionId) (Value, bool) {
	row, ok := t.Data[key]
	if !ok {
		return EmptyValue(), false
	}
	return row.Committed, true
}

func (t *Table) SetCommitted(key Key, value Value, txId TransactionId) {
	row, ok := t.Data[key]

	if !ok {
		panic("key not found")
	}

	row.Committed = value
	row.LatestUncommitted = value
	delete(row.UncommittedByTxId, txId)

	t.Data[key] = row
}

type Transaction interface {
	Set(key Key, value Value) Transaction
	Get(key Key) Value
	Delete(key Key) Transaction
	Lock(key Key) Transaction
	Rollback() Transaction
	Commit() Transaction
}
