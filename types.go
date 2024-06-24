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
	data map[Key]Row
}

func NewTable() Table {
	return Table{
		data: make(map[Key]Row),
	}
}

func (t *Table) GetRow(key Key) (Row, bool) {
	row, ok := t.data[key]
	return row, ok
}

func (t *Table) SetRow(key Key, row Row) {
	t.data[key] = row
}

type Transaction interface {
	Set(key Key, value Value) Transaction
	Get(key Key) Value
	Delete(key Key) Transaction
	Lock(key Key) Transaction
	Rollback() Transaction
	Commit() Transaction
}
