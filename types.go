package main

import (
	"sync"
)

func EmptyValue() string {
	return "<empty>"
}

type Operation struct {
	Key       string
	FromValue string
	ToValue   string
}

type Row struct {
	Key                        string
	Committed                  string
	LatestUncommitted          string
	UncommittedByTransactionId map[string]string
	ExclusiveLock              *sync.Mutex
}

func NewRow(key, value string) Row {
	return Row{
		Key:                        key,
		Committed:                  value,
		LatestUncommitted:          EmptyValue(),
		UncommittedByTransactionId: make(map[string]string),
		ExclusiveLock:              &sync.Mutex{},
	}
}

type Transaction interface {
	Set(key string, value string) Transaction
	Get(key string) (Transaction, string)
	Delete(key string) Transaction
	Lock(key string) Transaction
	Rollback()
	Commit()
}
