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
	Key           string
	Committed     string
	Uncommitted   string
	ExclusiveLock *sync.Mutex
}

type Transaction interface {
	Set(key string, value string) Transaction
	Get(key string) (Transaction, string)
	Delete(key string) Transaction
	Lock(key string) Transaction
	Rollback()
	Commit()
}
