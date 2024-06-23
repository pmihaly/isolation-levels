package main

import (
	"reflect"
	"sync"
	"testing"
)

func TestDirtyReads(t *testing.T) {
	data := &map[string]Row{
		"x": {
			Key:           "x",
			Committed:     "A",
			Uncommitted:   "A",
			ExclusiveLock: &sync.Mutex{},
			IsLocked:      false,
		},
	}

	transactionPairs := [][]Transaction{
		{
			NewReadCommitted("1", data),
			NewReadCommitted("2", data),
		},
	}

	for _, trPair := range transactionPairs {
		testDirtyReads(t, trPair)
	}

}

func testDirtyReads(t *testing.T, trPair []Transaction) {
	t1 := trPair[0]
	t2 := trPair[1]

	_, beforeCommitted := t1.Get("x")
	if !reflect.DeepEqual(beforeCommitted, "A") {
		t.Errorf("got %v, want %v", beforeCommitted, "A")
	}

	t2.Set("x", "B")
	_, afterUncommitted := t1.Get("x")
	if !reflect.DeepEqual(afterUncommitted, "A") {
		t.Errorf("got %v, want %v", afterUncommitted, "A")
	}

	t2.Commit()
	_, afterCommitted := t1.Get("x")
	if !reflect.DeepEqual(afterCommitted, "B") {
		t.Errorf("got %v, want %v", afterCommitted, "B")
	}

}
