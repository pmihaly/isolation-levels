package main

import (
	"reflect"
	"sync"
	"testing"
)

func TestExclusiveLocks(t *testing.T) {
	data := &map[string]Row{
		"x": {
			Key:           "x",
			Committed:     "A",
			Uncommitted:   "A",
			ExclusiveLock: &sync.Mutex{},
			IsLocked:      false,
		},
	}
	var wg sync.WaitGroup

	t1 := &ReadUncommitted{
		TransactionId: "1",
		Data:          data,
	}

	t1.Lock("x").Set("x", "B")

	wg.Add(1)
	go func() {
		defer wg.Done()
		t2 := &ReadUncommitted{
			TransactionId: "2",
			Data:          data,
		}

		t2.Lock("x").Set("x", "C").Commit()
	}()

	t1.Commit()

	_, value := t1.Get("x")

	if !reflect.DeepEqual(value, "B") {
		t.Errorf("got %v, want %v", value, "B")
	}

	wg.Wait()

	_, value = t1.Get("x")

	if !reflect.DeepEqual(value, "C") {
		t.Errorf("got %v, want %v", value, "C")
	}
}
