package main

import (
	"sync"
	"testing"
	"time"
)

func TestUnlock(t *testing.T) {
	table := NewTable()
	t1 := NewTwoPhaseLocking("1", &table)
	t2 := NewTwoPhaseLocking("2", &table)

	var wg sync.WaitGroup
	done := make(chan struct{})
	t1Wrote := make(chan struct{})

	wg.Add(2)
	go func() {
		defer wg.Done()
		t1.Set("x", "1")
		close(t1Wrote)
		time.Sleep(100 * time.Millisecond)
		t1.Commit()
	}()

	go func() {
		defer wg.Done()
		<-t1Wrote
		t2.Get("x")
		t2.Commit()
	}()

	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Errorf("did not unlock after 200ms")
	}
}
