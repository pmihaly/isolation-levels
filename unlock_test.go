package main

import (
	"log"
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
		log.Printf("t1.set start")
		t1.Set("x", "1")
		log.Printf("t1.set end")
		close(t1Wrote)
		time.Sleep(100 * time.Millisecond)
		t1.Commit()
		log.Printf("t1.commit")
	}()

	go func() {
		defer wg.Done()
		<-t1Wrote
		log.Printf("t2.get start")
		t2.Get("x")
		log.Printf("t2.get end")
		t2.Commit()
		log.Printf("t2.commit")
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
