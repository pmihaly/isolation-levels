package main

import (
	"testing"
	"time"
)

func TestExplicitLockDoesntBlockSubsequentLocks(t *testing.T) {
	data := &map[string]Row{}

	NewReadUncommitted("1", data).
		Lock("x").
		Lock("x").
		Set("x", "A").
		Commit().
		Lock("x").
		Set("x", "A").
		Rollback()
}

func TestExplicitLockBlocksOtherTransaction(t *testing.T) {
	data := &map[string]Row{
		"x": NewRow("x", "A"),
	}

	t1 := NewReadUncommitted("1", data)
	t2 := NewReadUncommitted("2", data)

	t1.Lock("x").Set("x", "B")

	t2Value := make(chan string)
	blocked := make(chan bool)

	go func() {
		blocked <- true
		t2Value <- t2.Lock("x").Get("x")
	}()

	<-blocked

	select {
	case <-t2Value:
		t.Error("t2 was not blocked as expected")
	case <-time.After(100 * time.Millisecond):
	}

	t1.Commit()

	select {
	case value := <-t2Value:
		if value != "B" {
			t.Errorf("got %v, want %v", value, "B")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("t2 did not proceed after t1 committed")
	}
}
