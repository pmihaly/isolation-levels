package main

import (
	"testing"
)

func TestDirtyReadsWrites(t *testing.T) {
	table := NewTable()
	table.Data["x"] = NewRow("x", "A")

	transactionPairs := [][]Transaction{
		{
			NewReadCommitted("1", &table),
			NewReadCommitted("2", &table),
		},
		{
			NewSnapshotIsolation("1", &table),
			NewSnapshotIsolation("2", &table),
		},
		// TODO figure out making these tests concurrent
		// {
		// 	NewTwoPhaseLocking("1", &table),
		// 	NewTwoPhaseLocking("2", &table),
		// },
	}

	for _, txPair := range transactionPairs {
		table = NewTable()
		table.Data["x"] = NewRow("x", "A")

		testDirtyReads(t, txPair)
		testDirtyWrites(t, txPair)
	}

}

func testDirtyReads(t *testing.T, txPair []Transaction) {
	t1 := txPair[0]
	t2 := txPair[1]

	beforeCommitted := t1.Get("x")
	if beforeCommitted != "A" {
		t.Errorf("got %v, want %v", beforeCommitted, "A")
	}

	t2.Set("x", "B")
	afterUncommitted := t1.Get("x")
	if afterUncommitted != "A" {
		t.Errorf("got %v, want %v", afterUncommitted, "A")
	}

	t2.Commit()
	afterCommitted := t1.Get("x")
	if afterCommitted != "B" {
		t.Errorf("got %v, want %v", afterCommitted, "B")
	}
}

func testDirtyWrites(t *testing.T, txPair []Transaction) {
	t1 := txPair[0]
	t2 := txPair[1]

	t1.Set("x", "B")
	t2.Set("x", "C")

	beforeCommitted := t1.Get("x")
	if beforeCommitted != "B" {
		t.Errorf("got %v, want %v", beforeCommitted, "B")
	}
}
