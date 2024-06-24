package main

import (
	"testing"
)

func TestReadSkew(t *testing.T) {
	table := NewTable()
	table.Data["x"] = NewRow("x", "A")

	transactionPairs := [][]Transaction{
		{
			NewSnapshotIsolation("1", &table),
			NewSnapshotIsolation("2", &table),
		},
	}

	for _, txPair := range transactionPairs {
		testReadSkew(t, txPair)
	}

}

func testReadSkew(t *testing.T, txPair []Transaction) {
	t1 := txPair[0]
	t2 := txPair[1]

	t2.Set("x", "B").Commit()
	afterT2Commit := t1.Get("x")

	if afterT2Commit != "A" {
		t.Errorf("got %v, want %v", afterT2Commit, "A")
	}

}
