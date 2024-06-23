package main

import (
	"testing"
)

func TestDirtyReadsWrites(t *testing.T) {
	data := &map[string]Row{}
	(*data)["x"] = NewRow("x", "A")

	transactionPairs := [][]Transaction{
		{
			NewReadCommitted("1", data),
			NewReadCommitted("2", data),
		},
	}

	for _, trPair := range transactionPairs {
		testDirtyReads(t, trPair)
		testDirtyWrites(t, trPair)
	}

}

func testDirtyReads(t *testing.T, trPair []Transaction) {
	t1 := trPair[0]
	t2 := trPair[1]

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

func testDirtyWrites(t *testing.T, trPair []Transaction) {
	t1 := trPair[0]
	t2 := trPair[1]

	t1.Set("x", "B")
	t2.Set("x", "C")

	beforeCommitted := t1.Get("x")
	if beforeCommitted != "B" {
		t.Errorf("got %v, want %v", beforeCommitted, "B")
	}
}
