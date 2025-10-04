package main

import (
	"testing"
)

func TestDontShowSnapshotByDefault(t *testing.T) {
	table := NewTable()
	(&table).Data["x"] = NewRow("x", "1")

	events := []Event{
		NewWrite("t1", twoPhaseLocking, "x", "2"),
		NewCommit("t1", twoPhaseLocking),
	}

	productedMermaid := PlayEvents(events, &table)

	expectedMermaid := `sequenceDiagram
    participant x
    actor t1
    note over x: {"Key":"x","Committed":"1","LatestUncommitted":"1","UncommittedByTxId":{}}
    t1 ->> x: set x = 2
    activate x
    activate x
    x ->> t1: ok
    note over x: {"Key":"x","Committed":"1","LatestUncommitted":"2","UncommittedByTxId":{"t1":"2"}}
    t1 ->> x: commit
    x ->> t1: ok
    deactivate x
    deactivate x
    note over x: {"Key":"x","Committed":"2","LatestUncommitted":"2","UncommittedByTxId":{}}
`

	if productedMermaid != expectedMermaid {
		t.Errorf("got %v, want %v", productedMermaid, expectedMermaid)
	}
}

func TestShowSnapshotUsedForReading(t *testing.T) {
	table := NewTable()
	(&table).Data["x"] = NewRow("x", "1")

	events := []Event{
		NewWrite("t1", twoPhaseLocking, "x", "2"),
		NewRead("t1", twoPhaseLocking, "x"),
		NewCommit("t1", twoPhaseLocking),
	}

	productedMermaid := PlayEvents(events, &table)

	expectedMermaid := `sequenceDiagram
    participant x
    actor t1
    participant t1 snapshot of x
    note over x: {"Key":"x","Committed":"1","LatestUncommitted":"1","UncommittedByTxId":{}}
    t1 ->> t1 snapshot of x: set x = 2
    t1 ->> x: set x = 2
    activate x
    activate x
    x ->> t1: ok
    note over x: {"Key":"x","Committed":"1","LatestUncommitted":"2","UncommittedByTxId":{"t1":"2"}}
    t1 ->> t1 snapshot of x: get x
    t1 snapshot of x ->> t1: x = 2
    t1 ->> x: commit
    x ->> t1: ok
    deactivate x
    deactivate x
    note over x: {"Key":"x","Committed":"2","LatestUncommitted":"2","UncommittedByTxId":{}}
`

	if productedMermaid != expectedMermaid {
		t.Errorf("got %v, want %v", productedMermaid, expectedMermaid)
	}
}
