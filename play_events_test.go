package main

import (
	"testing"
)

func TestPlayEventsProducingMermaid(t *testing.T) {
	table := NewTable()
	(&table).Data["x"] = NewRow("x", "1")

	events := []Event{
		NewWrite("t1", TwoPhaseLockingLevel, "x", "2"),
		NewRead("t2", TwoPhaseLockingLevel, "x"),
		NewCommit("t1", TwoPhaseLockingLevel),
		NewCommit("t2", TwoPhaseLockingLevel),
	}

	productedMermaid, err := PlayEvents(events, &table)
	if err != nil {
		t.Errorf("expted PlayEvents to succeed, got error: %v", err)
	}

	expectedMermaid := `
sequenceDiagram
    actor t1
    participant x
    actor t2
    note right of x: x = 1
    t1 ->> x: set x = 2
    activate x
    activate x
    x ->> t1: ok
    t2 -->> x: get x
    t1 ->> x: commit
    deactivate x
    deactivate x
    note right of x: x = 2
    t2 ->> x: get x
    activate x
    x ->> t2: x = 2
    t2 ->> x: commit
    deactivate x
    note right of x: x = 2
`

	if productedMermaid != expectedMermaid {
		t.Errorf("got %v, want %v", productedMermaid, expectedMermaid)
	}
}
