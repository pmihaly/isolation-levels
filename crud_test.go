package main

import (
	"testing"
)

func TestCrud(t *testing.T) {
	transactions := []Transaction{
		NewReadUncommitted("1", &map[string]Row{}),
		NewReadCommitted("1", &map[string]Row{}),
	}

	for _, tr := range transactions {
		testCrud(t, tr)
		testCommitRollback(t, tr)
	}

}

func testCrud(t *testing.T, tr Transaction) {
	value := tr.Get("x")

	if value != EmptyValue() {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}

	value = tr.Set("x", "A").Get("x")

	if value != "A" {
		t.Errorf("got %v, want %v", value, "A")
	}

	value = tr.Set("x", "B").Get("x")

	if value != "B" {
		t.Errorf("got %v, want %v", value, "B")
	}

	value = tr.Delete("x").Get("x")

	if value != EmptyValue() {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}
}

func testCommitRollback(t *testing.T, tr Transaction) {
	value := tr.Set("x", "A").Rollback().Get("x")

	if value != EmptyValue() {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}

	value = tr.Set("x", "A").Commit().Get("x")

	if value != "A" {
		t.Errorf("got %v, want %v", value, "A")
	}

	value = tr.
		Set("x", "B").
		Commit().
		Set("x", "C").
		Rollback().
		Get("x")

	if value != "B" {
		t.Errorf("got %v, want %v", value, "B")
	}
}
