package main

import (
	"reflect"
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

	if !reflect.DeepEqual(value, EmptyValue()) {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}

	value = tr.Set("x", "A").Get("x")

	if !reflect.DeepEqual(value, "A") {
		t.Errorf("got %v, want %v", value, "A")
	}

	value = tr.Set("x", "B").Get("x")

	if !reflect.DeepEqual(value, "B") {
		t.Errorf("got %v, want %v", value, "B")
	}

	value = tr.Delete("x").Commit().Get("x")

	if !reflect.DeepEqual(value, EmptyValue()) {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}
}

func testCommitRollback(t *testing.T, tr Transaction) {
	value := tr.Set("x", "A").Rollback().Get("x")

	if !reflect.DeepEqual(value, EmptyValue()) {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}

	value = tr.Set("x", "A").Commit().Get("x")

	if !reflect.DeepEqual(value, "A") {
		t.Errorf("got %v, want %v", value, "A")
	}
}
