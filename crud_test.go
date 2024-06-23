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
	tr.Get("x")
	_, value := tr.Get("x")

	if !reflect.DeepEqual(value, EmptyValue()) {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}

	tr.Set("x", "A")
	_, value = tr.Get("x")

	if !reflect.DeepEqual(value, "A") {
		t.Errorf("got %v, want %v", value, "A")
	}

	tr.Set("x", "B")
	_, value = tr.Get("x")

	if !reflect.DeepEqual(value, "B") {
		t.Errorf("got %v, want %v", value, "B")
	}

	tr.Delete("x")
	_, value = tr.Get("x")

	if !reflect.DeepEqual(value, EmptyValue()) {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}
}

func testCommitRollback(t *testing.T, tr Transaction) {
	tr.Set("x", "A")

	tr.Rollback()

	_, value := tr.Get("x")
	if !reflect.DeepEqual(value, EmptyValue()) {
		t.Errorf("got %v, want %v", value, EmptyValue())
	}

	tr.Set("x", "A")

	tr.Commit()

	_, value = tr.Get("x")
	if !reflect.DeepEqual(value, "A") {
		t.Errorf("got %v, want %v", value, "A")
	}
}
