package main

import (
	"reflect"
	"testing"
)

func TestNone(t *testing.T) {
	if !reflect.DeepEqual(true, true) {
		t.Errorf("got %v, want %v", true, true)
	}
}
