package main

import (
	"sync"
	"testing"
)

func TestWriteSkew(t *testing.T) {
	table := NewTable()
	table.Data["doctor-a-is-on-call"] = NewRow("doctor-a-is-on-call", "true")
	table.Data["doctor-b-is-on-call"] = NewRow("doctor-b-is-on-call", "true")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		doctorB := NewTwoPhaseLocking("doctor-b", &table)

		isDoctorAOnCall := doctorB.Get("doctor-a-is-on-call") == "true"

		if isDoctorAOnCall {
			doctorB.Lock("doctor-b-is-on-call").Set("doctor-b-is-on-call", "false")
		}

		doctorB.Commit()
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		doctorA := NewTwoPhaseLocking("doctor-a", &table)

		isDoctorBOnCall := doctorA.Get("doctor-b-is-on-call") == "true"

		if isDoctorBOnCall {
			doctorA.Lock("doctor-a-is-on-call").Set("doctor-a-is-on-call", "false")
		}

		doctorA.Commit()
		wg.Done()
	}()

	wg.Wait()

	isDoctorAOutSick := table.Data["doctor-a-is-on-call"].Committed == "false"
	isDoctorBOutSick := table.Data["doctor-b-is-on-call"].Committed == "false"

	if !isDoctorAOutSick && !isDoctorBOutSick {
		t.Error("at least one doctor should be on call")
	}

	if isDoctorAOutSick && isDoctorBOutSick {
		t.Error("write skew occurred, both doctors are out sick, but one should be on call")
	}
}
