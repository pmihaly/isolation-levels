package main

import (
	"log"
)

func main() {
	log.Printf("hello world")

	table := NewTable()
	t1 := NewReadUncommitted("1", &table)
	t1.Set("key1", "value1").Commit()

	t2 := NewReadUncommitted("2", &table)
	t2.Set("key1", "value2")
	t2.Delete("key1")
	t2.Rollback()

	log.Printf("data: %v", table)
}
