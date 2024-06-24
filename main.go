package main

import (
	"log"
)

func main() {
	log.Printf("hello world")

	data := NewTable()
	t := ReadUncommitted{
		TransactionId: "1",
		Data:          &data,
	}
	t.Set("key1", "value1").Commit()

	t2 := ReadUncommitted{
		TransactionId: "2",
		Data:          &data,
	}
	t2.Set("key1", "value2")
	t2.Delete("key1")
	t2.Rollback()

	log.Printf("data: %v", data)
}
