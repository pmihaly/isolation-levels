package main

func EmptyValue() string {
	return "<empty>"
}

type Operation struct {
	Key       string
	FromValue string
	ToValue   string
}

type Row struct {
	Key         string
	Committed   string
	Uncommitted string
}

type Transaction interface {
	Set(key string, value string) Transaction
	Get(key string) (Transaction, string)
	Delete(key string) Transaction
	Rollback()
	Commit()
}
