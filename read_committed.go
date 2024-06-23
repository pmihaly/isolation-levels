package main

import (
	"sync"
)

type ReadCommitted struct {
	TransactionId       string
	Data                *map[string]Row
	Operations          []Operation
	MyUncommittedWrites map[string]string
}

func NewReadCommitted(transactionId string, data *map[string]Row) *ReadCommitted {
	return &ReadCommitted{
		TransactionId:       transactionId,
		Data:                data,
		Operations:          make([]Operation, 0),
		MyUncommittedWrites: make(map[string]string),
	}
}

func (t *ReadCommitted) Set(key, value string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		t.Operations = append(t.Operations, Operation{
			Key:       key,
			FromValue: EmptyValue(),
			ToValue:   value,
		})
		(*t.Data)[key] = Row{Key: key, Committed: EmptyValue(), Uncommitted: value, ExclusiveLock: &sync.Mutex{}}
		t.MyUncommittedWrites[key] = value
		return t
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: (*t.Data)[key].Uncommitted,
		ToValue:   value,
	})

	row.Uncommitted = value
	t.MyUncommittedWrites[key] = value
	(*t.Data)[key] = row
	return t
}

func (t *ReadCommitted) Get(key string) (Transaction, string) {
	row, ok := (*t.Data)[key]

	if !ok {
		return t, EmptyValue()
	}

	if _, ok := t.MyUncommittedWrites[key]; ok {
		return t, t.MyUncommittedWrites[key]
	}

	return t, row.Committed
}

func (t *ReadCommitted) Delete(key string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		return t
	}

	if _, ok := t.MyUncommittedWrites[key]; ok {
		delete(t.MyUncommittedWrites, key)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.Uncommitted,
		ToValue:   EmptyValue(),
	})

	row.Uncommitted = EmptyValue()
	(*t.Data)[key] = row

	return t
}

func (t *ReadCommitted) Lock(key string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		return t
	}

	row.ExclusiveLock.Lock()
	(*t.Data)[key] = row
	return t
}

func (t *ReadCommitted) Rollback() {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]
		row := (*t.Data)[op.Key]
		row.Uncommitted = op.FromValue
		(*t.Data)[op.Key] = row
	}

	t.Operations = make([]Operation, 0)
	t.MyUncommittedWrites = make(map[string]string)
}

func (t *ReadCommitted) Commit() {
	for _, op := range t.Operations {
		row := (*t.Data)[op.Key]

		row.Committed = t.MyUncommittedWrites[op.Key]
		row.Uncommitted = t.MyUncommittedWrites[op.Key]

		row.ExclusiveLock.Lock()
		(*t.Data)[op.Key] = row
		row.ExclusiveLock.Unlock()
	}

	t.Operations = make([]Operation, 0)
	t.MyUncommittedWrites = make(map[string]string)
}
