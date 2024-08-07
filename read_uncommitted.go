package main

type ReadUncommitted struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	locks         *TransactionLocks
	keysWrittenTo map[Key]struct{}
}

func NewReadUncommitted(transactionId TransactionId, table *Table) *ReadUncommitted {
	return &ReadUncommitted{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		locks:         NewTransactionLocks(),
		keysWrittenTo: make(map[Key]struct{}),
	}
}

func (t *ReadUncommitted) Set(key Key, value Value) Transaction {
	row, ok := t.Table.Data[key]
	prevValue := row.LatestUncommitted

	if !ok {
		row = NewRow(key, value)
		prevValue = EmptyValue()
	}

	didILock := t.locks.Lock(ReadWrite, &row)
	if didILock {
		defer t.locks.Unlock(&row)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: prevValue,
		ToValue:   value,
	})

	t.keysWrittenTo[key] = struct{}{}
	row.LatestUncommitted = value
	t.Table.Data[key] = row

	return t
}

func (t *ReadUncommitted) Get(key Key) Value {
	row, ok := t.Table.Data[key]

	if !ok {
		return EmptyValue()
	}

	didILock := t.locks.Lock(Read, &row)
	if didILock {
		defer t.locks.Unlock(&row)
	}

	return row.LatestUncommitted
}

func (t *ReadUncommitted) Delete(key Key) Transaction {
	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	didILock := t.locks.Lock(ReadWrite, &row)
	if didILock {
		defer t.locks.Unlock(&row)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.LatestUncommitted,
		ToValue:   EmptyValue(),
	})

	if _, ok := t.keysWrittenTo[key]; ok {
		delete(t.keysWrittenTo, key)
	} else {
		t.keysWrittenTo[key] = struct{}{}
	}

	row.LatestUncommitted = EmptyValue()
	t.Table.Data[key] = row

	return t
}

func (t *ReadUncommitted) Lock(key Key) Transaction {
	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	t.locks.Lock(ReadWrite, &row)

	return t
}

func (t *ReadUncommitted) Rollback() Transaction {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]
		row := t.Table.Data[op.Key]
		row.LatestUncommitted = op.FromValue
		t.Table.Data[op.Key] = row
	}

	t.locks.UnlockAll()
	t.Operations = make([]Operation, 0)
	t.keysWrittenTo = make(map[Key]struct{})

	return t
}

func (t *ReadUncommitted) Commit() Transaction {
	for _, op := range t.Operations {
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	t.locks.UnlockAll()
	t.Operations = make([]Operation, 0)
	t.keysWrittenTo = make(map[Key]struct{})

	return t
}

func (t *ReadUncommitted) GetKeysWrittenTo() []Key {
	res := make([]Key, 0)

	for key := range t.keysWrittenTo {
		res = append(res, key)
	}

	return res
}

func (t *ReadUncommitted) GetLocks() *TransactionLocks {
	return t.locks
}
