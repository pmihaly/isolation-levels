package main

type ReadUncommitted struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	locks         *TransactionLocks
}

func NewReadUncommitted(transactionId TransactionId, table *Table) *ReadUncommitted {
	return &ReadUncommitted{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		locks:         NewTransactionLocks(),
	}
}

func (t *ReadUncommitted) Set(key Key, value Value) Transaction {
	row, ok := (*t.Table).Data[key]
	prevValue := row.LatestUncommitted

	if !ok {
		row = NewRow(key, value)
		prevValue = EmptyValue()
	}

	didLock := t.locks.WLock(&row)
	if didLock {
		defer t.locks.WUnlock(&row)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: prevValue,
		ToValue:   value,
	})

	row.LatestUncommitted = value
	(*t.Table).Data[key] = row
	return t
}

func (t *ReadUncommitted) Get(key Key) Value {
	row, ok := (*t.Table).Data[key]

	if !ok {
		return EmptyValue()
	}

	didLock := t.locks.RLock(&row)
	if didLock {
		defer t.locks.RUnlock(&row)
	}

	return row.LatestUncommitted
}

func (t *ReadUncommitted) Delete(key Key) Transaction {
	row, ok := (*t.Table).Data[key]

	if !ok {
		return t
	}

	didLock := t.locks.WLock(&row)
	if didLock {
		defer t.locks.WUnlock(&row)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.LatestUncommitted,
		ToValue:   EmptyValue(),
	})

	row.LatestUncommitted = EmptyValue()
	(*t.Table).Data[key] = row

	return t
}

func (t *ReadUncommitted) Lock(key Key) Transaction {
	row, ok := (*t.Table).Data[key]

	if !ok {
		return t
	}

	t.locks.WLock(&row)

	return t
}

func (t *ReadUncommitted) Rollback() Transaction {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]
		row := (*t.Table).Data[op.Key]
		row.LatestUncommitted = op.FromValue
		(*t.Table).Data[op.Key] = row
	}

	t.locks.UnlockAll()
	t.Operations = make([]Operation, 0)

	return t
}

func (t *ReadUncommitted) Commit() Transaction {
	for _, op := range t.Operations {
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	t.locks.UnlockAll()
	t.Operations = make([]Operation, 0)

	return t
}
