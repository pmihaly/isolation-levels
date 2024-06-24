package main

type ReadUncommitted struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	lockedKeys    map[Key]interface{}
}

func NewReadUncommitted(transactionId TransactionId, table *Table) *ReadUncommitted {
	return &ReadUncommitted{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		lockedKeys:    make(map[Key]interface{}),
	}
}

func (t *ReadUncommitted) Set(key Key, value Value) Transaction {
	row, ok := (*t.Table).Data[key]
	prevValue := row.LatestUncommitted

	if !ok {
		row = NewRow(key, value)
		prevValue = EmptyValue()
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

	return row.LatestUncommitted
}

func (t *ReadUncommitted) Delete(key Key) Transaction {
	row, ok := (*t.Table).Data[key]

	if !ok {
		return t
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

	if _, ok := t.lockedKeys[key]; ok {
		return t
	}

	t.lockedKeys[key] = nil
	row.ExclusiveLock.Lock()
	return t
}

func (t *ReadUncommitted) Rollback() Transaction {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]
		row := (*t.Table).Data[op.Key]
		row.LatestUncommitted = op.FromValue
		(*t.Table).Data[op.Key] = row
	}

	for key := range t.lockedKeys {
		row := (*t.Table).Data[key]
		row.ExclusiveLock.Unlock()
		delete(t.lockedKeys, key)
	}

	t.Operations = make([]Operation, 0)

	return t
}

func (t *ReadUncommitted) Commit() Transaction {
	for _, op := range t.Operations {
		t.Lock(op.Key)
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	for key := range t.lockedKeys {
		row := (*t.Table).Data[key]
		row.ExclusiveLock.Unlock()
		delete(t.lockedKeys, key)
	}

	t.Operations = make([]Operation, 0)

	return t
}
