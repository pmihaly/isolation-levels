package main

type SnapshotIsolation struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	lockedKeys    map[Key]interface{}
}

func NewSnapshotIsolation(transactionId TransactionId, table *Table) *SnapshotIsolation {
	table.TakeSnapshot(transactionId)

	return &SnapshotIsolation{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		lockedKeys:    make(map[Key]interface{}),
	}
}

func (t *SnapshotIsolation) Set(key Key, value Value) Transaction {
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
	row.UncommittedByTxId[t.TransactionId] = value
	(*t.Table).Data[key] = row
	return t
}

func (t *SnapshotIsolation) Get(key Key) Value {
	row, ok := (*t.Table).Data[key]

	if !ok {
		return EmptyValue()
	}

	if uncommitted, ok := row.UncommittedByTxId[t.TransactionId]; ok {
		return uncommitted
	}

	val, ok := t.Table.GetCommitted(key, t.TransactionId)

	if !ok {
		return EmptyValue()
	}

	return val
}

func (t *SnapshotIsolation) Delete(key Key) Transaction {
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
	row.UncommittedByTxId[t.TransactionId] = EmptyValue()
	(*t.Table).Data[key] = row

	return t
}

func (t *SnapshotIsolation) Lock(key Key) Transaction {
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

func (t *SnapshotIsolation) Rollback() Transaction {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]

		row := (*t.Table).Data[op.Key]
		row.UncommittedByTxId[t.TransactionId] = op.FromValue
		(*t.Table).Data[op.Key] = row
	}

	for key := range t.lockedKeys {
		row := (*t.Table).Data[key]
		row.ExclusiveLock.Unlock()
		delete(t.lockedKeys, key)
	}

	t.Table.DeleteSnapshot(t.TransactionId)
	t.Operations = make([]Operation, 0)

	return t
}

func (t *SnapshotIsolation) Commit() Transaction {
	for _, op := range t.Operations {
		t.Lock(op.Key)
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	for key := range t.lockedKeys {
		row := (*t.Table).Data[key]
		row.ExclusiveLock.Unlock()
		delete(t.lockedKeys, key)
	}

	t.Table.DeleteSnapshot(t.TransactionId)
	t.Operations = make([]Operation, 0)

	return t
}
