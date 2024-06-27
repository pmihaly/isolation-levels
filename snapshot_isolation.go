package main

type SnapshotIsolation struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	locks         *TransactionLocks
}

func NewSnapshotIsolation(transactionId TransactionId, table *Table) *SnapshotIsolation {
	table.TakeSnapshot(transactionId)

	return &SnapshotIsolation{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		locks:         NewTransactionLocks(),
	}
}

func (t *SnapshotIsolation) Set(key Key, value Value) Transaction {
	row, ok := t.Table.Data[key]
	prevValue, prevOk := row.UncommittedByTxId[t.TransactionId]

	if !prevOk {
		prevValue = row.Committed
	}

	if !ok {
		row = NewRow(key, value)
		prevValue = EmptyValue()
	}

	didILock := t.locks.Lock(&row, Write)
	if didILock {
		defer t.locks.Unlock(&row, Write)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: prevValue,
		ToValue:   value,
	})

	row.LatestUncommitted = value
	row.UncommittedByTxId[t.TransactionId] = value
	t.Table.Data[key] = row
	return t
}

func (t *SnapshotIsolation) Get(key Key) Value {
	row, ok := t.Table.Data[key]

	if !ok {
		return EmptyValue()
	}

	didILock := t.locks.Lock(&row, Read)
	if didILock {
		defer t.locks.Unlock(&row, Read)
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
	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	didILock := t.locks.Lock(&row, Write)
	if didILock {
		defer t.locks.Unlock(&row, Write)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.UncommittedByTxId[t.TransactionId],
		ToValue:   EmptyValue(),
	})

	row.LatestUncommitted = EmptyValue()
	row.UncommittedByTxId[t.TransactionId] = EmptyValue()
	t.Table.Data[key] = row

	return t
}

func (t *SnapshotIsolation) Lock(key Key) Transaction {
	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	t.locks.Lock(&row, Write)
	return t
}

func (t *SnapshotIsolation) Rollback() Transaction {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]

		row := t.Table.Data[op.Key]
		row.UncommittedByTxId[t.TransactionId] = op.FromValue
		t.Table.Data[op.Key] = row
	}

	t.locks.UnlockAll()
	t.Table.DeleteSnapshot(t.TransactionId)
	t.Operations = make([]Operation, 0)

	return t
}

func (t *SnapshotIsolation) Commit() Transaction {
	for _, op := range t.Operations {
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	t.locks.UnlockAll()
	t.Table.DeleteSnapshot(t.TransactionId)
	t.Operations = make([]Operation, 0)

	return t
}
