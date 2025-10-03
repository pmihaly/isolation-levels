package main

type SnapshotIsolation struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	locks         *TransactionLocks
	keysTouched   map[Key]struct{}
}

func NewSnapshotIsolation(transactionId TransactionId, table *Table) *SnapshotIsolation {
	return &SnapshotIsolation{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		locks:         NewTransactionLocks(),
		keysTouched:   make(map[Key]struct{}),
	}
}

func (t *SnapshotIsolation) Set(key Key, value Value) Transaction {
	t.Table.EnsureSnapshotTaken(t.TransactionId)

	row, ok := t.Table.Data[key]
	prevValue, prevOk := row.UncommittedByTxId[t.TransactionId]

	if !prevOk {
		prevValue = row.Committed
	}

	if !ok {
		row = NewRow(key, value)
		prevValue = EmptyValue()
	}

	didILock := t.locks.Lock(ReadWrite, t.TransactionId, &row)
	if didILock {
		defer t.locks.Unlock(&row)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: prevValue,
		ToValue:   value,
	})

	t.keysTouched[key] = struct{}{}
	row.LatestUncommitted = value
	row.UncommittedByTxId[t.TransactionId] = value
	t.Table.Data[key] = row
	return t
}

func (t *SnapshotIsolation) Get(key Key) Value {
	t.Table.EnsureSnapshotTaken(t.TransactionId)

	row, ok := t.Table.Data[key]

	if !ok {
		return EmptyValue()
	}

	didILock := t.locks.Lock(Read, t.TransactionId, &row)
	if didILock {
		defer t.locks.Unlock(&row)
	}

	t.keysTouched[key] = struct{}{}

	if uncommitted, ok := row.UncommittedByTxId[t.TransactionId]; ok {
		return uncommitted
	}

	val, _ := t.Table.GetCommitted(key, t.TransactionId)
	return val
}

func (t *SnapshotIsolation) Delete(key Key) Transaction {
	t.Table.EnsureSnapshotTaken(t.TransactionId)

	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	didILock := t.locks.Lock(ReadWrite, t.TransactionId, &row)
	if didILock {
		defer t.locks.Unlock(&row)
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.UncommittedByTxId[t.TransactionId],
		ToValue:   EmptyValue(),
	})

	if _, ok := t.keysTouched[key]; ok {
		delete(t.keysTouched, key)
	} else {
		t.keysTouched[key] = struct{}{}
	}

	row.LatestUncommitted = EmptyValue()
	row.UncommittedByTxId[t.TransactionId] = EmptyValue()
	t.Table.Data[key] = row

	return t
}

func (t *SnapshotIsolation) Lock(key Key) Transaction {
	t.Table.EnsureSnapshotTaken(t.TransactionId)

	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	t.locks.Lock(ReadWrite, t.TransactionId, &row)
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
	t.keysTouched = make(map[Key]struct{})

	return t
}

func (t *SnapshotIsolation) Commit() Transaction {
	for _, op := range t.Operations {
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	t.locks.UnlockAll()
	t.Table.DeleteSnapshot(t.TransactionId)
	t.Operations = make([]Operation, 0)
	t.keysTouched = make(map[Key]struct{})

	return t
}

func (t *SnapshotIsolation) GetKeysTouched() []Key {
	res := make([]Key, 0)

	for key := range t.keysTouched {
		res = append(res, key)
	}

	return res
}

func (t *SnapshotIsolation) GetLocks() *TransactionLocks {
	return t.locks
}
