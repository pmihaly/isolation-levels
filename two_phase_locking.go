package main

type TwoPhaseLocking struct {
	TransactionId TransactionId
	Table         *Table
	Operations    []Operation
	locks         *TransactionLocks
	keysTouched   map[Key]struct{}
}

func NewTwoPhaseLocking(transactionId TransactionId, table *Table) *TwoPhaseLocking {
	return &TwoPhaseLocking{
		TransactionId: transactionId,
		Table:         table,
		Operations:    make([]Operation, 0),
		locks:         NewTransactionLocks(),
		keysTouched:   make(map[Key]struct{}),
	}
}

func (t *TwoPhaseLocking) Set(key Key, value Value) Transaction {
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

	t.locks.Lock(ReadWrite, t.TransactionId, &row)

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

func (t *TwoPhaseLocking) Get(key Key) Value {
	t.Table.EnsureSnapshotTaken(t.TransactionId)

	row, ok := t.Table.Data[key]

	if !ok {
		return EmptyValue()
	}

	t.locks.Lock(Read, t.TransactionId, &row)
	t.keysTouched[key] = struct{}{}

	if uncommitted, ok := row.UncommittedByTxId[t.TransactionId]; ok {
		return uncommitted
	}

	val, _ := t.Table.GetCommitted(key, t.TransactionId)
	return val
}

func (t *TwoPhaseLocking) Lock(key Key) Transaction {
	t.Table.EnsureSnapshotTaken(t.TransactionId)

	row, ok := t.Table.Data[key]

	if !ok {
		return t
	}

	t.locks.Lock(ReadWrite, t.TransactionId, &row)
	return t
}

func (t *TwoPhaseLocking) Rollback() Transaction {
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

func (t *TwoPhaseLocking) Commit() Transaction {
	for _, op := range t.Operations {
		t.Table.SetCommitted(op.Key, op.ToValue, t.TransactionId)
	}

	t.locks.UnlockAll()
	t.Table.DeleteSnapshot(t.TransactionId)
	t.Operations = make([]Operation, 0)
	t.keysTouched = make(map[Key]struct{})

	return t
}

func (t *TwoPhaseLocking) GetKeysTouched() []Key {
	res := make([]Key, 0)

	for key := range t.keysTouched {
		res = append(res, key)
	}

	return res
}

func (t *TwoPhaseLocking) GetLocks() *TransactionLocks {
	return t.locks
}
