package main

type ReadCommitted struct {
	TransactionId string
	Data          *map[string]Row
	Operations    []Operation
}

func NewReadCommitted(transactionId string, data *map[string]Row) *ReadCommitted {
	return &ReadCommitted{
		TransactionId: transactionId,
		Data:          data,
		Operations:    make([]Operation, 0),
	}
}

func (t *ReadCommitted) Set(key, value string) Transaction {
	row, ok := (*t.Data)[key]
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
	row.UncommittedByTransactionId[t.TransactionId] = value
	(*t.Data)[key] = row
	return t
}

func (t *ReadCommitted) Get(key string) string {
	row, ok := (*t.Data)[key]

	if !ok {
		return EmptyValue()
	}

	if uncommitted, ok := row.UncommittedByTransactionId[t.TransactionId]; ok {
		return uncommitted
	}

	return row.Committed
}

func (t *ReadCommitted) Delete(key string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		return t
	}

	delete(row.UncommittedByTransactionId, t.TransactionId)

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.LatestUncommitted,
		ToValue:   EmptyValue(),
	})

	row.LatestUncommitted = EmptyValue()
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

func (t *ReadCommitted) Rollback() Transaction {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]
		row := (*t.Data)[op.Key]
		delete(row.UncommittedByTransactionId, t.TransactionId)
		(*t.Data)[op.Key] = row
	}

	t.Operations = make([]Operation, 0)

	return t
}

func (t *ReadCommitted) Commit() Transaction {
	for _, op := range t.Operations {
		row := (*t.Data)[op.Key]

		row.Committed = op.ToValue
		row.LatestUncommitted = op.ToValue

		delete(row.UncommittedByTransactionId, t.TransactionId)

		row.ExclusiveLock.Lock()
		(*t.Data)[op.Key] = row
		row.ExclusiveLock.Unlock()
	}

	t.Operations = make([]Operation, 0)

	return t
}
