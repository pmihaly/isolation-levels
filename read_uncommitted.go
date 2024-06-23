package main

type ReadUncommitted struct {
	TransactionId string
	Data          *map[string]Row
	Operations    []Operation
	lockedKeys    map[string]interface{}
}

func NewReadUncommitted(transactionId string, data *map[string]Row) *ReadUncommitted {
	return &ReadUncommitted{
		TransactionId: transactionId,
		Data:          data,
		Operations:    make([]Operation, 0),
		lockedKeys:    make(map[string]interface{}),
	}
}

func (t *ReadUncommitted) Set(key, value string) Transaction {
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
	(*t.Data)[key] = row
	return t
}

func (t *ReadUncommitted) Get(key string) string {
	row, ok := (*t.Data)[key]

	if !ok {
		return EmptyValue()
	}

	return row.LatestUncommitted
}

func (t *ReadUncommitted) Delete(key string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		return t
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: row.LatestUncommitted,
		ToValue:   EmptyValue(),
	})

	row.LatestUncommitted = EmptyValue()
	(*t.Data)[key] = row

	return t
}

func (t *ReadUncommitted) Lock(key string) Transaction {
	row, ok := (*t.Data)[key]

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
		row := (*t.Data)[op.Key]
		row.LatestUncommitted = op.FromValue
		(*t.Data)[op.Key] = row

		if _, ok := t.lockedKeys[op.Key]; ok {
			row.ExclusiveLock.Unlock()
			delete(t.lockedKeys, op.Key)
		}
	}

	t.Operations = make([]Operation, 0)

	return t
}

func (t *ReadUncommitted) Commit() Transaction {
	for _, op := range t.Operations {
		row := (*t.Data)[op.Key]

		row.Committed = op.ToValue

		t.Lock(op.Key)
		(*t.Data)[op.Key] = row

		if _, ok := t.lockedKeys[op.Key]; ok {
			row.ExclusiveLock.Unlock()
			delete(t.lockedKeys, op.Key)
		}
	}

	t.Operations = make([]Operation, 0)

	return t
}
