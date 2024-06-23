package main

type ReadUncommitted struct {
	TransactionId string
	Data          *map[string]Row
	Operations    []Operation
}

func (t *ReadUncommitted) Set(key, value string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		t.Operations = append(t.Operations, Operation{
			Key:       key,
			FromValue: EmptyValue(),
			ToValue:   value,
		})
		(*t.Data)[key] = Row{Key: key, Committed: EmptyValue(), Uncommitted: value}
		return t
	}

	t.Operations = append(t.Operations, Operation{
		Key:       key,
		FromValue: (*t.Data)[key].Uncommitted,
		ToValue:   value,
	})

	row.Uncommitted = value
	(*t.Data)[key] = row
	return t
}

func (t *ReadUncommitted) Get(key string) (Transaction, string) {
	row, ok := (*t.Data)[key]

	if !ok {
		return t, EmptyValue()
	}

	return t, row.Uncommitted
}

func (t *ReadUncommitted) Delete(key string) Transaction {
	row, ok := (*t.Data)[key]

	if !ok {
		return t
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

func (t *ReadUncommitted) Rollback() {
	for i := len(t.Operations) - 1; i >= 0; i-- {
		op := t.Operations[i]
		row := (*t.Data)[op.Key]
		row.Uncommitted = op.FromValue
		(*t.Data)[op.Key] = row
	}

	t.Operations = make([]Operation, 0)
}

func (t *ReadUncommitted) Commit() {
	for _, op := range t.Operations {
		row := (*t.Data)[op.Key]
		row.Committed = op.ToValue
		(*t.Data)[op.Key] = row
	}

	t.Operations = make([]Operation, 0)
}
