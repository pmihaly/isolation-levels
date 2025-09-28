package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"
)

func PlayEvents(events []Event, table *Table) string {
	transactionEvents := make(map[TransactionId][]Event)
	transactionOrder := make([]TransactionId, 0)
	rows := make(map[Key]struct{})

	for i, event := range events {
		transaction := event.TxId

		if _, ok := transactionEvents[transaction]; !ok {
			transactionOrder = append(transactionOrder, transaction)
		}

		transactionEvents[transaction] = append(transactionEvents[transaction], event)
		event.Position = i

		rows[event.Key] = struct{}{}
	}

	mermaid := "sequenceDiagram\n"
	mermaidLock := sync.Mutex{}

	participants := []string{string(EmptyTransactionId())}

	for row := range rows {
		if row == EmptyKey() {
			continue
		}

		participants = append(participants, "participant "+string(row))
	}

	for i, transactionId := range transactionOrder {
		if i == 0 {
			participants[0] = "actor " + string(transactionId)
			continue
		}

		participants = append(participants, "actor "+string(transactionId))
	}

	for _, participant := range participants {
		mermaid += addPrefixNewline(participant, &mermaidLock)
	}

	for key := range rows {
		row, ok := table.Data[key]
		if !ok || key == EmptyKey() {
			continue
		}
		rowJson, err := json.Marshal(row)
		if err == nil {
			mermaid += addPrefixNewline("note over "+string(key)+": "+string(rowJson), &mermaidLock)
		}
	}

	var transactions sync.Map

	var wg sync.WaitGroup
	unblocks := make(map[TransactionId]chan struct{})
	for txId := range transactionEvents {
		unblocks[txId] = make(chan struct{})
	}

	for txId, events := range transactionEvents {
		wg.Add(1)
		go func() {
			defer wg.Done()

			<-unblocks[txId]
			for _, event := range events {
				txVal, ok := transactions.Load(event.TxId)
				tx, _ := txVal.(Transaction)

				if !ok {
					switch event.TxLevel {
					case readUncommitted:
						tx = NewReadUncommitted(event.TxId, table)
					case readCommitted:
						tx = NewReadCommitted(event.TxId, table)
					case snapshotIsolation:
						tx = NewSnapshotIsolation(event.TxId, table)
					case twoPhaseLocking:
						tx = NewTwoPhaseLocking(event.TxId, table)
					default:
						continue
					}

					transactions.Store(event.TxId, tx)
				}

				switch event.OperationType {
				case WriteOperation:
					if event.Key == EmptyKey() {
						continue
					}

					row, found := table.Data[event.Key]

					if found && row.Lock.IsLocked() {
						mermaid += addPrefixNewline(string(event.TxId)+" -->> "+string(event.Key)+": set "+string(event.Key)+" = "+string(event.To), &mermaidLock)
					}

					tx.Set(event.Key, event.To)
					mermaid += addPrefixNewline(string(event.TxId)+" ->> "+string(event.Key)+": set "+string(event.Key)+" = "+string(event.To), &mermaidLock)

					lockLevels := tx.GetLocks().GetLockLevels()
					lockLevel := lockLevels[event.Key]

					if lockLevel >= Read {
						mermaid += addPrefixNewline("activate "+string(event.Key), &mermaidLock)
					}

					if lockLevel == ReadWrite {
						mermaid += addPrefixNewline("activate "+string(event.Key), &mermaidLock)
					}

					mermaid += addPrefixNewline(string(event.Key)+" ->> "+string(event.TxId)+": ok", &mermaidLock)

					row, ok := table.Data[event.Key]
					rowJson, err := json.Marshal(row)
					if ok && err == nil {
						mermaid += addPrefixNewline("note over "+string(event.Key)+": "+string(rowJson), &mermaidLock)
					}

				case ReadOperation:
					row, found := table.Data[event.Key]

					if found && row.Lock.IsLocked() {
						mermaid += addPrefixNewline(string(event.TxId)+" -->> "+string(event.Key)+": get "+string(event.Key), &mermaidLock)
					}

					tx.Get(event.Key)

					mermaid += addPrefixNewline(string(event.TxId)+" ->> "+string(event.Key)+": get "+string(event.Key), &mermaidLock)

					lockLevels := tx.GetLocks().GetLockLevels()
					lockLevel := lockLevels[event.Key]

					if lockLevel >= Read {
						mermaid += addPrefixNewline("activate "+string(event.Key), &mermaidLock)
					}

					if lockLevel == ReadWrite {
						mermaid += addPrefixNewline("activate "+string(event.Key), &mermaidLock)
					}

					mermaid += addPrefixNewline(string(event.Key)+" ->> "+string(event.TxId)+": "+string(event.Key)+" = "+string(tx.Get(event.Key)), &mermaidLock)

				case Commit:
					keysWrittenTo := tx.GetKeysWrittenTo()

					for _, key := range keysWrittenTo {
						mermaid += addPrefixNewline(string(event.TxId)+" ->> "+string(key)+": commit", &mermaidLock)
					}
					lockLevels := tx.GetLocks().GetLockLevels()

					tx.Commit()

					for _, key := range keysWrittenTo {
						mermaid += addPrefixNewline(string(key)+" ->> "+string(event.TxId)+": ok", &mermaidLock)

						lockLevel := lockLevels[key]

						if lockLevel >= Read {
							mermaid += addPrefixNewline("deactivate "+string(key), &mermaidLock)
						}

						if lockLevel == ReadWrite {
							mermaid += addPrefixNewline("deactivate "+string(key), &mermaidLock)
						}

						row, ok := table.Data[key]
						rowJson, err := json.Marshal(row)
						if ok && err == nil {
							mermaid += addPrefixNewline("note over "+string(key)+": "+string(rowJson), &mermaidLock)
						}
					}
				}

				// TODO rollback
				// TODO read from snapshot
			}
		}()

	}

	for _, txId := range transactionOrder {
		close(unblocks[txId])
		time.Sleep(10 * time.Millisecond)
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		return mermaid
	case <-time.After(3 * time.Second):
		log.Print("timed out after 3 secs")
		return mermaid
	}
}

func addPrefixNewline(mermaid string, mermaidLock *sync.Mutex) string {
	mermaidLock.Lock()
	defer mermaidLock.Unlock()
	return "    " + mermaid + "\n"
}
