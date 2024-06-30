package main

import (
	"encoding/json"
	"fmt"
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

	// transactions := make(sync.Map[TransactionId]Transaction)
	var transactions sync.Map

	var wg sync.WaitGroup
	for _, events := range transactionEvents {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for _, event := range events {
				i := event.Position

				txVal, ok := transactions.Load(event.TxId)
				tx, _ := txVal.(Transaction)

				if !ok {
					switch event.TxLevel {
					case readUncommitted:
						tx = NewReadUncommitted(event.TxId, table)
						log.Print(event.TxId + ".created")
					case readCommitted:
						tx = NewReadCommitted(event.TxId, table)
						log.Print(event.TxId + ".created")
					case snapshotIsolation:
						tx = NewSnapshotIsolation(event.TxId, table)
						x, _ := json.Marshal(table)
						log.Printf("%v", string(x))
						log.Print(event.TxId + ".created")
						// take snapshot
					case twoPhaseLocking:
						tx = NewTwoPhaseLocking(event.TxId, table)
						log.Print(event.TxId + ".created")
						// take snapshot
					default:
						continue
					}

					transactions.Store(event.TxId, tx)
				}

				switch event.OperationType {
				case WriteOperation:
					log.Print(event.TxId + ".write")
					if event.Key == EmptyKey() {
						continue
					}

					// tx.isBlocked(key) -> bool - add dotted arrow and stepDone

					mermaid += addPrefixNewline(string(event.TxId)+" -->> "+string(event.Key)+": set "+string(event.Key)+" = "+string(event.To), &mermaidLock)
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
					log.Print(event.TxId + ".read")
					// tx.isBlocked(key) -> bool - add dotted arrow and stepDone

					mermaid += addPrefixNewline(string(event.TxId)+" -->> "+string(event.Key)+": get "+string(event.Key), &mermaidLock)

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
					log.Print(event.TxId + ".commit")
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

				log.Print(string(event.TxId) + "(" + fmt.Sprint(i) + ") finished")
			}
		}()

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
