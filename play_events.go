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

	mermaid := NewMermaidBuilder()

	for row := range rows {
		if row == EmptyKey() {
			continue
		}

		mermaid.EnsureParticipantAdded(string(row), rowParticipant)
	}

	for _, transactionId := range transactionOrder {
		mermaid.EnsureParticipantAdded(string(transactionId), transactionParticipant)
	}

	for key := range rows {
		row, ok := table.Data[key]
		if !ok || key == EmptyKey() {
			continue
		}
		rowJson, err := json.Marshal(row)
		if err == nil {
			mermaid.AddNote(string(key), string(rowJson))
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
				isUsingSnapshots := event.TxLevel >= snapshotIsolation

				switch event.OperationType {
				case WriteOperation:
					if event.Key == EmptyKey() {
						continue
					}

					row, found := table.Data[event.Key]

					if found && row.Lock.IsBlocked(event.TxId) {
						mermaid.AddArrow(dotted, string(event.TxId), string(event.Key), fmt.Sprintf("set %v = %v", event.Key, event.To), asMaterialized)
					}

					tx.Set(event.Key, event.To)

					if isUsingSnapshots {
						mermaid.EnsureParticipantAdded(toSnapshotName(event.TxId, event.Key), snapshotParticipant)
						mermaid.AddArrow(solid, string(event.TxId), toSnapshotName(event.TxId, event.Key), fmt.Sprintf("set %v = %v", event.Key, event.To), asUnmaterialized)
					}

					mermaid.AddArrow(solid, string(event.TxId), string(event.Key), fmt.Sprintf("set %v = %v", event.Key, event.To), asMaterialized)
					mermaid.EnsureParticipantAdded(string(event.Key), rowParticipant)

					lockLevels := tx.GetLocks().GetLockLevels()
					lockLevel := lockLevels[event.Key]

					activationLevel := 0
					if lockLevel >= Read {
						activationLevel += 1
					}

					if lockLevel == ReadWrite {
						activationLevel += 1
					}

					mermaid.EnsureActivatedOnLevel(activationLevel, string(event.Key))
					mermaid.AddArrow(solid, string(event.Key), string(event.TxId), "ok", asMaterialized)

					row, ok := table.Data[event.Key]
					rowJson, err := json.Marshal(row)
					if ok && err == nil {
						mermaid.AddNote(string(event.Key), string(rowJson))
					}

				case ReadOperation:
					row, found := table.Data[event.Key]

					if found && row.Lock.IsBlocked(event.TxId) {
						mermaid.AddArrow(dotted, string(event.TxId), string(event.Key), ": get "+string(event.Key), asMaterialized)
					}

					value := tx.Get(event.Key)

					readTarget := string(event.Key)

					_, hasSnapshots := table.GetSnapshot(txId)

					if isUsingSnapshots && hasSnapshots {
						readTarget = toSnapshotName(event.TxId, event.Key)
						mermaid.EnsureParticipantAdded(readTarget, snapshotParticipant)
					}

					mermaid.AddArrow(solid, string(event.TxId), readTarget, "get "+string(event.Key), materializeOpposite)

					lockLevels := tx.GetLocks().GetLockLevels()
					lockLevel := lockLevels[event.Key]

					if lockLevel < Read {
						mermaid.EnsureActivatedOnLevel(1, string(event.Key))
					}

					mermaid.AddArrow(solid, readTarget, string(event.TxId), fmt.Sprintf("%v = %v", event.Key, value), asMaterialized)

				case Commit:
					keysTouched := tx.GetKeysTouched()
					tx.Commit()
					for _, key := range keysTouched {
						mermaid.AddArrow(solid, string(event.TxId), string(key), "commit", asMaterialized)
					}

					for _, key := range keysTouched {
						mermaid.AddArrow(solid, string(key), string(event.TxId), "ok", asMaterialized)
						mermaid.EnsureActivatedOnLevel(0, string(key))

						row, ok := table.Data[key]
						rowJson, err := json.Marshal(row)
						if ok && err == nil {
							mermaid.AddNote(string(key), string(rowJson))
						}
					}
				}

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
		return mermaid.Build()
	case <-time.After(3 * time.Second):
		log.Print("timed out after 3 secs")
		return mermaid.Build()
	}
}

func toSnapshotName(txId TransactionId, key Key) string {
	return string(txId) + " snapshot of " + string(key)
}
