package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

const TIMEOUT_SECS = 3
const STAGGER_DELAY_MILLIS = 10

func PlayEvents(events []Event, table *Table) (string, error) {
	if len(events) == 0 {
		return "", nil
	}

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

		mermaid.EnsureParticipantAdded(string(row), RowParticipant, Materialized, Static)
	}

	for _, transactionId := range transactionOrder {
		mermaid.EnsureParticipantAdded(string(transactionId), TransactionParticipant, Materialized, Static)
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
		go func() error {
			defer wg.Done()

			<-unblocks[txId]
			for _, event := range events {
				txVal, ok := transactions.Load(event.TxId)
				tx, _ := txVal.(Transaction)

				if !ok {
					tx, err := TransactionFromTransactionLevel(event.TxLevel, event.TxId, table)
					if err != nil {
						return nil
					}

					transactions.Store(event.TxId, tx)
				}
				isUsingSnapshots := event.TxLevel >= SnapshotIsolationLevel

				switch event.OperationType {
				case WriteOperation:
					if event.Key == EmptyKey() {
						continue
					}

					row, found := table.Data[event.Key]

					if found && row.Lock.IsBlocked(event.TxId) {
						mermaid.AddArrow(Dotted, string(event.TxId), string(event.Key), fmt.Sprintf("set %v = %v", event.Key, event.To), AsMaterialized)
					}

					tx.Set(event.Key, event.To)

					if isUsingSnapshots {
						snapshotName := toSnapshotName(event.TxId, event.Key)
						mermaid.EnsureParticipantAdded(snapshotName, SnapshotParticipant, Unmaterialized, Dynamic)
						mermaid.AddArrow(Solid, string(event.TxId), snapshotName, fmt.Sprintf("set %v = %v", event.Key, event.To), AsUnmaterialized)
					}

					mermaid.AddArrow(Solid, string(event.TxId), string(event.Key), fmt.Sprintf("set %v = %v", event.Key, event.To), AsMaterialized)
					mermaid.EnsureParticipantAdded(string(event.Key), RowParticipant, Materialized, Static)

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
					mermaid.AddArrow(Solid, string(event.Key), string(event.TxId), "ok", AsMaterialized)

					row, ok := table.Data[event.Key]
					rowJson, err := json.Marshal(row)
					if ok && err == nil {
						mermaid.AddNote(string(event.Key), string(rowJson))
					}

				case ReadOperation:
					if event.Key == EmptyKey() {
						continue
					}

					row, found := table.Data[event.Key]

					if found && row.Lock.IsBlocked(event.TxId) {
						mermaid.AddArrow(Dotted, string(event.TxId), string(event.Key), ": get "+string(event.Key), AsMaterialized)
					}

					value := tx.Get(event.Key)

					readTarget := string(event.Key)

					_, hasSnapshots := table.GetSnapshot(txId)

					if isUsingSnapshots && hasSnapshots {
						readTarget = toSnapshotName(event.TxId, event.Key)
						mermaid.EnsureParticipantAdded(readTarget, SnapshotParticipant, Materialized, Dynamic)
					}

					mermaid.AddArrow(Solid, string(event.TxId), readTarget, "get "+string(event.Key), MaterializeOpposite)

					lockLevels := tx.GetLocks().GetLockLevels()
					lockLevel := lockLevels[event.Key]

					if lockLevel < Read {
						mermaid.EnsureActivatedOnLevel(1, string(event.Key))
					}

					mermaid.AddArrow(Solid, readTarget, string(event.TxId), fmt.Sprintf("%v = %v", event.Key, value), AsMaterialized)

				case Commit:
					if event.Key == EmptyKey() {
						continue
					}

					keysTouched := tx.GetKeysTouched()
					tx.Commit()
					for _, key := range keysTouched {
						mermaid.AddArrow(Solid, string(event.TxId), string(key), "commit", AsMaterialized)
					}

					for _, key := range keysTouched {
						mermaid.AddArrow(Solid, string(key), string(event.TxId), "ok", AsMaterialized)
						mermaid.EnsureActivatedOnLevel(0, string(key))

						row, ok := table.Data[key]
						rowJson, err := json.Marshal(row)
						if ok && err == nil {
							mermaid.AddNote(string(key), string(rowJson))
						}
					}

					if isUsingSnapshots {
						for _, key := range keysTouched {
							snapshotName := toSnapshotName(event.TxId, key)
							mermaid.EnsureParticipantDestroyed(snapshotName)
						}
					}
				}

			}
			return nil
		}()

	}

	for _, txId := range transactionOrder {
		close(unblocks[txId])
		time.Sleep(STAGGER_DELAY_MILLIS * time.Millisecond)
	}

	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()

	select {
	case <-c:
		return mermaid.Build(), nil
	case <-time.After(TIMEOUT_SECS * time.Second):
		return mermaid.Build(), fmt.Errorf("timed out after %v secs", TIMEOUT_SECS)
	}
}

func toSnapshotName(txId TransactionId, key Key) string {
	return string(txId) + " snapshot of " + string(key)
}
