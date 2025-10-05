package main

import (
	"strings"
	"testing"
)

func TestParticipantOrdering(t *testing.T) {
	tests := []struct {
		name              string
		setupParticipants func(*MermaidBuilder)
		expectedOrder     []string
		description       string
	}{
		{
			name: "four transactions with equal split",
			setupParticipants: func(mb *MermaidBuilder) {
				mb.EnsureParticipantAdded("row1", rowParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("row2", rowParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t1", transactionParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t2", transactionParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t3", transactionParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t4", transactionParticipant, Materialized, Static)

				mb.participantsUsed["row1"] = struct{}{}
				mb.participantsUsed["row2"] = struct{}{}
				mb.participantsUsed["t1"] = struct{}{}
				mb.participantsUsed["t2"] = struct{}{}
				mb.participantsUsed["t3"] = struct{}{}
				mb.participantsUsed["t4"] = struct{}{}
			},
			expectedOrder: []string{"t1", "t2", "row1", "row2", "t3", "t4"},
			description:   "With 4 transactions (even number), 2 before rows, 2 after rows - equal split",
		},
		{
			name: "single transaction with rows",
			setupParticipants: func(mb *MermaidBuilder) {
				mb.EnsureParticipantAdded("row1", rowParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t1", transactionParticipant, Materialized, Static)

				mb.participantsUsed["row1"] = struct{}{}
				mb.participantsUsed["t1"] = struct{}{}
			},
			expectedOrder: []string{"t1", "row1"},
			description:   "With 1 transaction (left bias), it comes before all rows",
		},
		{
			name: "three transactions with rows",
			setupParticipants: func(mb *MermaidBuilder) {
				mb.EnsureParticipantAdded("row1", rowParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t1", transactionParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t2", transactionParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t3", transactionParticipant, Materialized, Static)

				mb.participantsUsed["row1"] = struct{}{}
				mb.participantsUsed["t1"] = struct{}{}
				mb.participantsUsed["t2"] = struct{}{}
				mb.participantsUsed["t3"] = struct{}{}
			},
			expectedOrder: []string{"t1", "t2", "row1", "t3"},
			description:   "With 3 transactions (left bias), 2 before rows, 1 after rows",
		},
		{
			name: "only rows, no transactions",
			setupParticipants: func(mb *MermaidBuilder) {
				mb.EnsureParticipantAdded("row1", rowParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("row2", rowParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("row3", rowParticipant, Materialized, Static)

				mb.participantsUsed["row1"] = struct{}{}
				mb.participantsUsed["row2"] = struct{}{}
				mb.participantsUsed["row3"] = struct{}{}
			},
			expectedOrder: []string{"row1", "row2", "row3"},
			description:   "Only rows should appear in their original order",
		},
		{
			name: "only transactions, no rows",
			setupParticipants: func(mb *MermaidBuilder) {
				mb.EnsureParticipantAdded("t1", transactionParticipant, Materialized, Static)
				mb.EnsureParticipantAdded("t2", transactionParticipant, Materialized, Static)

				mb.participantsUsed["t1"] = struct{}{}
				mb.participantsUsed["t2"] = struct{}{}
			},
			expectedOrder: []string{"t1", "t2"},
			description:   "Only transactions should appear in their original order",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mb := NewMermaidBuilder()
			tt.setupParticipants(mb)

			diagram := mb.Build()

			actualOrder := extractParticipantOrder(diagram)

			if len(actualOrder) != len(tt.expectedOrder) {
				t.Errorf("\n%s\nExpected %d participants, got %d\nExpected: %v\nActual: %v",
					tt.description, len(tt.expectedOrder), len(actualOrder), tt.expectedOrder, actualOrder)
				return
			}

			for i := range tt.expectedOrder {
				if actualOrder[i] != tt.expectedOrder[i] {
					t.Errorf("\n%s\nParticipant order mismatch at position %d\nExpected: %v\nActual: %v",
						tt.description, i, tt.expectedOrder, actualOrder)
					return
				}
			}
		})
	}
}

func extractParticipantOrder(diagram string) []string {
	lines := strings.Split(diagram, "\n")
	var participants []string

	for _, line := range lines {
		trimmed := strings.TrimSpace(line)

		if strings.HasPrefix(trimmed, "actor ") {
			name := strings.TrimPrefix(trimmed, "actor ")
			participants = append(participants, name)
		} else if strings.HasPrefix(trimmed, "participant ") {
			name := strings.TrimPrefix(trimmed, "participant ")
			participants = append(participants, name)
		}
	}

	return participants
}
