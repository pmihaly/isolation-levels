package main

type ParticipantType int

const (
	transactionParticipant ParticipantType = iota
	snapshotParticipant
	rowParticipant
)
const NO_ALIGNMENT = "no_alignment"

type Participants struct {
	participantTypesByName map[string]ParticipantType
}

func NewParticipants() *Participants {
	return &Participants{
		participantTypesByName: make(map[string]ParticipantType),
	}
}

func (participants *Participants) EnsureParticipantAdded(name string, participantType ParticipantType, rightOf string) {
	participants.participantTypesByName[name] = participantType
}

func (participants *Participants) AddToMermaid(mermaid *MermaidBuilder) {
	// TODO
	// sort by the following template: txns rows txns (keep all the txns evenly distribute on both sides as much as possible)
	// if a participant has a rightOf other than no_alignment, display that participant right of the value of that attribute

	for name, participantType := range participants.participantTypesByName {
		if participantType == transactionParticipant {
			mermaid.AddParticipant(name, "actor")
			continue
		}
		mermaid.AddParticipant(name, "participant")
	}
}
