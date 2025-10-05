package main

import (
	"fmt"
	"sync"
)

type ParticipantType int

const (
	transactionParticipant ParticipantType = iota
	snapshotParticipant
	rowParticipant
)

type ArrowType int

const (
	solid ArrowType = iota
	dotted
)

type ArrowMaterialization int

const (
	asMaterialized ArrowMaterialization = iota
	asUnmaterialized
	materializeOpposite
)

type ParticipantMaterialization int

const (
	Materialized ParticipantMaterialization = iota
	Unmaterialized
)

type ParticipantDynamism int

const (
	Static ParticipantDynamism = iota
	Dynamic
)

type MermaidBuilder struct {
	lock                         sync.Mutex
	diagramLines                 []string
	unmaterializedArrowsByFromTo map[ArrowFromTo]int
	arrowFromToByIndex           map[int]ArrowFromTo
	activationLevelByParticipant map[string]int
	participantOrder             []string
	participantsUsed             map[string]struct{}
	participantTypesByName       map[string]ParticipantType
	unmaterializedParticipants   map[string]struct{}
	dynamicallyCreated           map[string]struct{}
}

type ArrowFromTo struct {
	from string
	to   string
}

func (ft *ArrowFromTo) Opposite() ArrowFromTo {
	return ArrowFromTo{
		from: ft.to,
		to:   ft.from,
	}
}

func NewMermaidBuilder() *MermaidBuilder {
	return &MermaidBuilder{
		lock:                         sync.Mutex{},
		diagramLines:                 make([]string, 0),
		unmaterializedArrowsByFromTo: make(map[ArrowFromTo]int),
		arrowFromToByIndex:           make(map[int]ArrowFromTo),
		activationLevelByParticipant: map[string]int{},
		participantsUsed:             make(map[string]struct{}),
		participantTypesByName:       make(map[string]ParticipantType),
		unmaterializedParticipants:   make(map[string]struct{}),
		dynamicallyCreated:           make(map[string]struct{}),
	}
}

func (builder *MermaidBuilder) AddArrow(arrowType ArrowType, from, to, description string, arrowMaterialization ArrowMaterialization) {
	builder.lock.Lock()
	defer builder.lock.Unlock()

	var mermaidArrowType string
	switch arrowType {
	case solid:
		mermaidArrowType = "->>"
	case dotted:
		mermaidArrowType = "-->>"
	}

	if arrowMaterialization != asUnmaterialized {
		builder.participantsUsed[from] = struct{}{}
		builder.participantsUsed[to] = struct{}{}
	}

	fromTo := ArrowFromTo{
		from: from,
		to:   to,
	}

	switch arrowMaterialization {
	case asUnmaterialized:
		builder.unmaterializedArrowsByFromTo[fromTo] = len(builder.diagramLines)
	case materializeOpposite:
		delete(builder.unmaterializedArrowsByFromTo, fromTo.Opposite())
	}
	builder.diagramLines = append(builder.diagramLines, fmt.Sprintf("%v %v %v: %v", from, mermaidArrowType, to, description))
	builder.arrowFromToByIndex[len(builder.diagramLines)-1] = fromTo
}

func (builder *MermaidBuilder) EnsureActivatedOnLevel(desiredActivationLevel int, participant string) {
	builder.lock.Lock()
	defer builder.lock.Unlock()
	builder.participantsUsed[participant] = struct{}{}

	if desiredActivationLevel < 0 {
		return
	}

	if desiredActivationLevel > 2 {
		panic("activationLevel only supported up to 2")
	}

	startingActivationLevel, ok := builder.activationLevelByParticipant[participant]
	if !ok {
		startingActivationLevel = 0
	}

	if desiredActivationLevel < startingActivationLevel {
		for range startingActivationLevel - desiredActivationLevel {
			builder.diagramLines = append(builder.diagramLines, "deactivate "+participant)
		}
		builder.activationLevelByParticipant[participant] = desiredActivationLevel
		return
	}

	for range desiredActivationLevel - startingActivationLevel {
		builder.diagramLines = append(builder.diagramLines, "activate "+participant)
	}
	builder.activationLevelByParticipant[participant] = desiredActivationLevel
}

func (builder *MermaidBuilder) AddNote(participant, note string) {
	builder.lock.Lock()
	defer builder.lock.Unlock()
	builder.participantsUsed[participant] = struct{}{}

	builder.diagramLines = append(builder.diagramLines, fmt.Sprintf("note over %v: %v", participant, note))
}

func (builder *MermaidBuilder) EnsureParticipantAdded(name string, participantType ParticipantType, materialization ParticipantMaterialization, dynamism ParticipantDynamism) {
	builder.lock.Lock()
	defer builder.lock.Unlock()

	if _, alreadyAdded := builder.participantTypesByName[name]; !alreadyAdded {
		builder.participantTypesByName[name] = participantType
		builder.participantOrder = append(builder.participantOrder, name)
	}

	delete(builder.unmaterializedParticipants, name)
	if materialization == Unmaterialized {
		builder.unmaterializedParticipants[name] = struct{}{}
	}

	if dynamism == Static {
		return
	}

	if _, alreadyDynamicallyCreated := builder.dynamicallyCreated[name]; !alreadyDynamicallyCreated {
		builder.dynamicallyCreated[name] = struct{}{}
	}
}

func (builder *MermaidBuilder) EnsureParticipantDestroyed(name string) {
	builder.lock.Lock()
	defer builder.lock.Unlock()

	_, isDynamic := builder.dynamicallyCreated[name]
	_, isUsed := builder.participantsUsed[name]

	if isDynamic && isUsed {
		builder.diagramLines = append(builder.diagramLines, fmt.Sprintf("destroy %v", name))
	}
}

func (builder *MermaidBuilder) Build() string {
	diagram := "sequenceDiagram\n"

	for _, participant := range builder.participantOrder {
		_, isUsed := builder.participantsUsed[participant]
		if !isUsed {
			continue
		}

		_, isUnmaterialized := builder.unmaterializedParticipants[participant]
		if isUnmaterialized {
			continue
		}

		_, isDynamicallyCreated := builder.dynamicallyCreated[participant]
		if isDynamicallyCreated {
			continue
		}

		participantType, _ := builder.participantTypesByName[participant]
		if participantType == transactionParticipant {
			diagram += addPrefixNewline(fmt.Sprintf("actor %v", participant))
			continue
		}

		diagram += addPrefixNewline(fmt.Sprintf("participant %v", participant))
	}

	renderedCreateCommands := make(map[string]struct{})

	for i, line := range builder.diagramLines {
		arrowFromTo, isArrow := builder.arrowFromToByIndex[i]

		if !isArrow {
			diagram += addPrefixNewline(line)
			continue
		}

		_, isExplicitlyUnmaterialized := builder.unmaterializedArrowsByFromTo[arrowFromTo]
		_, fromUnmaterialized := builder.unmaterializedParticipants[arrowFromTo.from]
		_, toUnmaterialized := builder.unmaterializedParticipants[arrowFromTo.to]
		anyParticipantUnmaterialized := fromUnmaterialized || toUnmaterialized

		if isExplicitlyUnmaterialized && anyParticipantUnmaterialized {
			continue
		}

		for _, participantName := range []string{arrowFromTo.from, arrowFromTo.to} {
			if _, isDynamic := builder.dynamicallyCreated[participantName]; !isDynamic {
				continue
			}
			if _, alreadyRendered := renderedCreateCommands[participantName]; alreadyRendered {
				continue
			}
			diagram += addPrefixNewline(fmt.Sprintf("create participant %v", participantName))
			renderedCreateCommands[participantName] = struct{}{}
		}

		diagram += addPrefixNewline(line)
	}

	return diagram
}

func addPrefixNewline(mermaid string) string {
	return "    " + mermaid + "\n"
}
