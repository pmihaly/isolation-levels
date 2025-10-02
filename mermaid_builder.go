package main

import (
	"fmt"
	"sync"
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

type MermaidBuilder struct {
	lock         sync.Mutex
	diagramLines []string
	// unmaterializedArrowsByFromTo map[string]int
	activationLevelByParticipant map[string]int
	participantLines             []string
}

func NewMermaidBuilder() *MermaidBuilder {
	return &MermaidBuilder{
		lock:                         sync.Mutex{},
		diagramLines:                 make([]string, 0),
		activationLevelByParticipant: map[string]int{},
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

	builder.diagramLines = append(builder.diagramLines, fmt.Sprintf("%v %v %v: %v", from, mermaidArrowType, to, description))
	//	switch arrowMaterialization {
	//		case asUnmaterialized:
	//			builder.unmaterializedArrowsByFromTo[from+to] = len(builder.diagramLines)
	//		case materializeOpposite:
	//			delete(builder.unmaterializedArrowsByFromTo, to+from)
	//	}
}

func (builder *MermaidBuilder) EnsureActivatedOnLevel(desiredActivationLevel int, participant string) {
	builder.lock.Lock()
	defer builder.lock.Unlock()

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

	builder.diagramLines = append(builder.diagramLines, fmt.Sprintf("note over %v: %v", participant, note))
}

func (builder *MermaidBuilder) AddParticipant(name string, participantType string) {
	builder.lock.Lock()
	defer builder.lock.Unlock()

	builder.participantLines = append(builder.participantLines, fmt.Sprintf("%v %v", participantType, name))
}

func (builder *MermaidBuilder) Build() string {
	diagram := "sequenceDiagram\n"

	for _, participant := range builder.participantLines {
		diagram += addPrefixNewline(participant)
	}

	for _, line := range builder.diagramLines {
		diagram += addPrefixNewline(line)
	}

	return diagram
}

func addPrefixNewline(mermaid string) string {
	return "    " + mermaid + "\n"
}
