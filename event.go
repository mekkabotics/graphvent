package graphvent

import (
  "time"
  "fmt"
)

type EventExt struct {
  Name string `"name"`
  State string `"state"`
  Parent *NodeID `"parent"`
}

func NewEventExt(parent *NodeID, name string) *EventExt {
  return &EventExt{
    Name: name,
    State: "init",
    Parent: parent,
  }
}

type EventStateSignal struct {
  SignalHeader
  Source NodeID
  State string
  Time time.Time
}

func (signal EventStateSignal) Permission() Tree {
  return Tree{
    SerializedType(StatusType): nil,
  }
}

func (signal EventStateSignal) String() string {
  return fmt.Sprintf("EventStateSignal(%s, %s, %s, %+v)", signal.SignalHeader, signal.Source, signal.State, signal.Time)
}

func NewEventStateSignal(source NodeID, state string, t time.Time) *EventStateSignal {
  return &EventStateSignal{
    SignalHeader: NewSignalHeader(Up),
    Source: source,
    State: state,
    Time: t,
  }
}

type EventControlSignal struct {
  SignalHeader
  Command string
}

func NewEventControlSignal(command string) *EventControlSignal {
  return &EventControlSignal{
    NewSignalHeader(Direct),
    command,
  }
}

func (signal EventControlSignal) Permission() Tree {
  return Tree{
    SerializedType(EventControlSignalType): {
      Hash("command", signal.Command): nil,
    },
  }
}

var transitions = map[string]struct{
  from_state string
  to_state string
}{
  "start": {
    "init",
    "running",
  },
  "stop": {
    "running",
    "init",
  },
  "finish": {
    "running",
    "done",
  },
}

func (ext *EventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  if signal.Direction() == Up && ext.Parent != nil {
    messages = messages.Add(ctx, *ext.Parent, node, nil, signal)
  }

  switch sig := signal.(type) {
  case *EventControlSignal:
    info, exists := transitions[sig.Command]
    if exists == true {
      if ext.State == info.from_state {
        ext.State = info.to_state
        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
        node.QueueSignal(time.Now(), NewEventStateSignal(node.ID, ext.State, time.Now()))
      } else {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "bad_state"))
      }
    } else {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "bad_command"))
    }
  }

  return messages, changes
}
