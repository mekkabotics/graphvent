package graphvent

import (
  "time"
  "fmt"
)

type EventExt struct {
  Name string `gv:"name"`
  State string `gv:"state"`
  Parent NodeID `gv:"parent"`
}

func NewEventExt(parent NodeID, name string) *EventExt {
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

func (signal EventControlSignal) String() string {
  return fmt.Sprintf("EventControlSignal(%s, %s)", signal.SignalHeader, signal.Command)
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

func (ext *EventExt) UpdateState(node *Node, state string) {
  if ext.State != state {
    ext.State = state
    node.QueueSignal(time.Now(), NewEventStateSignal(node.ID, ext.State, time.Now()))
  }
}

func (ext *EventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  if signal.Direction() == Up && ext.Parent != node.ID {
    messages = messages.Add(ctx, ext.Parent, node, nil, signal)
  }

  return messages, changes
}

type TestEventExt struct {
  Length time.Duration
}

var test_event_transitions = map[string]struct{
  from_state string
  to_state string
}{
  "ready?": {
    "init",
    "ready",
  },
  "start": {
    "ready",
    "running",
  },
  "abort": {
    "ready",
    "init",
  },
  "stop": {
    "running",
    "stopped",
  },
  "finish": {
    "running",
    "done",
  },
}


func (ext *TestEventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  switch sig := signal.(type) {
  case *EventControlSignal:
    event_ext, err := GetExt[*EventExt](node, EventExtType)
    if err != nil {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "not_event"))
    } else {
      info, exists := test_event_transitions[sig.Command]
      if exists == true {
        if event_ext.State == info.from_state {
          ctx.Log.Logf("event", "%s %s->%s", node.ID, info.from_state, info.to_state)
          messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
          event_ext.UpdateState(node, info.to_state)
          if event_ext.State == "running" {
            node.QueueSignal(time.Now().Add(ext.Length), NewEventControlSignal("finish"))
          }
        } else {
          messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "bad_state"))
        }
      } else {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "bad_command"))
      }
    }
  }

  return messages, changes
}
