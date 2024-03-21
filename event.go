package graphvent

import (
  "time"
  "fmt"
)

type EventCommand string
type EventState string

type EventExt struct {
  Name string `gv:"name"`
  State EventState `gv:"state"`
  StateStart time.Time `gv:"state_start"`
  Parent NodeID `gv:"parent" node:"Base"`
}

func (ext *EventExt) Load(ctx *Context, node *Node) error {
  return nil
}

func (ext *EventExt) Unload(ctx *Context, node *Node) {
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
  Source NodeID `gv:"source"`
  State EventState `gv:"state"`
  Time time.Time `gv:"time"`
}

func (signal EventStateSignal) String() string {
  return fmt.Sprintf("EventStateSignal(%s, %s, %s, %+v)", signal.SignalHeader, signal.Source, signal.State, signal.Time)
}

func NewEventStateSignal(source NodeID, state EventState, t time.Time) *EventStateSignal {
  return &EventStateSignal{
    SignalHeader: NewSignalHeader(),
    Source: source,
    State: state,
    Time: t,
  }
}

type EventControlSignal struct {
  SignalHeader
  Command EventCommand `gv:"command"`
}

func (signal EventControlSignal) String() string {
  return fmt.Sprintf("EventControlSignal(%s, %s)", signal.SignalHeader, signal.Command)
}

func NewEventControlSignal(command EventCommand) *EventControlSignal {
  return &EventControlSignal{
    NewSignalHeader(),
    command,
  }
}

func (ext *EventExt) UpdateState(node *Node, changes Changes, state EventState, state_start time.Time) {
  if ext.State != state {
    ext.StateStart = state_start
    changes = append(changes, "state")
    ext.State = state
    node.QueueSignal(time.Now(), NewEventStateSignal(node.ID, ext.State, time.Now()))
  }
}

func (ext *EventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
  var changes = Changes{}

  return messages, changes
}

type TestEventExt struct {
  Length time.Duration
}

func (ext *TestEventExt) Load(ctx *Context, node *Node) error {
  return nil
}

func (ext *TestEventExt) Unload(ctx *Context, node *Node) {
}

type EventCommandMap map[EventCommand]map[EventState]EventState
var test_event_commands = EventCommandMap{
  "ready?": {
    "init": "ready",
  },
  "start": {
    "ready": "running",
  },
  "abort": {
    "ready": "init",
  },
  "stop": {
    "running": "stopped",
  },
  "finish": {
    "running": "done",
  },
}


func (ext *TestEventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
  var changes = Changes{}

  switch sig := signal.(type) {
  case *EventControlSignal:
    event_ext, err := GetExt[EventExt](node)
    if err != nil {
      messages = append(messages, SendMsg{source, NewErrorSignal(sig.Id, "not_event")})
    } else {
      ctx.Log.Logf("event", "%s got %s EventControlSignal while in %s", node.ID, sig.Command, event_ext.State)
      new_state, error_signal := event_ext.ValidateEventCommand(sig, test_event_commands)
      if error_signal != nil {
        messages = append(messages, SendMsg{source, error_signal})
      } else {
        switch sig.Command {
        case "start":
          node.QueueSignal(time.Now().Add(ext.Length), NewEventControlSignal("finish"))
        }
        event_ext.UpdateState(node, changes, new_state, time.Now())
        messages = append(messages, SendMsg{source, NewSuccessSignal(sig.Id)})
      }
    }
  }

  return messages, changes
}

func(ext *EventExt) ValidateEventCommand(signal *EventControlSignal, commands EventCommandMap) (EventState, *ErrorSignal) {
  transitions, command_mapped := commands[signal.Command]
  if command_mapped == false {
    return "", NewErrorSignal(signal.Id, "unknown command %s", signal.Command)
  } else {
    new_state, valid_transition := transitions[ext.State]
    if valid_transition == false {
      return "", NewErrorSignal(signal.Id, "invalid command state %s(%s)", signal.Command, ext.State)
    } else {
      return new_state, nil
    }
  }
}
