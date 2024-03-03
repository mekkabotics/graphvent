package graphvent

import (
  "time"
  "fmt"
)

type EventCommand string
type EventState string

type ParentOfPolicy struct {
  PolicyHeader
  Policy Tree
}

func NewParentOfPolicy(policy Tree) *ParentOfPolicy {
  return &ParentOfPolicy{
    PolicyHeader: NewPolicyHeader(),
    Policy: policy,
  }
}

func (policy ParentOfPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  event_ext, err := GetExt[EventExt](node)
  if err != nil {
    ctx.Log.Logf("event", "ParentOfPolicy, node not event %s", node.ID)
    return nil, Deny
  }

  if event_ext.Parent == principal_id {
    return nil, policy.Policy.Allows(action)
  }

  return nil, Deny
}

func (policy ParentOfPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  return Deny
}

var DefaultEventPolicy = NewParentOfPolicy(Tree{
  SerializedType(SignalTypeFor[EventControlSignal]()): nil,
})

type EventExt struct {
  Name string `gv:"name"`
  State EventState `gv:"state"`
  StateStart time.Time `gv:"state_start"`
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
  Source NodeID `gv:"source"`
  State EventState `gv:"state"`
  Time time.Time `gv:"time"`
}

func (signal EventStateSignal) Permission() Tree {
  return Tree{
    SerializedType(SignalTypeFor[StatusSignal]()): nil,
  }
}

func (signal EventStateSignal) String() string {
  return fmt.Sprintf("EventStateSignal(%s, %s, %s, %+v)", signal.SignalHeader, signal.Source, signal.State, signal.Time)
}

func NewEventStateSignal(source NodeID, state EventState, t time.Time) *EventStateSignal {
  return &EventStateSignal{
    SignalHeader: NewSignalHeader(Up),
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
    NewSignalHeader(Direct),
    command,
  }
}

func (signal EventControlSignal) Permission() Tree {
  return Tree{
    SerializedType(SignalTypeFor[EventControlSignal]()): {
      Hash("command", string(signal.Command)): nil,
    },
  }
}

func (ext *EventExt) UpdateState(node *Node, changes Changes, state EventState, state_start time.Time) {
  if ext.State != state {
    ext.StateStart = state_start
    AddChange[EventExt](changes, "state")
    ext.State = state
    node.QueueSignal(time.Now(), NewEventStateSignal(node.ID, ext.State, time.Now()))
  }
}

func (ext *EventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes = Changes{}

  if signal.Direction() == Up && ext.Parent != node.ID {
    messages = messages.Add(ctx, ext.Parent, node, nil, signal)
  }

  return messages, changes
}

type TestEventExt struct {
  Length time.Duration
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


func (ext *TestEventExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes = Changes{}

  switch sig := signal.(type) {
  case *EventControlSignal:
    event_ext, err := GetExt[EventExt](node)
    if err != nil {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "not_event"))
    } else {
      ctx.Log.Logf("event", "%s got %s EventControlSignal while in %s", node.ID, sig.Command, event_ext.State)
      new_state, error_signal := event_ext.ValidateEventCommand(sig, test_event_commands)
      if error_signal != nil {
        messages = messages.Add(ctx, source, node, nil, error_signal)
      } else {
        switch sig.Command {
        case "start":
          node.QueueSignal(time.Now().Add(ext.Length), NewEventControlSignal("finish"))
        }
        event_ext.UpdateState(node, changes, new_state, time.Now())
        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
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
