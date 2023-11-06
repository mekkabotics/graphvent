package graphvent

import (
  "time"
  "fmt"
)

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
  event_ext, err := GetExt[*EventExt](node, EventExtType)
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
  SerializedType(EventControlSignalType): nil,
})

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

func (ext *EventExt) UpdateState(node *Node, changes Changes, state string) {
  if ext.State != state {
    changes.Add(EventExtType, "changes")
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

var test_event_commands = map[string]map[string]string{
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
  var changes Changes = nil

  switch sig := signal.(type) {
  case *EventControlSignal:
    event_ext, err := GetExt[*EventExt](node, EventExtType)
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
        event_ext.UpdateState(node, changes, new_state)
        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
      }
    }
  }

  return messages, changes
}

type TransitionValidation struct {
  ToState string
}

func(ext *EventExt) ValidateEventCommand(signal *EventControlSignal, commands map[string]map[string]string) (string, *ErrorSignal) {
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
