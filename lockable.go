package graphvent

import (
  "github.com/google/uuid"
  "time"
)

type ReqState byte
const (
  Unlocked = ReqState(0)
  Unlocking = ReqState(1)
  Locked = ReqState(2)
  Locking = ReqState(3)
  AbortingLock = ReqState(4)
)

var ReqStateStrings = map[ReqState]string {
  Unlocked: "Unlocked",
  Unlocking: "Unlocking",
  Locked: "Locked",
  Locking: "Locking",
  AbortingLock: "AbortingLock",
}

type LockableExt struct{
  State ReqState `gv:"state"`
  ReqID *uuid.UUID `gv:"req_id"`
  Owner *NodeID `gv:"owner"`
  PendingOwner *NodeID `gv:"pending_owner"`
  PendingID uuid.UUID `gv:"pending_id"`
  Requirements map[NodeID]ReqState `gv:"requirements"`
  WaitInfos WaitMap `gv:"wait_infos"`
}

func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func NewLockableExt(requirements []NodeID) *LockableExt {
  var reqs map[NodeID]ReqState = nil
  if requirements != nil {
    reqs = map[NodeID]ReqState{}
    for _, id := range(requirements) {
      reqs[id] = Unlocked
    }
  }
  return &LockableExt{
    State: Unlocked,
    Owner: nil,
    PendingOwner: nil,
    Requirements: reqs,
    WaitInfos: WaitMap{},
  }
}

func UnlockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  messages := Messages{}
  signal := NewLockSignal("unlock")
  messages = messages.Add(ctx, node.ID, node, nil, signal)
  return signal.ID(), ctx.Send(messages)
}

func LockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  messages := Messages{}
  signal := NewLockSignal("lock")
  messages = messages.Add(ctx, node.ID, node, nil, signal)
  return signal.ID(), ctx.Send(messages)
}

func (ext *LockableExt) HandleErrorSignal(ctx *Context, node *Node, source NodeID, signal *ErrorSignal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  info, info_found := node.ProcessResponse(ext.WaitInfos, signal)
  if info_found {
    state, found := ext.Requirements[info.NodeID]
    if found == true {
      ctx.Log.Logf("lockable", "got mapped response %+v for %+v in state %s", signal, info, ReqStateStrings[state])
      switch state {
      case Locking:
        ext.State = AbortingLock
        ext.Requirements[info.NodeID] = Unlocked
        for id, state := range(ext.Requirements) {
          if state == Locked {
            ext.Requirements[id] = Unlocking
            lock_signal := NewLockSignal("unlock")
            ext.WaitInfos[lock_signal.Id] = node.QueueTimeout(id, lock_signal, 100*time.Millisecond)
            messages = messages.Add(ctx, id, node, nil, lock_signal)
            ctx.Log.Logf("lockable", "sent abort unlock to %s from %s", id, node.ID)
          }
        }
      case Unlocking:
      }
    } else {
      ctx.Log.Logf("lockable", "Got mapped error %s, but %s isn't a requirement", signal, info.NodeID)
    }
  }

  return messages, changes
}

func (ext *LockableExt) HandleLinkSignal(ctx *Context, node *Node, source NodeID, signal *LinkSignal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil
  if ext.State == Unlocked {
    switch signal.Action {
    case "add":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == true {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "already_requirement"))
      } else {
        if ext.Requirements == nil {
          ext.Requirements = map[NodeID]ReqState{}
        }
        ext.Requirements[signal.NodeID] = Unlocked
        changes = changes.Add("requirement_added")
        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(signal.ID()))
      }
    case "remove":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == false {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "can't link: not_requirement"))
      } else {
        delete(ext.Requirements, signal.NodeID)
        changes = changes.Add("requirement_removed")
        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(signal.ID()))
      }
    default:
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "unknown_action"))
    }
  } else {
    messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "not_unlocked"))
  }
  return messages, changes
}

func (ext *LockableExt) HandleSuccessSignal(ctx *Context, node *Node, source NodeID, signal *SuccessSignal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil
  if source == node.ID {
    return messages, changes
  }

  info, info_found := node.ProcessResponse(ext.WaitInfos, signal)
  if info_found == true {
    state, found := ext.Requirements[info.NodeID]
    if found == false {
      ctx.Log.Logf("lockable", "Got success signal for requirement that is no longer in the map(%s), ignoring...", info.NodeID)
    } else {
      ctx.Log.Logf("lockable", "got mapped response %+v for %+v in state %s", signal, info, ReqStateStrings[state])
      switch state {
      case Locking:
        switch ext.State {
        case Locking:
          ext.Requirements[info.NodeID] = Locked
          locked := 0
          for _, s := range(ext.Requirements) {
            if s == Locked {
              locked += 1
            }
          }
          if locked == len(ext.Requirements) {
            ctx.Log.Logf("lockable", "WHOLE LOCK: %s - %s - %+v", node.ID, ext.PendingID, ext.PendingOwner)
            ext.State = Locked
            ext.Owner = ext.PendingOwner
            changes = changes.Add("locked")
            messages = messages.Add(ctx, *ext.Owner, node, nil, NewSuccessSignal(ext.PendingID))
          } else {
            changes = changes.Add("partial_lock")
            ctx.Log.Logf("lockable", "PARTIAL LOCK: %s - %d/%d", node.ID, locked, len(ext.Requirements))
          }
        case AbortingLock:
          ext.Requirements[info.NodeID] = Unlocking

          lock_signal := NewLockSignal("unlock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout(info.NodeID, lock_signal, 100*time.Millisecond)
          messages = messages.Add(ctx, info.NodeID, node, nil, lock_signal)

          ctx.Log.Logf("lockable", "sending abort_lock to %s for %s", info.NodeID, node.ID)
        }
      case AbortingLock:
        ctx.Log.Logf("lockable", "Got success signal in AbortingLock %s", node.ID)
        fallthrough
      case Unlocking:
        ext.Requirements[source] = Unlocked

        unlocked := 0
        for _, s := range(ext.Requirements) {
          if s == Unlocked {
            unlocked += 1
          }
        }

        if unlocked == len(ext.Requirements) {
          old_state := ext.State
          ext.State = Unlocked
          ctx.Log.Logf("lockable", "WHOLE UNLOCK: %s - %s - %+v", node.ID, ext.PendingID, ext.PendingOwner)
          if old_state == Unlocking {
            previous_owner := *ext.Owner
            ext.Owner = ext.PendingOwner
            ext.ReqID = nil
            changes = changes.Add("unlocked")
            messages = messages.Add(ctx, previous_owner, node, nil, NewSuccessSignal(ext.PendingID))
          } else if old_state == AbortingLock {
            changes = changes.Add("lock_aborted")
            messages = messages.Add(ctx, *ext.PendingOwner, node, nil, NewErrorSignal(*ext.ReqID, "not_unlocked"))
            ext.PendingOwner = ext.Owner
          }
        } else {
          changes = changes.Add("partial_unlock")
          ctx.Log.Logf("lockable", "PARTIAL UNLOCK: %s - %d/%d", node.ID, unlocked, len(ext.Requirements))
        }
      }
    }
  }

  return messages, changes
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, node *Node, source NodeID, signal *LockSignal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  switch signal.State {
  case "lock":
    switch ext.State {
    case Unlocked:
      if len(ext.Requirements) == 0 {
        ext.State = Locked
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.Owner = &new_owner
        changes = changes.Add("locked")
        messages = messages.Add(ctx, new_owner, node, nil, NewSuccessSignal(signal.ID()))
      } else {
        ext.State = Locking
        id := signal.ID()
        ext.ReqID = &id
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.PendingID = signal.ID()
        changes = changes.Add("locking")
        for id, state := range(ext.Requirements) {
          if state != Unlocked {
            ctx.Log.Logf("lockable", "REQ_NOT_UNLOCKED_WHEN_LOCKING")
          }

          lock_signal := NewLockSignal("lock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout(id, lock_signal, 100*time.Millisecond)
          ext.Requirements[id] = Locking

          messages = messages.Add(ctx, id, node, nil, lock_signal)
        }
      }
    default:
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "not_unlocked"))
      ctx.Log.Logf("lockable", "Tried to lock %s while locked", node.ID)
    }
  case "unlock":
    if ext.State == Locked {
      if len(ext.Requirements) == 0 {
        ext.State = Unlocked
        new_owner := source
        ext.PendingOwner = nil
        ext.Owner = nil
        changes = changes.Add("unlocked")
        messages = messages.Add(ctx, new_owner, node, nil, NewSuccessSignal(signal.ID()))
      } else if source == *ext.Owner {
        ext.State = Unlocking
        id := signal.ID()
        ext.ReqID = &id
        ext.PendingOwner = nil
        ext.PendingID = signal.ID()
        changes = changes.Add("unlocking")
        for id, state := range(ext.Requirements) {
          if state != Locked {
            ctx.Log.Logf("lockable", "REQ_NOT_LOCKED_WHEN_UNLOCKING")
          }

          lock_signal := NewLockSignal("unlock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout(id, lock_signal, 100*time.Millisecond)
          ext.Requirements[id] = Unlocking

          messages = messages.Add(ctx, id, node, nil, lock_signal)
        }
      }
    } else {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "not_locked"))
    }
  default:
    ctx.Log.Logf("lockable", "LOCK_ERR: unkown state %s", signal.State)
  }
  return messages, changes
}

func (ext *LockableExt) HandleTimeoutSignal(ctx *Context, node *Node, source NodeID, signal *TimeoutSignal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  //TODO: Handle timeout errors better
  wait_info, found := node.ProcessResponse(ext.WaitInfos, signal)
  if found == true {
    state, found := ext.Requirements[wait_info.NodeID]
    if found == true {
      ctx.Log.Logf("lockable", "%s timed out %s", wait_info.NodeID, ReqStateStrings[state])
    } else {
      ctx.Log.Logf("lockable", "%s timed out", wait_info.NodeID)
    }
  }

  return messages, changes
}

// LockableExts process Up/Down signals by forwarding them to owner, dependency, and requirement nodes
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  switch signal.Direction() {
  case Up:
    if ext.Owner != nil {
      if *ext.Owner != node.ID {
        messages = messages.Add(ctx, *ext.Owner, node, nil, signal)
      }
    }

  case Down:
    for requirement := range(ext.Requirements) {
      messages = messages.Add(ctx, requirement, node, nil, signal)
    }

  case Direct:
    switch sig := signal.(type) {
    case *LinkSignal:
      messages, changes = ext.HandleLinkSignal(ctx, node, source, sig)
    case *LockSignal:
      messages, changes = ext.HandleLockSignal(ctx, node, source, sig)
    case *ErrorSignal:
      messages, changes = ext.HandleErrorSignal(ctx, node, source, sig)
    case *SuccessSignal:
      messages, changes = ext.HandleSuccessSignal(ctx, node, source, sig)
    case *TimeoutSignal:
      messages, changes = ext.HandleTimeoutSignal(ctx, node, source, sig)
    default:
    }
  default:
  }
  return messages, changes
}

type RequirementOfPolicy struct {
  PerNodePolicy
}

func NewRequirementOfPolicy(dep_rules map[NodeID]Tree) RequirementOfPolicy {
  return RequirementOfPolicy {
    PerNodePolicy: NewPerNodePolicy(dep_rules),
  }
}

func (policy RequirementOfPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  sig, ok := signal.(*ReadResultSignal)
  if ok == false {
    return Deny
  }

  ext, ok := sig.Extensions[LockableExtType]
  if ok == false {
    return Deny
  }

  reqs_ser, ok := ext["requirements"]
  if ok == false {
    return Deny
  }

  reqs_type, _, err := DeserializeType(ctx, reqs_ser.TypeStack)
  if err != nil {
    return Deny
  }

  reqs_if, _, err := DeserializeValue(ctx, reqs_type, reqs_ser.Data)
  if err != nil {
    return Deny
  }

  requirements, ok := reqs_if.Interface().(map[NodeID]ReqState)
  if ok == false {
    return Deny
  }

  for req, _ := range(requirements) {
    if req == current.Principal {
      return policy.NodeRules[sig.NodeID].Allows(current.Action)
    }
  }

  return Deny
}
