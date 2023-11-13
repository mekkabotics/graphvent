package graphvent

import (
  "github.com/google/uuid"
  "time"
)

var AllowParentUnlockPolicy = NewOwnerOfPolicy(Tree{
  SerializedType(LockSignalType): {
    Hash(LockStateBase, "unlock"): nil,
  },
})

var AllowAnyLockPolicy = NewAllNodesPolicy(Tree{
  SerializedType(LockSignalType): {
    Hash(LockStateBase, "lock"): nil,
  },
})

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
  var changes = Changes{}

  info, info_found := node.ProcessResponse(ext.WaitInfos, signal)
  if info_found {
    state, found := ext.Requirements[info.Destination]
    if found == true {
      changes.Add(LockableExtType, "wait_infos")
      ctx.Log.Logf("lockable", "got mapped response %+v for %+v in state %s while in %s", signal, info, ReqStateStrings[state], ReqStateStrings[ext.State])
      switch ext.State {
      case AbortingLock:
        ext.Requirements[info.Destination] = Unlocked
        all_unlocked := true
        for _, state := range(ext.Requirements) {
          if state != Unlocked {
            all_unlocked = false
            break
          }
        }
        if all_unlocked == true {
          changes.Add(LockableExtType, "state")
          ext.State = Unlocked
        }
      case Locking:
        changes.Add(LockableExtType, "state")
        ext.State = AbortingLock
        ext.Requirements[info.Destination] = Unlocked
        for id, state := range(ext.Requirements) {
          if state == Locked {
            ext.Requirements[id] = Unlocking
            lock_signal := NewLockSignal("unlock")
            ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", id, lock_signal, 100*time.Millisecond)
            messages = messages.Add(ctx, id, node, nil, lock_signal)
            ctx.Log.Logf("lockable", "sent abort unlock to %s from %s", id, node.ID)
          }
        }
      case Unlocking:
        ext.Requirements[info.Destination] = Locked
        all_returned := true
        for _, state := range(ext.Requirements) {
          if state == Unlocking {
            all_returned = false
            break
          }
        }
        if all_returned == true {
          ext.State = Locked
        }
      }
    } else {
      ctx.Log.Logf("lockable", "Got mapped error %s, but %s isn't a requirement", signal, info.Destination)
    }
  }

  return messages, changes
}

func (ext *LockableExt) HandleLinkSignal(ctx *Context, node *Node, source NodeID, signal *LinkSignal) (Messages, Changes) {
  var messages Messages = nil
  var changes = Changes{}
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
        changes.Add(LockableExtType, "requirements")
        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(signal.ID()))
      }
    case "remove":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == false {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "can't link: not_requirement"))
      } else {
        delete(ext.Requirements, signal.NodeID)
        changes.Add(LockableExtType, "requirements")
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
  var changes = Changes{}
  if source == node.ID {
    return messages, changes
  }

  info, info_found := node.ProcessResponse(ext.WaitInfos, signal)
  if info_found == true {
    state, found := ext.Requirements[info.Destination]
    if found == false {
      ctx.Log.Logf("lockable", "Got success signal for requirement that is no longer in the map(%s), ignoring...", info.Destination)
    } else {
      ctx.Log.Logf("lockable", "got mapped response %+v for %+v in state %s", signal, info, ReqStateStrings[state])
      switch state {
      case Locking:
        switch ext.State {
        case Locking:
          ext.Requirements[info.Destination] = Locked
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
            changes.Add(LockableExtType, "state", "owner", "requirements")
            messages = messages.Add(ctx, *ext.Owner, node, nil, NewSuccessSignal(ext.PendingID))
          } else {
            changes.Add(LockableExtType, "requirements")
            ctx.Log.Logf("lockable", "PARTIAL LOCK: %s - %d/%d", node.ID, locked, len(ext.Requirements))
          }
        case AbortingLock:
          ext.Requirements[info.Destination] = Unlocking

          lock_signal := NewLockSignal("unlock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", info.Destination, lock_signal, 100*time.Millisecond)
          messages = messages.Add(ctx, info.Destination, node, nil, lock_signal)

          ctx.Log.Logf("lockable", "sending abort_lock to %s for %s", info.Destination, node.ID)
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
            changes.Add(LockableExtType, "state", "owner", "req_id")
            messages = messages.Add(ctx, previous_owner, node, nil, NewSuccessSignal(ext.PendingID))
          } else if old_state == AbortingLock {
            changes.Add(LockableExtType, "state", "pending_owner")
            messages = messages.Add(ctx, *ext.PendingOwner, node, nil, NewErrorSignal(*ext.ReqID, "not_unlocked"))
            ext.PendingOwner = ext.Owner
          }
        } else {
          changes.Add(LockableExtType, "state")
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
  var changes = Changes{}

  switch signal.State {
  case "lock":
    switch ext.State {
    case Unlocked:
      if len(ext.Requirements) == 0 {
        ext.State = Locked
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.Owner = &new_owner
        changes.Add(LockableExtType, "state", "pending_owner", "owner")
        messages = messages.Add(ctx, new_owner, node, nil, NewSuccessSignal(signal.ID()))
      } else {
        ext.State = Locking
        id := signal.ID()
        ext.ReqID = &id
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.PendingID = signal.ID()
        changes.Add(LockableExtType, "state", "req_id", "pending_owner", "pending_id")
        for id, state := range(ext.Requirements) {
          if state != Unlocked {
            ctx.Log.Logf("lockable", "REQ_NOT_UNLOCKED_WHEN_LOCKING")
          }

          lock_signal := NewLockSignal("lock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("lock", id, lock_signal, 500*time.Millisecond)
          ext.Requirements[id] = Locking

          messages = messages.Add(ctx, id, node, nil, lock_signal)
        }
      }
    default:
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(signal.ID(), "not_unlocked"))
      ctx.Log.Logf("lockable", "Tried to lock %s while %s", node.ID, ext.State)
    }
  case "unlock":
    if ext.State == Locked {
      if len(ext.Requirements) == 0 {
        ext.State = Unlocked
        new_owner := source
        ext.PendingOwner = nil
        ext.Owner = nil
        changes.Add(LockableExtType, "state", "pending_owner", "owner")
        messages = messages.Add(ctx, new_owner, node, nil, NewSuccessSignal(signal.ID()))
      } else if source == *ext.Owner {
        ext.State = Unlocking
        id := signal.ID()
        ext.ReqID = &id
        ext.PendingOwner = nil
        ext.PendingID = signal.ID()
        changes.Add(LockableExtType, "state", "pending_owner", "pending_id", "req_id")
        for id, state := range(ext.Requirements) {
          if state != Locked {
            ctx.Log.Logf("lockable", "REQ_NOT_LOCKED_WHEN_UNLOCKING")
          }

          lock_signal := NewLockSignal("unlock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", id, lock_signal, 100*time.Millisecond)
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
  var changes = Changes{}

  wait_info, found := node.ProcessResponse(ext.WaitInfos, signal)
  if found == true {
    changes.Add(LockableExtType, "wait_infos")
    state, found := ext.Requirements[wait_info.Destination]
    if found == true {
      ctx.Log.Logf("lockable", "%s timed out %s while %s was %s", wait_info.Destination, ReqStateStrings[state], node.ID, ReqStateStrings[state])
      switch ext.State {
      case AbortingLock:
        ext.Requirements[wait_info.Destination] = Unlocked
        all_unlocked := true
        for _, state := range(ext.Requirements) {
          if state != Unlocked {
            all_unlocked = false
            break
          }
        }
        if all_unlocked == true {
          changes.Add(LockableExtType, "state")
          ext.State = Unlocked
        }
      case Locking:
        ext.State = AbortingLock
        ext.Requirements[wait_info.Destination] = Unlocked
        for id, state := range(ext.Requirements) {
          if state == Locked {
            ext.Requirements[id] = Unlocking
            lock_signal := NewLockSignal("unlock")
            ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", id, lock_signal, 100*time.Millisecond)
            messages = messages.Add(ctx, id, node, nil, lock_signal)
            ctx.Log.Logf("lockable", "sent abort unlock to %s from %s", id, node.ID)
          }
        }
      case Unlocking:
        ext.Requirements[wait_info.Destination] = Locked
        all_returned := true
        for _, state := range(ext.Requirements) {
          if state == Unlocking {
            all_returned = false
            break
          }
        }
        if all_returned == true {
          ext.State = Locked
        }
      }
    } else {
      ctx.Log.Logf("lockable", "%s timed out", wait_info.Destination)
    }
  }

  return messages, changes
}

// LockableExts process Up/Down signals by forwarding them to owner, dependency, and requirement nodes
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes = Changes{}

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

type OwnerOfPolicy struct {
  PolicyHeader
  Rules Tree `gv:"rules"`
}

func NewOwnerOfPolicy(rules Tree) OwnerOfPolicy {
  return OwnerOfPolicy{
    PolicyHeader: NewPolicyHeader(),
    Rules: rules,
  }
}

func (policy OwnerOfPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  return Deny
}

func (policy OwnerOfPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  l_ext, err := GetExt[*LockableExt](node, LockableExtType)
  if err != nil {
    ctx.Log.Logf("lockable", "OwnerOfPolicy.Allows called on node without LockableExt")
    return nil, Deny
  }

  if l_ext.Owner == nil {
    return nil, Deny
  }

  if principal_id == *l_ext.Owner {
    return nil, Allow
  }

  return nil, Deny
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

  for req := range(requirements) {
    if req == current.Principal {
      return policy.NodeRules[sig.NodeID].Allows(current.Action)
    }
  }

  return Deny
}
