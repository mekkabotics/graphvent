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
  State ReqState `gv:"lockable_state"`
  ReqID *uuid.UUID `gv:"req_id"`
  Owner *NodeID `gv:"owner" node:"Base"`
  PendingOwner *NodeID `gv:"pending_owner" node:"Base"`
  PendingID uuid.UUID `gv:"pending_id"`
  Requirements map[NodeID]ReqState `gv:"requirements" node:"Lockable:"`
  WaitInfos WaitMap `gv:"wait_infos" node:":Base"`
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
  signal := NewLockSignal("unlock")
  messages := []SendMsg{{node.ID, signal}}
  return signal.ID(), ctx.Send(node, messages)
}

func LockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  signal := NewLockSignal("lock")
  messages := []SendMsg{{node.ID, signal}}
  return signal.ID(), ctx.Send(node, messages)
}

func (ext *LockableExt) Load(ctx *Context, node *Node) error {
  return nil
}

func (ext *LockableExt) Unload(ctx *Context, node *Node) {
}

func (ext *LockableExt) HandleErrorSignal(ctx *Context, node *Node, source NodeID, signal *ErrorSignal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
  var changes Changes = nil

  info, info_found := node.ProcessResponse(ext.WaitInfos, signal)
  if info_found {
    state, found := ext.Requirements[info.Destination]
    if found == true {
      changes.Add("wait_infos")
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
          changes.Add("state")
          ext.State = Unlocked
        }
      case Locking:
        changes.Add("state")
        ext.Requirements[info.Destination] = Unlocked
        unlocked := 0
        for _, state := range(ext.Requirements) {
          if state == Unlocked {
            unlocked += 1
          }
        }

        if unlocked == len(ext.Requirements) {
          ctx.Log.Logf("lockable", "%s unlocked from error %s from %s", node.ID, signal.Error, source)
          ext.State = Unlocked
        } else {
          ext.State = AbortingLock
          for id, state := range(ext.Requirements) {
            if state == Locked {
              ext.Requirements[id] = Unlocking
              lock_signal := NewLockSignal("unlock")
              ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", id, lock_signal, 100*time.Millisecond)
              messages = append(messages, SendMsg{id, lock_signal})
              ctx.Log.Logf("lockable", "sent abort unlock to %s from %s", id, node.ID)
            }
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

func (ext *LockableExt) HandleLinkSignal(ctx *Context, node *Node, source NodeID, signal *LinkSignal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
  var changes = Changes{}
  if ext.State == Unlocked {
    switch signal.Action {
    case "add":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == true {
        messages = append(messages, SendMsg{source, NewErrorSignal(signal.ID(), "already_requirement")})
      } else {
        if ext.Requirements == nil {
          ext.Requirements = map[NodeID]ReqState{}
        }
        ext.Requirements[signal.NodeID] = Unlocked
        changes.Add("requirements")
        messages = append(messages, SendMsg{source, NewSuccessSignal(signal.ID())})
      }
    case "remove":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == false {
        messages = append(messages, SendMsg{source, NewErrorSignal(signal.ID(), "can't link: not_requirement")})
      } else {
        delete(ext.Requirements, signal.NodeID)
        changes.Add("requirements")
        messages = append(messages, SendMsg{source, NewSuccessSignal(signal.ID())})
      }
    default:
      messages = append(messages, SendMsg{source, NewErrorSignal(signal.ID(), "unknown_action")})
    }
  } else {
    messages = append(messages, SendMsg{source, NewErrorSignal(signal.ID(), "not_unlocked")})
  }
  return messages, changes
}

func (ext *LockableExt) HandleSuccessSignal(ctx *Context, node *Node, source NodeID, signal *SuccessSignal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
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
            changes.Add("state", "owner", "requirements")
            messages = append(messages, SendMsg{*ext.Owner, NewSuccessSignal(ext.PendingID)})
          } else {
            changes.Add("requirements")
            ctx.Log.Logf("lockable", "PARTIAL LOCK: %s - %d/%d", node.ID, locked, len(ext.Requirements))
          }
        case AbortingLock:
          ext.Requirements[info.Destination] = Unlocking

          lock_signal := NewLockSignal("unlock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", info.Destination, lock_signal, 100*time.Millisecond)
          messages = append(messages, SendMsg{info.Destination, lock_signal})

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
            changes.Add("state", "owner", "req_id")
            messages = append(messages, SendMsg{previous_owner, NewSuccessSignal(ext.PendingID)})
          } else if old_state == AbortingLock {
            changes.Add("state", "pending_owner")
            messages = append(messages, SendMsg{*ext.PendingOwner, NewErrorSignal(*ext.ReqID, "not_unlocked")})
            ext.PendingOwner = ext.Owner
          }
        } else {
          changes.Add("state")
          ctx.Log.Logf("lockable", "PARTIAL UNLOCK: %s - %d/%d", node.ID, unlocked, len(ext.Requirements))
        }
      }
    }
  }

  return messages, changes
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, node *Node, source NodeID, signal *LockSignal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
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
        changes.Add("state", "pending_owner", "owner")
        messages = append(messages, SendMsg{new_owner, NewSuccessSignal(signal.ID())})
      } else {
        ext.State = Locking
        id := signal.ID()
        ext.ReqID = &id
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.PendingID = signal.ID()
        changes.Add("state", "req_id", "pending_owner", "pending_id")
        for id, state := range(ext.Requirements) {
          if state != Unlocked {
            ctx.Log.Logf("lockable", "REQ_NOT_UNLOCKED_WHEN_LOCKING")
          }

          lock_signal := NewLockSignal("lock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("lock", id, lock_signal, 500*time.Millisecond)
          ext.Requirements[id] = Locking

          messages = append(messages, SendMsg{id, lock_signal})
        }
      }
    default:
      messages = append(messages, SendMsg{source, NewErrorSignal(signal.ID(), "not_unlocked")})
      ctx.Log.Logf("lockable", "Tried to lock %s while %s", node.ID, ext.State)
    }
  case "unlock":
    if ext.State == Locked {
      if len(ext.Requirements) == 0 {
        ext.State = Unlocked
        new_owner := source
        ext.PendingOwner = nil
        ext.Owner = nil
        changes.Add("state", "pending_owner", "owner")
        messages = append(messages, SendMsg{new_owner, NewSuccessSignal(signal.ID())})
      } else if source == *ext.Owner {
        ext.State = Unlocking
        id := signal.ID()
        ext.ReqID = &id
        ext.PendingOwner = nil
        ext.PendingID = signal.ID()
        changes.Add("state", "pending_owner", "pending_id", "req_id")
        for id, state := range(ext.Requirements) {
          if state != Locked {
            ctx.Log.Logf("lockable", "REQ_NOT_LOCKED_WHEN_UNLOCKING")
          }

          lock_signal := NewLockSignal("unlock")
          ext.WaitInfos[lock_signal.Id] = node.QueueTimeout("unlock", id, lock_signal, 100*time.Millisecond)
          ext.Requirements[id] = Unlocking

          messages = append(messages, SendMsg{id, lock_signal})
        }
      }
    } else {
      messages = append(messages, SendMsg{source, NewErrorSignal(signal.ID(), "not_locked")})
    }
  default:
    ctx.Log.Logf("lockable", "LOCK_ERR: unkown state %s", signal.State)
  }
  return messages, changes
}

func (ext *LockableExt) HandleTimeoutSignal(ctx *Context, node *Node, source NodeID, signal *TimeoutSignal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
  var changes = Changes{}

  wait_info, found := node.ProcessResponse(ext.WaitInfos, signal)
  if found == true {
    changes.Add("wait_infos")
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
          changes.Add("state")
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
            messages = append(messages, SendMsg{id, lock_signal})
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

// LockableExts process status signals by forwarding them to it's owner
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) ([]SendMsg, Changes) {
  var messages []SendMsg = nil
  var changes = Changes{}

  switch sig := signal.(type) {
  case *StatusSignal:
    if ext.Owner != nil {
      if *ext.Owner != node.ID {
        messages = append(messages, SendMsg{*ext.Owner, signal})
      }
    }
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

  return messages, changes
}

