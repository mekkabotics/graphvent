package graphvent

import (
  "github.com/google/uuid"
)

type ReqState byte
const (
  Unlocked = ReqState(0)
  Unlocking = ReqState(1)
  Locked = ReqState(2)
  Locking = ReqState(3)
  AbortingLock = ReqState(4)
)

type LockableExt struct{
  State ReqState `gv:"0"`
  ReqID *uuid.UUID `gv:"1"`
  Owner *NodeID `gv:"2"`
  PendingOwner *NodeID `gv:"3"`
  Requirements map[NodeID]ReqState `gv:"4"`
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
  }
}

// Send the signal to unlock a node from itself
func UnlockLockable(ctx *Context, owner *Node, target NodeID) (uuid.UUID, error) {
  msgs := Messages{}
  signal := NewLockSignal("unlock")
  msgs = msgs.Add(ctx, owner.ID, owner.Key, signal, target)
  return signal.Header().ID, ctx.Send(msgs)
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, owner *Node, target NodeID) (uuid.UUID, error) {
  msgs := Messages{}
  signal := NewLockSignal("lock")
  msgs = msgs.Add(ctx, owner.ID, owner.Key, signal, target)
  return signal.Header().ID, ctx.Send(msgs)
}

func (ext *LockableExt) HandleErrorSignal(ctx *Context, node *Node, source NodeID, signal *ErrorSignal) Messages {
  str := signal.Error
  ctx.Log.Logf("lockable", "ERROR_SIGNAL: %s->%s %+v", source, node.ID, str)

  msgs := Messages {}
  switch str {
  case "not_unlocked":
    if ext.State == Locking {
      ext.State = AbortingLock
      ext.Requirements[source] = Unlocked
      for id, state := range(ext.Requirements) {
        if state == Locked {
          ext.Requirements[id] = Unlocking
          msgs = msgs.Add(ctx, node.ID, node.Key, NewLockSignal("unlock"), id)
        }
      }
    }
  case "not_locked":
    panic("RECEIVED not_locked, meaning a node thought it held a lock it didn't")
  case "not_requirement":
  }

  return msgs
}

func (ext *LockableExt) HandleLinkSignal(ctx *Context, node *Node, source NodeID, signal *LinkSignal) Messages {
  msgs := Messages {}
  if ext.State == Unlocked {
    switch signal.Action {
    case "add":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == true {
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "already_requirement"), source)
      } else {
        if ext.Requirements == nil {
          ext.Requirements = map[NodeID]ReqState{}
        }
        ext.Requirements[signal.NodeID] = Unlocked
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "req_added"), source)
      }
    case "remove":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == false {
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "not_requirement"), source)
      } else {
        delete(ext.Requirements, signal.NodeID)
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "req_removed"), source)
      }
    default:
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "unknown_action"), source)
    }
  } else {
    msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "not_unlocked"), source)
  }
  return msgs
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, node *Node, source NodeID, signal *LockSignal) Messages {
  ctx.Log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal.State)

  msgs := Messages{}
  switch signal.State {
  case "locked":
    state, found := ext.Requirements[source]
    if found == false {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "not_requirement"), source)
    } else if state == Locking {
      if ext.State == Locking {
        ext.Requirements[source] = Locked
        reqs := 0
        locked := 0
        for _, s := range(ext.Requirements) {
          reqs += 1
          if s == Locked {
            locked += 1
          }
        }

        if locked == reqs {
          ext.State = Locked
          ext.Owner = ext.PendingOwner
          msgs = msgs.Add(ctx, node.ID, node.Key, NewLockSignal("locked"), *ext.Owner)
        } else {
          ctx.Log.Logf("lockable", "PARTIAL LOCK: %s - %d/%d", node.ID, locked, reqs)
        }
      } else if ext.State == AbortingLock {
        ext.Requirements[source] = Unlocking
        msgs = msgs.Add(ctx, node.ID, node.Key, NewLockSignal("unlock"), source)
      }
    }
  case "unlocked":
    state, found := ext.Requirements[source]
    if found == false {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "not_requirement"), source)
    } else if state == Unlocking {
      ext.Requirements[source] = Unlocked
      reqs := 0
      unlocked := 0
      for _, s := range(ext.Requirements) {
        reqs += 1
        if s == Unlocked {
          unlocked += 1
        }
      }

      if unlocked == reqs {
        old_state := ext.State
        ext.State = Unlocked
        if old_state == Unlocking {
          ext.Owner = ext.PendingOwner
          ext.ReqID = nil
          msgs = msgs.Add(ctx, node.ID, node.Key, NewLockSignal("unlocked"), *ext.Owner)
        } else if old_state == AbortingLock {
          msgs = msgs.Add(ctx ,node.ID, node.Key, NewErrorSignal(*ext.ReqID, "not_unlocked"), *ext.PendingOwner)
          ext.PendingOwner = ext.Owner
        }
      } else {
        ctx.Log.Logf("lockable", "PARTIAL UNLOCK: %s - %d/%d", node.ID, unlocked, reqs)
      }
    }
  case "lock":
    if ext.State == Unlocked {
      if len(ext.Requirements) == 0 {
        ext.State = Locked
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.Owner = &new_owner
        msgs = msgs.Add(ctx, node.ID, node.Key, NewLockSignal("locked"), new_owner)
      } else {
        ext.State = Locking
        id := signal.ID
        ext.ReqID = &id
        new_owner := source
        ext.PendingOwner = &new_owner
        for id, state := range(ext.Requirements) {
          if state != Unlocked {
            ctx.Log.Logf("lockable", "REQ_NOT_UNLOCKED_WHEN_LOCKING")
          }
          ext.Requirements[id] = Locking
          lock_signal := NewLockSignal("lock")
          msgs = msgs.Add(ctx, node.ID, node.Key, lock_signal, id)
        }
      }
    } else {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "not_unlocked"), source)
    }
  case "unlock":
    if ext.State == Locked {
      if len(ext.Requirements) == 0 {
        ext.State = Unlocked
        new_owner := source
        ext.PendingOwner = nil
        ext.Owner = nil
        msgs = msgs.Add(ctx, node.ID, node.Key, NewLockSignal("unlocked"), new_owner)
      } else if source == *ext.Owner {
        ext.State = Unlocking
        id := signal.ID
        ext.ReqID = &id
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state != Locked {
            ctx.Log.Logf("lockable", "REQ_NOT_LOCKED_WHEN_UNLOCKING")
          }
          ext.Requirements[id] = Unlocking
          lock_signal := NewLockSignal("unlock")
          msgs = msgs.Add(ctx, node.ID, node.Key, lock_signal, id)
        }
      }
    } else {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID, "not_locked"), source)
    }
  default:
    ctx.Log.Logf("lockable", "LOCK_ERR: unkown state %s", signal.State)
  }
  return msgs
}

// LockableExts process Up/Down signals by forwarding them to owner, dependency, and requirement nodes
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  messages := Messages{}
  switch signal.Header().Direction {
  case Up:
    if ext.Owner != nil {
      if *ext.Owner != node.ID {
        messages = messages.Add(ctx, node.ID, node.Key, signal, *ext.Owner)
      }
    }
  case Down:
    for requirement, _ := range(ext.Requirements) {
      messages = messages.Add(ctx, node.ID, node.Key, signal, requirement)
    }
  case Direct:
    switch sig := signal.(type) {
    case *LinkSignal:
      messages = ext.HandleLinkSignal(ctx, node, source, sig)
    case *LockSignal:
      messages = ext.HandleLockSignal(ctx, node, source, sig)
    case *ErrorSignal:
      messages = ext.HandleErrorSignal(ctx, node, source, sig)
    default:
    }
  default:
  }
  return messages
}

