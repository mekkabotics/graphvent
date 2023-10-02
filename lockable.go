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

type ReqInfo struct {
  State ReqState `gv:"state"`
  MsgID uuid.UUID `gv:"msg_id"`
}

type LockableExt struct{
  State ReqState `gv:"state"`
  ReqID *uuid.UUID `gv:"req_id"`
  Owner *NodeID `gv:"owner"`
  PendingOwner *NodeID `gv:"pending_owner"`
  PendingID uuid.UUID `gv:"pending_id"`
  Requirements map[NodeID]ReqInfo `gv:"requirements"`
}

func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func NewLockableExt(requirements []NodeID) *LockableExt {
  var reqs map[NodeID]ReqInfo = nil
  if requirements != nil {
    reqs = map[NodeID]ReqInfo{}
    for _, id := range(requirements) {
      reqs[id] = ReqInfo{
        Unlocked,
        uuid.UUID{},
      }
    }
  }
  return &LockableExt{
    State: Unlocked,
    Owner: nil,
    PendingOwner: nil,
    Requirements: reqs,
  }
}

func UnlockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  msgs := Messages{}
  signal := NewLockSignal("unlock")
  msgs = msgs.Add(ctx, node.ID, node.Key, signal, node.ID)
  return signal.ID(), ctx.Send(msgs)
}

func LockLockable(ctx *Context, node *Node) (uuid.UUID, error) {
  msgs := Messages{}
  signal := NewLockSignal("lock")
  msgs = msgs.Add(ctx, node.ID, node.Key, signal, node.ID)
  return signal.ID(), ctx.Send(msgs)
}

func (ext *LockableExt) HandleErrorSignal(ctx *Context, node *Node, source NodeID, signal *ErrorSignal) Messages {
  str := signal.Error
  ctx.Log.Logf("lockable", "ERROR_SIGNAL: %s->%s %+v", source, node.ID, str)

  msgs := Messages {}
  switch str {
  case "not_unlocked":
    if ext.State == Locking {
      ext.State = AbortingLock
      req_info := ext.Requirements[source]
      req_info.State = Unlocked
      ext.Requirements[source] = req_info
      for id, info := range(ext.Requirements) {
        if info.State == Locked {
          lock_signal := NewLockSignal("unlock")

          req_info := ext.Requirements[id]
          req_info.State = Unlocking
          req_info.MsgID = lock_signal.ID()
          ext.Requirements[id] = req_info
          ctx.Log.Logf("lockable", "SENT_ABORT_UNLOCK: %s to %s", lock_signal.ID(), id)

          msgs = msgs.Add(ctx, node.ID, node.Key, lock_signal, id)
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
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_requirement"), source)
      } else {
        if ext.Requirements == nil {
          ext.Requirements = map[NodeID]ReqInfo{}
        }
        ext.Requirements[signal.NodeID] = ReqInfo{
          Unlocked,
          uuid.UUID{},
        }
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "req_added"), source)
      }
    case "remove":
      _, exists := ext.Requirements[signal.NodeID]
      if exists == false {
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "can't link: not_requirement"), source)
      } else {
        delete(ext.Requirements, signal.NodeID)
        msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "req_removed"), source)
      }
    default:
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "unknown_action"), source)
    }
  } else {
    msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocked"), source)
  }
  return msgs
}

func (ext *LockableExt) HandleSuccessSignal(ctx *Context, node *Node, source NodeID, signal *SuccessSignal) Messages {
  ctx.Log.Logf("lockable", "SUCCESS_SIGNAL: %+v", signal)

  msgs := Messages{}
  if source == node.ID {
    return msgs
  }

  info, found := ext.Requirements[source]
  ctx.Log.Logf("lockable", "State: %+v", ext.State)
  if found == false {
    ctx.Log.Logf("lockable", "Got success from non-requirement %s", source)
  } else if info.MsgID != signal.ReqID {
    ctx.Log.Logf("lockable", "Got success for wrong signal for %s: %s, expecting %s", source, signal.ReqID, info.MsgID)
  } else {
    if info.State == Locking {
      if ext.State == Locking {
        info.State = Locked
        info.MsgID = uuid.UUID{}
        ext.Requirements[source] = info
        reqs := 0
        locked := 0
        for _, s := range(ext.Requirements) {
          reqs += 1
          if s.State == Locked {
            locked += 1
          }
        }

        if locked == reqs {
          ctx.Log.Logf("lockable", "WHOLE LOCK: %s - %s - %+v", node.ID, ext.PendingID, ext.PendingOwner)
          ext.State = Locked
          ext.Owner = ext.PendingOwner
          msgs = msgs.Add(ctx, node.ID, node.Key, NewSuccessSignal(ext.PendingID), *ext.Owner)
        } else {
          ctx.Log.Logf("lockable", "PARTIAL LOCK: %s - %d/%d", node.ID, locked, reqs)
        }
      } else if ext.State == AbortingLock {
        lock_signal := NewLockSignal("unlock")
        info.State = Unlocking
        info.MsgID = lock_signal.ID()
        ext.Requirements[source] = info
        msgs = msgs.Add(ctx, node.ID, node.Key, lock_signal, source)
      }
    } else if info.State == Unlocking {
      info.State = Unlocked
      info.MsgID = uuid.UUID{}
      ext.Requirements[source] = info
      reqs := 0
      unlocked := 0
      for _, s := range(ext.Requirements) {
        reqs += 1
        if s.State == Unlocked {
          unlocked += 1
        }
      }

      if unlocked == reqs {
        old_state := ext.State
        ext.State = Unlocked
        ctx.Log.Logf("lockable", "WHOLE UNLOCK: %s - %s - %+v", node.ID, ext.PendingID, ext.PendingOwner)
        if old_state == Unlocking {
          previous_owner := *ext.Owner
          ext.Owner = ext.PendingOwner
          ext.ReqID = nil
          msgs = msgs.Add(ctx, node.ID, node.Key, NewSuccessSignal(ext.PendingID), previous_owner)
        } else if old_state == AbortingLock {
          msgs = msgs.Add(ctx ,node.ID, node.Key, NewErrorSignal(*ext.ReqID, "not_unlocked"), *ext.PendingOwner)
          ext.PendingOwner = ext.Owner
        }
      } else {
        ctx.Log.Logf("lockable", "PARTIAL UNLOCK: %s - %d/%d", node.ID, unlocked, reqs)
      }
    }
  }

  return msgs
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, node *Node, source NodeID, signal *LockSignal) Messages {
  ctx.Log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal.State)

  msgs := Messages{}
  switch signal.State {
  case "lock":
    if ext.State == Unlocked {
      if len(ext.Requirements) == 0 {
        ext.State = Locked
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.Owner = &new_owner
        msgs = msgs.Add(ctx, node.ID, node.Key, NewSuccessSignal(signal.ID()), new_owner)
      } else {
        ext.State = Locking
        id := signal.ID()
        ext.ReqID = &id
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.PendingID = signal.ID()
        for id, info := range(ext.Requirements) {
          if info.State != Unlocked {
            ctx.Log.Logf("lockable", "REQ_NOT_UNLOCKED_WHEN_LOCKING")
          }
          lock_signal := NewLockSignal("lock")
          info.State = Locking
          info.MsgID = lock_signal.ID()
          ext.Requirements[id] = info
          msgs = msgs.Add(ctx, node.ID, node.Key, lock_signal, id)
        }
      }
    } else {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocked"), source)
    }
  case "unlock":
    if ext.State == Locked {
      if len(ext.Requirements) == 0 {
        ext.State = Unlocked
        new_owner := source
        ext.PendingOwner = nil
        ext.Owner = nil
        msgs = msgs.Add(ctx, node.ID, node.Key, NewSuccessSignal(signal.ID()), new_owner)
      } else if source == *ext.Owner {
        ext.State = Unlocking
        id := signal.ID()
        ext.ReqID = &id
        ext.PendingOwner = nil
        ext.PendingID = signal.ID()
        for id, info := range(ext.Requirements) {
          if info.State != Locked {
            ctx.Log.Logf("lockable", "REQ_NOT_LOCKED_WHEN_UNLOCKING")
          }
          lock_signal := NewLockSignal("unlock")
          info.State = Unlocking
          info.MsgID = lock_signal.ID()
          ext.Requirements[id] = info
          msgs = msgs.Add(ctx, node.ID, node.Key, lock_signal, id)
        }
      }
    } else {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locked"), source)
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
  switch signal.Direction() {
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
    case *SuccessSignal:
      messages = ext.HandleSuccessSignal(ctx, node, source, sig)
    default:
    }
  default:
  }
  return messages
}

