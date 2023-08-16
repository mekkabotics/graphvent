package graphvent

import (
  "encoding/binary"
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
  State ReqState
  ReqID uuid.UUID
  Owner *NodeID
  PendingOwner *NodeID
  Requirements map[NodeID]ReqState
}

func (ext *LockableExt) Field(name string) interface{} {
  return ResolveFields(ext, name, map[string]func(*LockableExt)interface{}{
    "owner": func(ext *LockableExt) interface{} {
      return ext.Owner
    },
    "pending_owner": func(ext *LockableExt) interface{} {
      return ext.PendingOwner
    },
    "requirements": func(ext *LockableExt) interface{} {
      return ext.Requirements
    },
  })
}

func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func (ext *LockableExt) Serialize() ([]byte, error) {
  ret := make([]byte, 9 + (16 * 2) + (17 * len(ext.Requirements)))
  if ext.Owner != nil {
    bytes, err := ext.Owner.MarshalBinary()
    if err != nil {
      return nil, err
    }
    copy(ret[0:16], bytes)
  }

  if ext.PendingOwner != nil {
    bytes, err := ext.PendingOwner.MarshalBinary()
    if err != nil {
      return nil, err
    }
    copy(ret[16:32], bytes)
  }

  binary.BigEndian.PutUint64(ret[32:40], uint64(len(ext.Requirements)))
  ret[40] = byte(ext.State)
  cur := 41
  for req, state := range(ext.Requirements) {
    bytes, err := req.MarshalBinary()
    if err != nil {
      return nil, err
    }
    copy(ret[cur:cur+16], bytes)
    ret[cur+16] = byte(state)
    cur += 17
  }

  return ret, nil
}

func (ext *LockableExt) Deserialize(ctx *Context, data []byte) error {
  cur := 0
  all_zero := true
  for _, b := range(data[cur:cur+16]) {
    if all_zero == true && b != 0x00 {
      all_zero = false
    }
  }
  if all_zero == false {
    tmp, err := IDFromBytes(data[cur:cur+16])
    if err != nil {
      return err
    }
    ext.Owner = &tmp
  }
  cur += 16

  all_zero = true
  for _, b := range(data[cur:cur+16]) {
    if all_zero == true && b != 0x00 {
      all_zero = false
    }
  }
  if all_zero == false {
    tmp, err := IDFromBytes(data[cur:cur+16])
    if err != nil {
      return err
    }
    ext.PendingOwner = &tmp
  }
  cur += 16

  num_requirements := int(binary.BigEndian.Uint64(data[cur:cur+8]))
  cur += 8

  ext.State = ReqState(data[cur])
  cur += 1

  if num_requirements != 0 {
    ext.Requirements = map[NodeID]ReqState{}
  }
  for i := 0; i < num_requirements; i++ {
    id, err := IDFromBytes(data[cur:cur+16])
    if err != nil {
      return err
    }
    cur += 16
    state := ReqState(data[cur])
    cur += 1
    ext.Requirements[id] = state
  }
  return nil
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
  msgs = msgs.Add(owner.ID, owner.Key, signal, target)
  return signal.ID(), ctx.Send(msgs)
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, owner *Node, target NodeID) (uuid.UUID, error) {
  msgs := Messages{}
  signal := NewLockSignal("lock")
  msgs = msgs.Add(owner.ID, owner.Key, signal, target)
  return signal.ID(), ctx.Send(msgs)
}

func (ext *LockableExt) HandleErrorSignal(log Logger, node *Node, source NodeID, signal *ErrorSignal) Messages {
  str := signal.Error
  log.Logf("lockable", "ERROR_SIGNAL: %s->%s %+v", source, node.ID, str)

  msgs := Messages {}
  switch str {
  case "not_unlocked":
    if ext.State == Locking {
      ext.State = AbortingLock
      ext.Requirements[source] = Unlocked
      for id, state := range(ext.Requirements) {
        if state == Locked {
          ext.Requirements[id] = Unlocking
          msgs = msgs.Add(node.ID, node.Key, NewLockSignal("unlock"), id)
        }
      }
    }
  case "not_locked":
    panic("RECEIVED not_locked, meaning a node thought it held a lock it didn't")
  case "not_requirement":
  }

  return msgs
}

func (ext *LockableExt) HandleLinkSignal(log Logger, node *Node, source NodeID, signal *IDStringSignal) Messages {
  id := signal.NodeID
  action := signal.Str
  msgs := Messages {}
  if ext.State == Unlocked {
    switch action {
    case "add":
      _, exists := ext.Requirements[id]
      if exists == true {
        msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "already_requirement"), source)
      } else {
        ext.Requirements[id] = Unlocked
        msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "req_added"), source)
      }
    case "remove":
      _, exists := ext.Requirements[id]
      if exists == false {
        msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
      } else {
        delete(ext.Requirements, id)
        msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "req_removed"), source)
      }
    default:
      msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "unknown_action"), source)
    }
  } else {
    msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocked"), source)
  }
  return msgs
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(log Logger, node *Node, source NodeID, signal *StringSignal) Messages {
  state := signal.Str
  log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, state)

  msgs := Messages{}
  switch state {
  case "locked":
    state, found := ext.Requirements[source]
    if found == false {
      msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
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
          msgs = msgs.Add(node.ID, node.Key, NewLockSignal("locked"), *ext.Owner)
        } else {
          log.Logf("lockable", "PARTIAL LOCK: %s - %d/%d", node.ID, locked, reqs)
        }
      } else if ext.State == AbortingLock {
        ext.Requirements[source] = Unlocking
        msgs = msgs.Add(node.ID, node.Key, NewLockSignal("unlock"), source)
      }
    }
  case "unlocked":
    state, found := ext.Requirements[source]
    if found == false {
      msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
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
          msgs = msgs.Add(node.ID, node.Key, NewLockSignal("unlocked"), *ext.Owner)
        } else if old_state == AbortingLock {
          msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(ext.ReqID, "not_unlocked"), *ext.PendingOwner)
          ext.PendingOwner = ext.Owner
        }
      } else {
        log.Logf("lockable", "PARTIAL UNLOCK: %s - %d/%d", node.ID, unlocked, reqs)
      }
    }
  case "lock":
    if ext.State == Unlocked {
      if len(ext.Requirements) == 0 {
        ext.State = Locked
        new_owner := source
        ext.PendingOwner = &new_owner
        ext.Owner = &new_owner
        msgs = msgs.Add(node.ID, node.Key, NewLockSignal("locked"), new_owner)
      } else {
        ext.State = Locking
        ext.ReqID = signal.ID()
        new_owner := source
        ext.PendingOwner = &new_owner
        for id, state := range(ext.Requirements) {
          if state != Unlocked {
            log.Logf("lockable", "REQ_NOT_UNLOCKED_WHEN_LOCKING")
          }
          ext.Requirements[id] = Locking
          lock_signal := NewLockSignal("lock")
          msgs = msgs.Add(node.ID, node.Key, lock_signal, id)
        }
      }
    } else {
      msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocked"), source)
    }
  case "unlock":
    if ext.State == Locked {
      if len(ext.Requirements) == 0 {
        ext.State = Unlocked
        new_owner := source
        ext.PendingOwner = nil
        ext.Owner = nil
        msgs = msgs.Add(node.ID, node.Key, NewLockSignal("unlocked"), new_owner)
      } else if source == *ext.Owner {
        ext.State = Unlocking
        ext.ReqID = signal.ID()
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state != Locked {
            log.Logf("lockable", "REQ_NOT_LOCKED_WHEN_UNLOCKING")
          }
          ext.Requirements[id] = Unlocking
          lock_signal := NewLockSignal("unlock")
          msgs = msgs.Add(node.ID, node.Key, lock_signal, id)
        }
      }
    } else {
      msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locked"), source)
    }
  default:
    log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
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
        messages = messages.Add(node.ID, node.Key, signal, *ext.Owner)
      }
    }
  case Down:
    for requirement, _ := range(ext.Requirements) {
      messages = messages.Add(node.ID, node.Key, signal, requirement)
    }
  case Direct:
    switch signal.Type() {
    case LinkSignalType:
      messages = ext.HandleLinkSignal(ctx.Log, node, source, signal.(*IDStringSignal))
    case LockSignalType:
      messages = ext.HandleLockSignal(ctx.Log, node, source, signal.(*StringSignal))
    case ErrorSignalType:
      messages = ext.HandleErrorSignal(ctx.Log, node, source, signal.(*ErrorSignal))
    default:
    }
  default:
  }
  return messages
}

