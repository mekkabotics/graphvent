package graphvent

import (
  "encoding/binary"
)

type ReqState int
const (
  Unlocked = ReqState(0)
  Unlocking = ReqState(1)
  Locked = ReqState(2)
  Locking = ReqState(3)
)

type LockableExt struct{
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
  ret := make([]byte, 8 + (16 * 2) + (17 * len(ext.Requirements)))
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

  cur := 40
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
    Owner: nil,
    PendingOwner: nil,
    Requirements: reqs,
  }
}

// Send the signal to unlock a node from itself
func UnlockLockable(ctx *Context, node *Node) error {
  msgs := Messages{}
  msgs = msgs.Add(node.ID, node.Key, NewLockSignal("unlock"), node.ID)
  return ctx.Send(msgs)
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, node *Node) error {
  msgs := Messages{}
  msgs = msgs.Add(node.ID, node.Key, NewLockSignal("lock"), node.ID)
  return ctx.Send(msgs)
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(log Logger, node *Node, source NodeID, signal *StringSignal) Messages {
  state := signal.Str
  log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal)

  messages := Messages{}
  switch state {
  case "unlock":
    if ext.Owner == nil {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "already_unlocked"), source)
    } else if source != *ext.Owner {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_owner"), source)
    } else if ext.PendingOwner == nil {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "already_unlocking"), source)
    } else {
      if len(ext.Requirements) == 0 {
        ext.Owner = nil
        ext.PendingOwner = nil
        messages = messages.Add(node.ID, node.Key, NewLockSignal("unlocked"), source)
      } else {
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state != Locked {
            panic("NOT_LOCKED")
          }
          ext.Requirements[id] = Unlocking
          messages = messages.Add(node.ID, node.Key, NewLockSignal("unlock"), id)
        }
        if source != node.ID {
          messages = messages.Add(node.ID, node.Key, NewLockSignal("unlocking"), source)
        }
      }
    }
  case "unlocking":
    if ext.Requirements != nil {
      state, exists := ext.Requirements[source]
      if exists == false {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
      } else if state != Unlocking {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocking"), source)
      }
    }

  case "unlocked":
    if source == node.ID {
      return nil
    }

    if ext.Requirements != nil {
      state, exists := ext.Requirements[source]
      if exists == false {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
      } else if state != Unlocking {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocking"), source)
      } else {
        ext.Requirements[source] = Unlocked

        if ext.PendingOwner == nil {
          unlocked := 0
          for _, s := range(ext.Requirements) {
            if s == Unlocked {
              unlocked += 1
            }
          }

          if len(ext.Requirements) == unlocked {
            previous_owner := *ext.Owner
            ext.Owner = nil
            messages = messages.Add(node.ID, node.Key, NewLockSignal("unlocked"), previous_owner)
          }
        }
      }
    }
  case "locked":
    if source == node.ID {
      return nil
    }

    if ext.Requirements != nil {
      state, exists := ext.Requirements[source]
      if exists == false {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
      } else if state != Locking {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locking"), source)
      } else {
        ext.Requirements[source] = Locked

        if ext.PendingOwner != nil {
          locked := 0
          for _, s := range(ext.Requirements) {
            if s == Locked {
              locked += 1
            }
          }

          if len(ext.Requirements) == locked {
            ext.Owner = ext.PendingOwner
            messages = messages.Add(node.ID, node.Key, NewLockSignal("locked"), *ext.Owner)
          }
        }
      }
    }
  case "locking":
    if ext.Requirements != nil {
      state, exists := ext.Requirements[source]
      if exists == false {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
      } else if state != Locking {
        messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locking"), source)
      }
    }

  case "lock":
    if ext.Owner != nil {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "already_locked"), source)
    } else if ext.PendingOwner != nil {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "already_locking"), source)
    } else {
      owner := source
      if len(ext.Requirements) == 0 {
        ext.Owner = &owner
        ext.PendingOwner = ext.Owner
        messages = messages.Add(node.ID, node.Key, NewLockSignal("locked"), source)
      } else {
        ext.PendingOwner = &owner
        for id, state := range(ext.Requirements) {
          log.Logf("lockable_detail", "LOCK_REQ: %s sending 'lock' to %s", node.ID, id)
          if state != Unlocked {
            panic("NOT_UNLOCKED")
          }
          ext.Requirements[id] = Locking
          messages = messages.Add(node.ID, node.Key, NewLockSignal("lock"), id)
        }
        log.Logf("lockable", "LOCK_REQ: %s sending 'lock' to %d requirements", node.ID, len(ext.Requirements))
        if source != node.ID {
          messages = messages.Add(node.ID, node.Key, NewLockSignal("locking"), source)
        }
      }
    }
  default:
    log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
  }
  return messages
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
    case LockSignalType:
      messages = ext.HandleLockSignal(ctx.Log, node, source, signal.(*StringSignal))
    default:
    }
  default:
  }
  return messages
}

