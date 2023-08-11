package graphvent

import (
  "encoding/json"
)

type LockableExt struct {
  Owner *NodeID `json:"owner"`
  PendingOwner *NodeID `json:"pending_owner"`
  Requirements map[NodeID]string `json:"requirements"`
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
  return json.Marshal(ext)
}

func (ext *LockableExt) Deserialize(ctx *Context, data []byte) error {
  return json.Unmarshal(data, ext)
}

func NewLockableExt(requirements []NodeID) *LockableExt {
  reqs := map[NodeID]string{}
  for _, id := range(requirements) {
    reqs[id] = "unlocked"
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
          if state != "locked" {
            panic("NOT_LOCKED")
          }
          ext.Requirements[id] = "unlocking"
          messages = messages.Add(node.ID, node.Key, NewLockSignal("unlock"), id)
        }
        if source != node.ID {
          messages = messages.Add(node.ID, node.Key, NewLockSignal("unlocking"), source)
        }
      }
    }
  case "unlocking":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state != "unlocking" {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocking"), source)
    }

  case "unlocked":
    if source == node.ID {
      return nil
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state != "unlocking" {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocking"), source)
    } else {
      ext.Requirements[source] = "unlocked"

      if ext.PendingOwner == nil {
        unlocked := 0
        for _, s := range(ext.Requirements) {
          if s == "unlocked" {
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
  case "locked":
    if source == node.ID {
      return nil
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state != "locking" {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locking"), source)
    } else {
      ext.Requirements[source] = "locked"

      if ext.PendingOwner != nil {
        locked := 0
        for _, s := range(ext.Requirements) {
          if s == "locked" {
            locked += 1
          }
        }

        if len(ext.Requirements) == locked {
          ext.Owner = ext.PendingOwner
          messages = messages.Add(node.ID, node.Key, NewLockSignal("locked"), *ext.Owner)
        }
      }
    }
  case "locking":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state != "locking" {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locking"), source)
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
          log.Logf("lockable", "LOCK_REQ: %s sending 'lock' to %s", node.ID, id)
          if state != "unlocked" {
            panic("NOT_UNLOCKED")
          }
          ext.Requirements[id] = "locking"
          messages = messages.Add(node.ID, node.Key, NewLockSignal("lock"), id)
        }
        if source != node.ID {
          messages = messages.Add(node.ID, node.Key, NewLockSignal("locking"), source)
        }
      }
    }
  default:
    log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
  }
  log.Logf("lockable", "LOCK_MESSAGES: %+v", messages)
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

