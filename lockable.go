package graphvent

import (
  "encoding/json"
)

// A Listener extension provides a channel that can receive signals on a different thread
type ListenerExt struct {
  Buffer int
  Chan chan Signal
}

// Create a new listener extension with a given buffer size
func NewListenerExt(buffer int) *ListenerExt {
  return &ListenerExt{
    Buffer: buffer,
    Chan: make(chan Signal, buffer),
  }
}

func (ext *ListenerExt) Field(name string) interface{} {
  return ResolveFields(ext, name, map[string]func(*ListenerExt)interface{}{
    "buffer": func(ext *ListenerExt) interface{} {
      return ext.Buffer
    },
    "chan": func(ext *ListenerExt) interface{} {
      return ext.Chan
    },
  })
}

// Simple load function, unmarshal the buffer int from json
func (ext *ListenerExt) Deserialize(ctx *Context, data []byte) error {
  err := json.Unmarshal(data, &ext.Buffer)
  ext.Chan = make(chan Signal, ext.Buffer)
  return err
}

func (listener *ListenerExt) Type() ExtType {
  return ListenerExtType
}

// Send the signal to the channel, logging an overflow if it occurs
func (ext *ListenerExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  ctx.Log.Logf("listener", "LISTENER_PROCESS: %s - %+v", node.ID, signal)
  select {
  case ext.Chan <- signal:
  default:
    ctx.Log.Logf("listener", "LISTENER_OVERFLOW: %s", node.ID)
  }
  return nil
}

// ReqState holds the multiple states of a requirement
type LinkState struct {
  Link string `json:"link"`
  Lock string `json:"lock"`
  Initiator NodeID `json:"initiator"`
}

// A LockableExt allows a node to be linked to other nodes(via LinkSignal) and locked/unlocked(via LockSignal)
type LinkMap map[NodeID]LinkState
func (m LinkMap) MarshalJSON() ([]byte, error) {
  tmp := map[string]LinkState{}
  for id, state := range(m) {
    tmp[id.String()] = state
  }

  return json.Marshal(tmp)
}

func (m LinkMap) UnmarshalJSON(data []byte) error {
  tmp := map[string]LinkState{}
  err := json.Unmarshal(data, &tmp)
  if err != nil {
    return err
  }

  for id_str, state := range(tmp) {
    id, err := ParseID(id_str)
    if err != nil {
      return err
    }

    m[id] = state
  }
  return nil
}

type LockableExt struct {
  Owner *NodeID `json:"owner"`
  PendingOwner *NodeID `json:"pending_owner"`
  Requirements LinkMap `json:"requirements"`
  Dependencies LinkMap `json:"dependencies"`
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
    "dependencies": func(ext *LockableExt) interface{} {
      return ext.Dependencies
    },
  })
}

func (ext *ListenerExt) Serialize() ([]byte, error) {
  return json.Marshal(ext.Buffer)
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

func NewLockableExt() *LockableExt {
  return &LockableExt{
    Owner: nil,
    PendingOwner: nil,
    Requirements: map[NodeID]LinkState{},
    Dependencies: map[NodeID]LinkState{},
  }
}

// Send the signal to unlock a node from itself
func UnlockLockable(ctx *Context, node *Node) error {
  msgs := Messages{}
  msgs = msgs.Add(ctx.Log, node.ID, node.Key, NewLockSignal("unlock"), node.ID)
  return ctx.Send(msgs)
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, node *Node) error {
  msgs := Messages{}
  msgs = msgs.Add(ctx.Log, node.ID, node.Key, NewLockSignal("lock"), node.ID)
  return ctx.Send(msgs)
}

// Setup a node to send the initial requirement link signal, then send the signal
func LinkRequirement(ctx *Context, dependency *Node, requirement NodeID) error {
  msgs := Messages{}
  msgs = msgs.Add(ctx.Log, dependency.ID, dependency.Key, NewLinkStartSignal("req", requirement), dependency.ID)
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
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_unlocked"), source)
    } else if source != *ext.Owner {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_owner"), source)
    } else if ext.PendingOwner == nil {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_unlocking"), source)
    } else {
      if len(ext.Requirements) == 0 {
        ext.Owner = nil
        ext.PendingOwner = nil
        messages = messages.Add(log, node.ID, node.Key, NewLockSignal("unlocked"), source)
      } else {
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            if state.Lock != "locked" {
              panic("NOT_LOCKED")
            }
            state.Lock = "unlocking"
            ext.Requirements[id] = state
            messages = messages.Add(log, node.ID, node.Key, NewLockSignal("unlock"), id)
          }
        }
        if source != node.ID {
          messages = messages.Add(log, node.ID, node.Key, NewLockSignal("unlocking"), source)
        }
      }
    }
  case "unlocking":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state.Link != "linked" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_linked"), source)
    } else if state.Lock != "unlocking" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocking"), source)
    }

  case "unlocked":
    if source == node.ID {
      return nil
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state.Link != "linked" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_linked"), source)
    } else if state.Lock != "unlocking" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_unlocking"), source)
    } else {
      state.Lock = "unlocked"
      ext.Requirements[source] = state

      if ext.PendingOwner == nil {
        linked := 0
        unlocked := 0
        for _, s := range(ext.Requirements) {
          if s.Link == "linked" {
            linked += 1
          }
          if s.Lock == "unlocked" {
            unlocked += 1
          }
        }

        if linked == unlocked {
          previous_owner := *ext.Owner
          ext.Owner = nil
          messages = messages.Add(log, node.ID, node.Key, NewLockSignal("unlocked"), previous_owner)
        }
      }
    }
  case "locked":
    if source == node.ID {
      return nil
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state.Link != "linked" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_linked"), source)
    } else if state.Lock != "locking" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locking"), source)
    } else {
      state.Lock = "locked"
      ext.Requirements[source] = state

      if ext.PendingOwner != nil {
        linked := 0
        locked := 0
        for _, s := range(ext.Requirements) {
          if s.Link == "linked" {
            linked += 1
          }
          if s.Lock == "locked" {
            locked += 1
          }
        }

        if linked == locked {
          ext.Owner = ext.PendingOwner
          messages = messages.Add(log, node.ID, node.Key, NewLockSignal("locked"), *ext.Owner)
        }
      }
    }
  case "locking":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_requirement"), source)
    } else if state.Link != "linked" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_linked"), source)
    } else if state.Lock != "locking" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_locking"), source)
    }

  case "lock":
    if ext.Owner != nil {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_locked"), source)
    } else if ext.PendingOwner != nil {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_locking"), source)
    } else {
      owner := source
      if len(ext.Requirements) == 0 {
        ext.Owner = &owner
        ext.PendingOwner = ext.Owner
        messages = messages.Add(log, node.ID, node.Key, NewLockSignal("locked"), source)
      } else {
        ext.PendingOwner = &owner
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            log.Logf("lockable", "LOCK_REQ: %s sending 'lock' to %s", node.ID, id)
            if state.Lock != "unlocked" {
              panic("NOT_UNLOCKED")
            }
            state.Lock = "locking"
            ext.Requirements[id] = state
            messages = messages.Add(log, node.ID, node.Key, NewLockSignal("lock"), id)
          }
        }
        if source != node.ID {
          messages = messages.Add(log, node.ID, node.Key, NewLockSignal("locking"), source)
        }
      }
    }
  default:
    log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
  }
  log.Logf("lockable", "LOCK_MESSAGES: %+v", messages)
  return messages
}

func (ext *LockableExt) HandleLinkStartSignal(log Logger, node *Node, source NodeID, signal *IDStringSignal) Messages {
  link_type := signal.Str
  target := signal.NodeID
  log.Logf("lockable", "LINK_START_SIGNAL: %s->%s %s %s", source, node.ID, link_type, target)

  messages := Messages{}
  switch link_type {
  case "req":
    state, exists := ext.Requirements[target]
    _, dep_exists := ext.Dependencies[target]
    if ext.Owner != nil {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_locked"), source)
    } else if ext.Owner != ext.PendingOwner {
      if ext.PendingOwner == nil {
        messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "unlocking"), source)
      } else {
        messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "locking"), source)
      }
    } else if exists == true {
      if state.Link == "linking" {
        messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_linking_req"), source)
      } else if state.Link == "linked" {
        messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_req"), source)
      }
    } else if dep_exists == true {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_dep"), source)
    } else {
      ext.Requirements[target] = LinkState{"linking", "unlocked", source}
      messages = messages.Add(log, node.ID, node.Key, NewLinkSignal("linked_as_req"), target)
      messages = messages.Add(log, node.ID, node.Key, NewLinkStartSignal("linking_req", target), source)
    }
  }
  return messages
}

// Handle LinkSignal, updating the extensions requirements and dependencies as necessary
// TODO: Add unlink
func (ext *LockableExt) HandleLinkSignal(log Logger, node *Node, source NodeID, signal *StringSignal) Messages {
  log.Logf("lockable", "LINK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.Str

  messages := Messages{}
  switch state {
  case "dep_done":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "not_linking"), source)
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Requirements[source] = state
      log.Logf("lockable", "FINISHED_LINKING_REQ: %s->%s", node.ID, source)
    }
  case "linked_as_req":
    state, exists := ext.Dependencies[source]
    if exists == false {
      ext.Dependencies[source] = LinkState{"linked", "unlocked", source}
      messages = messages.Add(log, node.ID, node.Key, NewLinkSignal("dep_done"), source)
    } else if state.Link == "linking" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_linking"), source)
    } else if state.Link == "linked" {
      messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "already_linked"), source)
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "locking"), source)
      } else {
        messages = messages.Add(log, node.ID, node.Key, NewErrorSignal(signal.ID(), "unlocking"), source)
      }
    }

  default:
    log.Logf("lockable", "LINK_ERROR: unknown state %s", state)
  }
  return messages
}

// LockableExts process Up/Down signals by forwarding them to owner, dependency, and requirement nodes
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  messages := Messages{}
  switch signal.Direction() {
  case Up:
    ctx.Log.Logf("lockable", "LOCKABLE_DEPENDENCIES: %+v", ext.Dependencies)
    owner_sent := false
    for dependency, state := range(ext.Dependencies) {
      if state.Link == "linked" {
        messages = messages.Add(ctx.Log, node.ID, node.Key, signal, dependency)
        if ext.Owner != nil {
          if dependency == *ext.Owner {
            owner_sent = true
          }
        }
      }
    }

    if ext.Owner != nil && owner_sent == false {
      if *ext.Owner != node.ID {
        messages = messages.Add(ctx.Log, node.ID, node.Key, signal, *ext.Owner)
      }
    }
  case Down:
    for requirement, state := range(ext.Requirements) {
      if state.Link == "linked" {
        messages = messages.Add(ctx.Log, node.ID, node.Key, signal, requirement)
      }
    }
  case Direct:
    switch signal.Type() {
    case LinkSignalType:
      messages = ext.HandleLinkSignal(ctx.Log, node, source, signal.(*StringSignal))
    case LockSignalType:
      messages = ext.HandleLockSignal(ctx.Log, node, source, signal.(*StringSignal))
    case LinkStartSignalType:
      messages = ext.HandleLinkStartSignal(ctx.Log, node, source, signal.(*IDStringSignal))
    default:
    }
  default:
  }
  return messages
}

