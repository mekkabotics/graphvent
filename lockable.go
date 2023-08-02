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
func (ext *ListenerExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
  ctx.Log.Logf("listener", "LISTENER_PROCESS: %s - %+v", node.ID, signal)
  select {
  case ext.Chan <- signal:
  default:
    ctx.Log.Logf("listener", "LISTENER_OVERFLOW: %s", node.ID)
  }
  return
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
  lock_signal := NewLockSignal("unlock")
  return ctx.Send(node.ID, node.ID, &lock_signal)
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, node *Node) error {
  lock_signal := NewLockSignal("lock")
  return ctx.Send(node.ID, node.ID, &lock_signal)
}

// Setup a node to send the initial requirement link signal, then send the signal
func LinkRequirement(ctx *Context, dependency NodeID, requirement NodeID) error {
  start_signal := NewLinkStartSignal("req", requirement)
  return ctx.Send(dependency, dependency, &start_signal)
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, source NodeID, node *Node, signal *StringSignal) {
  ctx.Log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.Str
  switch state {
  case "unlock":
    if ext.Owner == nil {
      resp := NewErrorSignal(signal.ID(), "already_unlocked")
      ctx.Send(node.ID, source, &resp)
    } else if source != *ext.Owner {
      resp := NewErrorSignal(signal.ID(), "not_owner")
      ctx.Send(node.ID, source, &resp)
    } else if ext.PendingOwner == nil {
      resp := NewErrorSignal(signal.ID(), "already_unlocking")
      ctx.Send(node.ID, source, &resp)
    } else {
      if len(ext.Requirements) == 0 {
        ext.Owner = nil
        ext.PendingOwner = nil
        resp := NewLockSignal("unlocked")
        ctx.Send(node.ID, source, &resp)
      } else {
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            if state.Lock != "locked" {
              panic("NOT_LOCKED")
            }
            state.Lock = "unlocking"
            ext.Requirements[id] = state
            resp := NewLockSignal("unlock")
            ctx.Send(node.ID, id, &resp)
          }
        }
        if source != node.ID {
          resp := NewLockSignal("unlocking")
          ctx.Send(node.ID, source, &resp)
        }
      }
    }
  case "unlocking":
    state, exists := ext.Requirements[source]
    if exists == false {
      resp := NewErrorSignal(signal.ID(), "not_requirement")
      ctx.Send(node.ID, source, &resp)
    } else if state.Link != "linked" {
      resp := NewErrorSignal(signal.ID(), "not_linked")
      ctx.Send(node.ID, source, &resp)
    } else if state.Lock != "unlocking" {
      resp := NewErrorSignal(signal.ID(), "not_unlocking")
      ctx.Send(node.ID, source, &resp)
    }

  case "unlocked":
    if source == node.ID {
      return
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      resp := NewErrorSignal(signal.ID(), "not_requirement")
      ctx.Send(node.ID, source, &resp)
    } else if state.Link != "linked" {
      resp := NewErrorSignal(signal.ID(), "not_linked")
      ctx.Send(node.ID, source, &resp)
    } else if state.Lock != "unlocking" {
      resp := NewErrorSignal(signal.ID(), "not_unlocking")
      ctx.Send(node.ID, source, &resp)
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
          resp := NewLockSignal("unlocked")
          ctx.Send(node.ID, previous_owner, &resp)
        }
      }
    }
  case "locked":
    if source == node.ID {
      return
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      resp := NewErrorSignal(signal.ID(), "not_requirement")
      ctx.Send(node.ID, source, &resp)
    } else if state.Link != "linked" {
      resp := NewErrorSignal(signal.ID(), "not_linked")
      ctx.Send(node.ID, source, &resp)
    } else if state.Lock != "locking" {
      resp := NewErrorSignal(signal.ID(), "not_locking")
      ctx.Send(node.ID, source, &resp)
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
          resp := NewLockSignal("locked")
          ctx.Send(node.ID, *ext.Owner, &resp)
        }
      }
    }
  case "locking":
    state, exists := ext.Requirements[source]
    if exists == false {
      resp := NewErrorSignal(signal.ID(), "not_requirement")
      ctx.Send(node.ID, source, &resp)
    } else if state.Link != "linked" {
      resp := NewErrorSignal(signal.ID(), "not_linked")
      ctx.Send(node.ID, source, &resp)
    } else if state.Lock != "locking" {
      resp := NewErrorSignal(signal.ID(), "not_locking")
      ctx.Send(node.ID, source, &resp)
    }

  case "lock":
    if ext.Owner != nil {
      resp := NewErrorSignal(signal.ID(), "already_locked")
      ctx.Send(node.ID, source, &resp)
    } else if ext.PendingOwner != nil {
      resp := NewErrorSignal(signal.ID(), "already_locking")
      ctx.Send(node.ID, source, &resp)
    } else {
      owner := source
      if len(ext.Requirements) == 0 {
        ext.Owner = &owner
        ext.PendingOwner = ext.Owner
        resp := NewLockSignal("locked")
        ctx.Send(node.ID, source, &resp)
      } else {
        ext.PendingOwner = &owner
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            if state.Lock != "unlocked" {
              panic("NOT_UNLOCKED")
            }
            state.Lock = "locking"
            ext.Requirements[id] = state
            sub := NewLockSignal("lock")
            ctx.Send(node.ID, id, &sub)
          }
        }
        if source != node.ID {
          resp := NewLockSignal("locking")
          ctx.Send(node.ID, source, &resp)
        }
      }
    }
  default:
    ctx.Log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
  }
}

func (ext *LockableExt) HandleLinkStartSignal(ctx *Context, source NodeID, node *Node, signal *IDStringSignal) {
  ctx.Log.Logf("lockable", "LINK__START_SIGNAL: %s->%s %+v", source, node.ID, signal)
  link_type := signal.Str
  target := signal.NodeID
  switch link_type {
  case "req":
    state, exists := ext.Requirements[target]
    _, dep_exists := ext.Dependencies[target]
    if ext.Owner != nil {
      resp := NewLinkStartSignal("locked", target)
      ctx.Send(node.ID, source, &resp)
    } else if ext.Owner != ext.PendingOwner {
      if ext.PendingOwner == nil {
        resp := NewLinkStartSignal("unlocking", target)
        ctx.Send(node.ID, source, &resp)
      } else {
        resp := NewLinkStartSignal("locking", target)
        ctx.Send(node.ID, source, &resp)
      }
    } else if exists == true {
      if state.Link == "linking" {
        resp := NewErrorSignal(signal.ID(), "already_linking_req")
        ctx.Send(node.ID, source, &resp)
      } else if state.Link == "linked" {
        resp := NewErrorSignal(signal.ID(), "already_req")
        ctx.Send(node.ID, source, &resp)
      }
    } else if dep_exists == true {
      resp := NewLinkStartSignal("already_dep", target)
      ctx.Send(node.ID, source, &resp)
    } else {
      ext.Requirements[target] = LinkState{"linking", "unlocked", source}
      resp := NewLinkSignal("linked_as_req")
      ctx.Send(node.ID, target, &resp)
      notify := NewLinkStartSignal("linking_req", target)
      ctx.Send(node.ID, source, &notify)
    }
  }
}

// Handle LinkSignal, updating the extensions requirements and dependencies as necessary
// TODO: Add unlink
func (ext *LockableExt) HandleLinkSignal(ctx *Context, source NodeID, node *Node, signal *StringSignal) {
  ctx.Log.Logf("lockable", "LINK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.Str
  switch state {
  case "linked_as_dep":
    state, exists := ext.Requirements[source]
    if exists == true && state.Link == "linked" {
      resp := NewLinkStartSignal("linked_as_req", source)
      ctx.Send(node.ID, state.Initiator, &resp)
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Requirements[source] = state
      resp := NewLinkSignal("linked_as_req")
      ctx.Send(node.ID, source, &resp)
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        resp := NewLinkSignal("locking")
        ctx.Send(node.ID, source, &resp)
      } else {
        resp := NewLinkSignal("unlocking")
        ctx.Send(node.ID, source, &resp)
      }
    } else {
      ext.Requirements[source] = LinkState{"linking", "unlocked", source}
      resp := NewLinkSignal("linked_as_req")
      ctx.Send(node.ID, source, &resp)
    }
    ctx.Log.Logf("lockable", "%s is a dependency of %s", node.ID, source)

  case "linked_as_req":
    state, exists := ext.Dependencies[source]
    if exists == true && state.Link == "linked" {
      resp := NewLinkStartSignal("linked_as_dep", source)
      ctx.Send(node.ID, state.Initiator, &resp)
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Dependencies[source] = state
      resp := NewLinkSignal("linked_as_dep")
      ctx.Send(node.ID, source, &resp)
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        resp := NewLinkSignal("locking")
        ctx.Send(node.ID, source, &resp)
      } else {
        resp := NewLinkSignal("unlocking")
        ctx.Send(node.ID, source, &resp)
      }
    } else {
      ext.Dependencies[source] = LinkState{"linking", "unlocked", source}
      resp := NewLinkSignal("linked_as_dep")
      ctx.Send(node.ID, source, &resp)
    }
    ctx.Log.Logf("lockable", "%s is a requirement of %s", node.ID, source)

  default:
    ctx.Log.Logf("lockable", "LINK_ERROR: unknown state %s", state)
  }
}

// LockableExts process Up/Down signals by forwarding them to owner, dependency, and requirement nodes
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, source NodeID, node *Node, signal Signal) {
  switch signal.Direction() {
  case Up:
    owner_sent := false
    for dependency, state := range(ext.Dependencies) {
      if state.Link == "linked" {
        err := ctx.Send(node.ID, dependency, signal)
        if err != nil {
          ctx.Log.Logf("signal", "LOCKABLE_SIGNAL_ERR: %s->%s - %e", node.ID, dependency, err)
        }

        if ext.Owner != nil {
          if dependency == *ext.Owner {
            owner_sent = true
          }
        }
      }
    }

    if ext.Owner != nil && owner_sent == false {
      if *ext.Owner != node.ID {
        err := ctx.Send(node.ID, *ext.Owner, signal)
        if err != nil {
          ctx.Log.Logf("signal", "LOCKABLE_SIGNAL_ERR: %s->%s - %e", node.ID, *ext.Owner, err)
        }
      }
    }
  case Down:
    for requirement, state := range(ext.Requirements) {
      if state.Link == "linked" {
        err := ctx.Send(node.ID, requirement, signal)
        if err != nil {
          ctx.Log.Logf("signal", "LOCKABLE_SIGNAL_ERR: %s->%s - %e", node.ID, requirement, err)
        }
      }
    }
  case Direct:
    switch signal.Type() {
    case LinkSignalType:
      ext.HandleLinkSignal(ctx, source, node, signal.(*StringSignal))
    case LockSignalType:
      ext.HandleLockSignal(ctx, source, node, signal.(*StringSignal))
    case LinkStartSignalType:
      ext.HandleLinkStartSignal(ctx, source, node, signal.(*IDStringSignal))
    default:
    }
  default:
  }
}

