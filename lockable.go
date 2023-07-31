package graphvent

import (
  "encoding/json"
  "fmt"
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
func LoadListenerExt(ctx *Context, data []byte) (Extension, error) {
  var j int
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  return NewListenerExt(j), nil
}

func (listener *ListenerExt) Type() ExtType {
  return ListenerExtType
}

// Send the signal to the channel, logging an overflow if it occurs
func (ext *ListenerExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
  ctx.Log.Logf("signal", "LISTENER_PROCESS: %s - %+v", node.ID, signal)
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

func LoadLockableExt(ctx *Context, data []byte) (Extension, error) {
  var ext LockableExt
  err := json.Unmarshal(data, &ext)
  if err != nil {
    return nil, err
  }

  return &ext, nil
}

func (ext *ListenerExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext.Buffer, "", "  ")
}

func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func (ext *LockableExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext, "", "  ")
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
  return ctx.Send(node.ID, node.ID, NewLockSignal("unlock"))
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, node *Node) error {
  return ctx.Send(node.ID, node.ID, NewLockSignal("lock"))
}

// Setup a node to send the initial requirement link signal, then send the signal
func LinkRequirement(ctx *Context, dependency NodeID, requirement NodeID) error {
  return ctx.Send(dependency, dependency, NewLinkStartSignal("req", requirement))
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, source NodeID, node *Node, signal StringSignal) {
  ctx.Log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.Str
  switch state {
  case "unlock":
    if ext.Owner == nil {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("already_unlocked")))
    } else if source != *ext.Owner {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_owner")))
    } else if ext.PendingOwner == nil {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("already_unlocking")))
    } else {
      if len(ext.Requirements) == 0 {
        ext.Owner = nil
        ext.PendingOwner = nil
        ctx.Send(node.ID, source, NewLockSignal("unlocked"))
      } else {
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            if state.Lock != "locked" {
              panic("NOT_LOCKED")
            }
            state.Lock = "unlocking"
            ext.Requirements[id] = state
            ctx.Send(node.ID, id, NewLockSignal("unlock"))
          }
        }
        if source != node.ID {
          ctx.Send(node.ID, source, NewLockSignal("unlocking"))
        }
      }
    }
  case "unlocking":
    state, exists := ext.Requirements[source]
    if exists == false {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_requirement")))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("node_not_linked")))
    } else if state.Lock != "unlocking" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_unlocking")))
    }

  case "unlocked":
    if source == node.ID {
      return
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_requirement")))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_linked")))
    } else if state.Lock != "unlocking" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_unlocking")))
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
          ctx.Send(node.ID, previous_owner, NewLockSignal("unlocked"))
        }
      }
    }
  case "locked":
    if source == node.ID {
      return
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_requirement")))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_linked")))
    } else if state.Lock != "locking" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_locking")))
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
          ctx.Send(node.ID, *ext.Owner, NewLockSignal("locked"))
        }
      }
    }
  case "locking":
    state, exists := ext.Requirements[source]
    if exists == false {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_requirement")))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("node_not_linked")))
    } else if state.Lock != "locking" {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("not_locking")))
    }

  case "lock":
    if ext.Owner != nil {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("already_locked")))
    } else if ext.PendingOwner != nil {
      ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("already_locking")))
    } else {
      owner := source
      if len(ext.Requirements) == 0 {
        ext.Owner = &owner
        ext.PendingOwner = ext.Owner
        ctx.Send(node.ID, source, NewLockSignal("locked"))
      } else {
        ext.PendingOwner = &owner
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            if state.Lock != "unlocked" {
              panic("NOT_UNLOCKED")
            }
            state.Lock = "locking"
            ext.Requirements[id] = state
            ctx.Send(node.ID, id, NewLockSignal("lock"))
          }
        }
        if source != node.ID {
          ctx.Send(node.ID, source, NewLockSignal("locking"))
        }
      }
    }
  default:
    ctx.Log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
  }
}

func (ext *LockableExt) HandleLinkStartSignal(ctx *Context, source NodeID, node *Node, signal IDStringSignal) {
  ctx.Log.Logf("lockable", "LINK__START_SIGNAL: %s->%s %+v", source, node.ID, signal)
  link_type := signal.Str
  target := signal.ID
  switch link_type {
  case "req":
    state, exists := ext.Requirements[target]
    _, dep_exists := ext.Dependencies[target]
    if ext.Owner != nil {
      ctx.Send(node.ID, source, NewLinkStartSignal("locked", target))
    } else if ext.Owner != ext.PendingOwner {
      if ext.PendingOwner == nil {
        ctx.Send(node.ID, source, NewLinkStartSignal("unlocking", target))
      } else {
        ctx.Send(node.ID, source, NewLinkStartSignal("locking", target))
      }
    } else if exists == true {
      if state.Link == "linking" {
        ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("already_linking_req")))
      } else if state.Link == "linked" {
        ctx.Send(node.ID, source, NewErrorSignal(fmt.Errorf("already_req")))
      }
    } else if dep_exists == true {
      ctx.Send(node.ID, source, NewLinkStartSignal("already_dep", target))
    } else {
      ext.Requirements[target] = LinkState{"linking", "unlocked", source}
      ctx.Send(node.ID, target, NewLinkSignal("linked_as_req"))
      ctx.Send(node.ID, source, NewLinkStartSignal("linking_req", target))
    }
  }
}

// Handle LinkSignal, updating the extensions requirements and dependencies as necessary
// TODO: Add unlink
func (ext *LockableExt) HandleLinkSignal(ctx *Context, source NodeID, node *Node, signal StringSignal) {
  ctx.Log.Logf("lockable", "LINK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.Str
  switch state {
  case "linked_as_dep":
    state, exists := ext.Requirements[source]
    if exists == true && state.Link == "linked" {
      ctx.Send(node.ID, state.Initiator, NewLinkStartSignal("linked_as_req", source))
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Requirements[source] = state
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_req"))
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        ctx.Send(node.ID, source, NewLinkSignal("locking"))
      } else {
        ctx.Send(node.ID, source, NewLinkSignal("unlocking"))
      }
    } else {
      ext.Requirements[source] = LinkState{"linking", "unlocked", source}
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_req"))
    }
    ctx.Log.Logf("lockable", "%s is a dependency of %s", node.ID, source)

  case "linked_as_req":
    state, exists := ext.Dependencies[source]
    if exists == true && state.Link == "linked" {
      ctx.Send(node.ID, state.Initiator, NewLinkStartSignal("linked_as_dep", source))
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Dependencies[source] = state
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_dep"))
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        ctx.Send(node.ID, source, NewLinkSignal("locking"))
      } else {
        ctx.Send(node.ID, source, NewLinkSignal("unlocking"))
      }
    } else {
      ext.Dependencies[source] = LinkState{"linking", "unlocked", source}
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_dep"))
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
      ext.HandleLinkSignal(ctx, source, node, signal.(StringSignal))
    case LockSignalType:
      ext.HandleLockSignal(ctx, source, node, signal.(StringSignal))
    case LinkStartSignalType:
      ext.HandleLinkStartSignal(ctx, source, node, signal.(IDStringSignal))
    default:
    }
  default:
  }
}

