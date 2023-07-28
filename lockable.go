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
type ReqState struct {
  Link string `json:"link"`
  Lock string `json:"lock"`
}

// A LockableExt allows a node to be linked to other nodes(via LinkSignal) and locked/unlocked(via LockSignal)
type LockableExt struct {
  Owner *NodeID
  PendingOwner *NodeID
  Requirements map[NodeID]ReqState
  Dependencies map[NodeID]string
}

type LockableExtJSON struct {
  Owner *NodeID `json:"owner"`
  PendingOwner *NodeID `json:"pending_owner"`
  Requirements map[string]ReqState `json:"requirements"`
  Dependencies map[string]string `json:"dependencies"`
}

func LoadLockableExt(ctx *Context, data []byte) (Extension, error) {
  var j LockableExtJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  requirements, err := LoadIDMap(j.Requirements)
  if err != nil {
    return nil, err
  }

  dependencies, err := LoadIDMap(j.Dependencies)
  if err != nil {
    return nil, err
  }

  return &LockableExt{
    Owner: j.Owner,
    PendingOwner: j.PendingOwner,
    Requirements: requirements,
    Dependencies: dependencies,
  }, nil
}

func (ext *ListenerExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext.Buffer, "", "  ")
}

func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func (ext *LockableExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(&LockableExtJSON{
    Owner: ext.Owner,
    PendingOwner: ext.PendingOwner,
    Requirements: IDMap(ext.Requirements),
    Dependencies: IDMap(ext.Dependencies),
  }, "", "  ")
}

func NewLockableExt() *LockableExt {
  return &LockableExt{
    Owner: nil,
    PendingOwner: nil,
    Requirements: map[NodeID]ReqState{},
    Dependencies: map[NodeID]string{},
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
func LinkRequirement(ctx *Context, dependency *Node, requirement NodeID) error {
  dep_ext, err := GetExt[*LockableExt](dependency)
  if err != nil {
    return err
  }

  _, exists := dep_ext.Requirements[requirement]
  if exists == true {
    return fmt.Errorf("%s is already a requirement of %s", requirement, dependency.ID)
  }

  _, exists = dep_ext.Dependencies[requirement]
  if exists == true {
    return fmt.Errorf("%s is a dependency of %s, cannot link as requirement", requirement, dependency.ID)
  }

  dep_ext.Requirements[requirement] = ReqState{"linking", "unlocked"}
  return ctx.Send(dependency.ID, requirement, NewLinkSignal("link_as_req"))
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(ctx *Context, source NodeID, node *Node, signal StateSignal) {
  ctx.Log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.State
  switch state {
  case "unlock":
    if ext.Owner == nil {
      ctx.Send(node.ID, source, NewLockSignal("already_unlocked"))
    } else if source != *ext.Owner {
      ctx.Send(node.ID, source, NewLockSignal("not_owner"))
    } else if ext.PendingOwner == nil {
      ctx.Send(node.ID, source, NewLockSignal("already_unlocking"))
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
      ctx.Send(node.ID, source, NewLockSignal("not_requirement"))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewLockSignal("node_not_linked"))
    } else if state.Lock != "unlocking" {
      ctx.Send(node.ID, source, NewLockSignal("not_unlocking"))
    }

  case "unlocked":
    if source == node.ID {
      return
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      ctx.Send(node.ID, source, NewLockSignal("not_requirement"))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewLockSignal("not_linked"))
    } else if state.Lock != "unlocking" {
      ctx.Send(node.ID, source, NewLockSignal("not_unlocking"))
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
      ctx.Send(node.ID, source, NewLockSignal("not_requirement"))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewLockSignal("not_linked"))
    } else if state.Lock != "locking" {
      ctx.Send(node.ID, source, NewLockSignal("not_locking"))
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
      ctx.Send(node.ID, source, NewLockSignal("not_requirement"))
    } else if state.Link != "linked" {
      ctx.Send(node.ID, source, NewLockSignal("node_not_linked"))
    } else if state.Lock != "locking" {
      ctx.Send(node.ID, source, NewLockSignal("not_locking"))
    }

  case "lock":
    if ext.Owner != nil {
      ctx.Send(node.ID, source, NewLockSignal("already_locked"))
    } else if ext.PendingOwner != nil {
      ctx.Send(node.ID, source, NewLockSignal("already_locking"))
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

// Handle LinkSignal, updating the extensions requirements and dependencies as necessary
// TODO: Add unlink
func (ext *LockableExt) HandleLinkSignal(ctx *Context, source NodeID, node *Node, signal StateSignal) {
  ctx.Log.Logf("lockable", "LINK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.State
  switch state {
  case "link_as_dep":
    state, exists := ext.Requirements[source]
    if exists == true && state.Link == "linked" {
      ctx.Send(node.ID, source, NewLinkSignal("already_req"))
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Requirements[source] = state
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_dep"))
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        ctx.Send(node.ID, source, NewLinkSignal("locking"))
      } else {
        ctx.Send(node.ID, source, NewLinkSignal("unlocking"))
      }
    } else {
      ext.Requirements[source] = ReqState{"linking", "unlocked"}
      ctx.Send(node.ID, source, NewLinkSignal("link_as_req"))
    }

  case "link_as_req":
    state, exists := ext.Dependencies[source]
    if exists == true && state == "linked" {
      ctx.Send(node.ID, source, NewLinkSignal("already_dep"))
    } else if state == "linking" {
      ext.Dependencies[source] = "linked"
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_req"))
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        ctx.Send(node.ID, source, NewLinkSignal("locking"))
      } else {
        ctx.Send(node.ID, source, NewLinkSignal("unlocking"))
      }
    } else {
      ext.Dependencies[source] = "linking"
      ctx.Send(node.ID, source, NewLinkSignal("link_as_dep"))
    }
  case "linked_as_dep":
    state, exists := ext.Dependencies[source]
    if exists == false {
      ctx.Send(node.ID, source, NewLinkSignal("not_linking"))
    } else if state == "linked" {
    } else if state == "linking" {
      ext.Dependencies[source] = "linked"
      ctx.Send(node.ID, source, NewLinkSignal("linked_as_req"))
    }
    ctx.Log.Logf("lockable", "%s is a dependency of %s", node.ID, source)

  case "linked_as_req":
    state, exists := ext.Requirements[source]
    if exists == false {
      ctx.Send(node.ID, source, NewLinkSignal("not_linking"))
    } else if state.Link == "linked" {
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Requirements[source] = state
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
  ctx.Log.Logf("signal", "LOCKABLE_PROCESS: %s", node.ID)

  switch signal.Direction() {
  case Up:
    owner_sent := false
    for dependency, state := range(ext.Dependencies) {
      if state == "linked" {
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
      ext.HandleLinkSignal(ctx, source, node, signal.(StateSignal))
    case LockSignalType:
      ext.HandleLockSignal(ctx, source, node, signal.(StateSignal))
    default:
    }
  default:
  }
}

