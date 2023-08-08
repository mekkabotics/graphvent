package graphvent

import (
  "encoding/json"
)

// A Listener extension provides a channel that can receive signals on a different thread
type ListenerExt struct {
  Buffer int
  Chan chan Message
}

// Create a new listener extension with a given buffer size
func NewListenerExt(buffer int) *ListenerExt {
  return &ListenerExt{
    Buffer: buffer,
    Chan: make(chan Message, buffer),
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
  ext.Chan = make(chan Message, ext.Buffer)
  return err
}

func (listener *ListenerExt) Type() ExtType {
  return ListenerExtType
}

// Send the signal to the channel, logging an overflow if it occurs
func (ext *ListenerExt) Process(ctx *Context, node *Node, msg Message) []Message {
  ctx.Log.Logf("listener", "LISTENER_PROCESS: %s - %+v", node.ID, msg.Signal)
  select {
  case ext.Chan <- msg:
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
  return ctx.Send(node.ID, []Message{Message{node.ID, NewLockSignal("unlock")}})
}

// Send the signal to lock a node from itself
func LockLockable(ctx *Context, node *Node) error {
  return ctx.Send(node.ID, []Message{Message{node.ID, NewLockSignal("lock")}})
}

// Setup a node to send the initial requirement link signal, then send the signal
func LinkRequirement(ctx *Context, dependency NodeID, requirement NodeID) error {
  return ctx.Send(dependency, []Message{Message{dependency, NewLinkStartSignal("req", requirement)}})
}

// Handle a LockSignal and update the extensions owner/requirement states
func (ext *LockableExt) HandleLockSignal(log Logger, node *Node, source NodeID, signal *StringSignal) []Message {
  state := signal.Str
  log.Logf("lockable", "LOCK_SIGNAL: %s->%s %+v", source, node.ID, signal)

  messages := []Message{}
  switch state {
  case "unlock":
    if ext.Owner == nil {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_unlocked")})
    } else if source != *ext.Owner {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_owner")})
    } else if ext.PendingOwner == nil {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_unlocking")})
    } else {
      if len(ext.Requirements) == 0 {
        ext.Owner = nil
        ext.PendingOwner = nil
        messages = append(messages, Message{source, NewLockSignal("unlocked")})
      } else {
        ext.PendingOwner = nil
        for id, state := range(ext.Requirements) {
          if state.Link == "linked" {
            if state.Lock != "locked" {
              panic("NOT_LOCKED")
            }
            state.Lock = "unlocking"
            ext.Requirements[id] = state
            messages = append(messages, Message{id, NewLockSignal("unlock")})
          }
        }
        if source != node.ID {
          messages = append(messages, Message{source, NewLockSignal("unlocking")})
        }
      }
    }
  case "unlocking":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_requirement")})
    } else if state.Link != "linked" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_linked")})
    } else if state.Lock != "unlocking" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_unlocking")})
    }

  case "unlocked":
    if source == node.ID {
      return nil
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_requirement")})
    } else if state.Link != "linked" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_linked")})
    } else if state.Lock != "unlocking" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_unlocking")})
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
          messages = append(messages, Message{previous_owner, NewLockSignal("unlocked")})
        }
      }
    }
  case "locked":
    if source == node.ID {
      return nil
    }

    state, exists := ext.Requirements[source]
    if exists == false {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_requirement")})
    } else if state.Link != "linked" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_linked")})
    } else if state.Lock != "locking" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_locking")})
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
          messages = append(messages, Message{*ext.Owner, NewLockSignal("locked")})
        }
      }
    }
  case "locking":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_requirement")})
    } else if state.Link != "linked" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_linked")})
    } else if state.Lock != "locking" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_locking")})
    }

  case "lock":
    if ext.Owner != nil {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_locked")})
    } else if ext.PendingOwner != nil {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_locking")})
    } else {
      owner := source
      if len(ext.Requirements) == 0 {
        ext.Owner = &owner
        ext.PendingOwner = ext.Owner
        messages = append(messages, Message{source, NewLockSignal("locked")})
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
            messages = append(messages, Message{id, NewLockSignal("lock")})
          }
        }
        if source != node.ID {
          messages = append(messages, Message{source, NewLockSignal("locking")})
        }
      }
    }
  default:
    log.Logf("lockable", "LOCK_ERR: unkown state %s", state)
  }
  log.Logf("lockable", "LOCK_MESSAGES: %+v", messages)
  return messages
}

func (ext *LockableExt) HandleLinkStartSignal(log Logger, node *Node, source NodeID, signal *IDStringSignal) []Message {
  link_type := signal.Str
  target := signal.NodeID
  log.Logf("lockable", "LINK_START_SIGNAL: %s->%s %s %s", source, node.ID, link_type, target)

  messages := []Message{}
  switch link_type {
  case "req":
    state, exists := ext.Requirements[target]
    _, dep_exists := ext.Dependencies[target]
    if ext.Owner != nil {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already locked")})
    } else if ext.Owner != ext.PendingOwner {
      if ext.PendingOwner == nil {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "unlocking")})
      } else {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "locking")})
      }
    } else if exists == true {
      if state.Link == "linking" {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_linking_req")})
      } else if state.Link == "linked" {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_req")})
      }
    } else if dep_exists == true {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_dep")})
    } else {
      ext.Requirements[target] = LinkState{"linking", "unlocked", source}
      messages = append(messages, Message{target, NewLinkSignal("linked_as_req")})
      messages = append(messages, Message{source, NewLinkStartSignal("linking_req", target)})
    }
  }
  return messages
}

// Handle LinkSignal, updating the extensions requirements and dependencies as necessary
// TODO: Add unlink
func (ext *LockableExt) HandleLinkSignal(log Logger, node *Node, source NodeID, signal *StringSignal) []Message {
  log.Logf("lockable", "LINK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.Str

  messages := []Message{}
  switch state {
  case "dep_done":
    state, exists := ext.Requirements[source]
    if exists == false {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "not_linking")})
    } else if state.Link == "linking" {
      state.Link = "linked"
      ext.Requirements[source] = state
      log.Logf("lockable", "FINISHED_LINKING_REQ: %s->%s", node.ID, source)
    }
  case "linked_as_req":
    state, exists := ext.Dependencies[source]
    if exists == false {
      ext.Dependencies[source] = LinkState{"linked", "unlocked", source}
      messages = append(messages, Message{source, NewLinkSignal("dep_done")})
    } else if state.Link == "linking" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_linking")})
    } else if state.Link == "linked" {
      messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "already_linked")})
    } else if ext.PendingOwner != ext.Owner {
      if ext.Owner == nil {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "locking")})
      } else {
        messages = append(messages, Message{source, NewErrorSignal(signal.ID(), "unlocking")})
      }
    }

  default:
    log.Logf("lockable", "LINK_ERROR: unknown state %s", state)
  }
  return messages
}

// LockableExts process Up/Down signals by forwarding them to owner, dependency, and requirement nodes
// LockSignal and LinkSignal Direct signals are processed to update the requirement/dependency/lock state
func (ext *LockableExt) Process(ctx *Context, node *Node, msg Message) []Message {
  messages := []Message{}
  switch msg.Signal.Direction() {
  case Up:
    ctx.Log.Logf("lockable", "LOCKABLE_DEPENDENCIES: %+v", ext.Dependencies)
    owner_sent := false
    for dependency, state := range(ext.Dependencies) {
      if state.Link == "linked" {
        messages = append(messages, Message{dependency, msg.Signal})
        if ext.Owner != nil {
          if dependency == *ext.Owner {
            owner_sent = true
          }
        }
      }
    }

    if ext.Owner != nil && owner_sent == false {
      if *ext.Owner != node.ID {
        messages = append(messages, Message{*ext.Owner, msg.Signal})
      }
    }
  case Down:
    for requirement, state := range(ext.Requirements) {
      if state.Link == "linked" {
        messages = append(messages, Message{requirement, msg.Signal})
      }
    }
  case Direct:
    switch msg.Signal.Type() {
    case LinkSignalType:
      messages = ext.HandleLinkSignal(ctx.Log, node, msg.NodeID, msg.Signal.(*StringSignal))
    case LockSignalType:
      messages = ext.HandleLockSignal(ctx.Log, node, msg.NodeID, msg.Signal.(*StringSignal))
    case LinkStartSignalType:
      messages = ext.HandleLinkStartSignal(ctx.Log, node, msg.NodeID, msg.Signal.(*IDStringSignal))
    default:
    }
  default:
  }
  return messages
}

