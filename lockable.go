package graphvent

import (
  "encoding/json"
  "fmt"
)

type ListenerExt struct {
  Buffer int
  Chan chan Signal
}

func NewListenerExt(buffer int) *ListenerExt {
  return &ListenerExt{
    Buffer: buffer,
    Chan: make(chan Signal, buffer),
  }
}

func LoadListenerExt(ctx *Context, data []byte) (Extension, error) {
  var j int
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  return NewListenerExt(j), nil
}

const ListenerExtType = ExtType("LISTENER")
func (listener *ListenerExt) Type() ExtType {
  return ListenerExtType
}

func (ext *ListenerExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
  ctx.Log.Logf("signal", "LISTENER_PROCESS: %s - %+v", node.ID, signal)
  select {
  case ext.Chan <- signal:
  default:
    ctx.Log.Logf("listener", "LISTENER_OVERFLOW: %s", node.ID)
  }
  return
}

func LoadLockableExt(ctx *Context, data []byte) (Extension, error) {
  var ext LockableExt
  err := json.Unmarshal(data, &ext)
  if err != nil {
    return nil, err
  }

  ctx.Log.Logf("db", "DB_LOADING_LOCKABLE_EXT_JSON: %+v", ext)

  return &ext, nil
}

func (ext *ListenerExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext.Buffer, "", "  ")
}

const LockableExtType = ExtType("LOCKABLE")
func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func (ext *LockableExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext, "", "  ")
}

func NewLockableExt(owner *NodeID, requirements map[NodeID]string, dependencies map[NodeID]string, locks_held map[NodeID]*NodeID) *LockableExt {
  if requirements == nil {
    requirements = map[NodeID]string{}
  }

  if dependencies == nil {
    dependencies = map[NodeID]string{}
  }

  if locks_held == nil {
    locks_held = map[NodeID]*NodeID{}
  }

  return &LockableExt{
    Owner: owner,
    Requirements: requirements,
    Dependencies: dependencies,
    LocksHeld: locks_held,
  }
}

type LockableExt struct {
  Owner *NodeID `json:"owner"`
  Requirements map[NodeID]string `json:"requirements"`
  Dependencies map[NodeID]string `json:"dependencies"`
  LocksHeld map[NodeID]*NodeID `json:"locks_held"`
}

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

  dep_ext.Requirements[requirement] = "start"
  return ctx.Send(dependency.ID, requirement, NewLinkSignal("req_link"))
}

func (ext *LockableExt) HandleLinkSignal(ctx *Context, source NodeID, node *Node, signal LinkSignal) {
  ctx.Log.Logf("lockable", "LINK_SIGNAL: %s->%s %+v", source, node.ID, signal)
  state := signal.State
  switch state {
  // sent by a node to link this node as a requirement
  case "req_link":
    _, exists := ext.Requirements[source]
    if exists == false {
      dep_state, exists := ext.Dependencies[source]
      if exists == false {
        ext.Dependencies[source] = "start"
        ctx.Send(node.ID, source, NewLinkSignal("dep_link"))
      } else if dep_state == "start" {
        ext.Dependencies[source] = "linked"
        ctx.Send(node.ID, source, NewLinkSignal("dep_linked"))
      }
    } else {
      delete(ext.Requirements, source)
      ctx.Send(node.ID, source, NewLinkSignal("req_reset"))
    }
  case "dep_link":
    _, exists := ext.Dependencies[source]
    if exists == false {
      req_state, exists := ext.Requirements[source]
      if exists == false {
        ext.Requirements[source] = "start"
        ctx.Send(node.ID, source, NewLinkSignal("req_link"))
      } else if req_state == "start" {
        ext.Requirements[source] = "linked"
        ctx.Send(node.ID, source, NewLinkSignal("req_linked"))
      }
    } else {
      delete(ext.Dependencies, source)
      ctx.Send(node.ID, source, NewLinkSignal("dep_reset"))
    }
  case "dep_reset":
    ctx.Log.Logf("lockable", "%s reset %s dependency state", node.ID, source)
  case "req_reset":
    ctx.Log.Logf("lockable", "%s reset %s requirement state", node.ID, source)
  case "dep_linked":
    ctx.Log.Logf("lockable", "%s is a dependency of %s", node.ID, source)
    req_state, exists := ext.Requirements[source]
    if exists == true && req_state == "start" {
      ext.Requirements[source] = "linked"
      ctx.Send(node.ID, source, NewLinkSignal("req_linked"))
    }

  case "req_linked":
    ctx.Log.Logf("lockable", "%s is a requirement of %s", node.ID, source)
    dep_state, exists := ext.Dependencies[source]
    if exists == true && dep_state == "start" {
      ext.Dependencies[source] = "linked"
      ctx.Send(node.ID, source, NewLinkSignal("dep_linked"))
    }

  default:
    ctx.Log.Logf("lockable", "LINK_ERROR: unknown state %s", state)
  }
}

func (ext *LockableExt) Process(ctx *Context, source NodeID, node *Node, signal Signal) {
  ctx.Log.Logf("signal", "LOCKABLE_PROCESS: %s", node.ID)

  switch signal.Direction() {
  case Up:
    owner_sent := false
    for dependency, _ := range(ext.Dependencies) {
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

    if ext.Owner != nil && owner_sent == false {
      if *ext.Owner != node.ID {
        err := ctx.Send(node.ID, *ext.Owner, signal)
        if err != nil {
          ctx.Log.Logf("signal", "LOCKABLE_SIGNAL_ERR: %s->%s - %e", node.ID, *ext.Owner, err)
        }
      }
    }
  case Down:
    for requirement, _ := range(ext.Requirements) {
      err := ctx.Send(node.ID, requirement, signal)
      if err != nil {
        ctx.Log.Logf("signal", "LOCKABLE_SIGNAL_ERR: %s->%s - %e", node.ID, requirement, err)
      }
    }
  case Direct:
    switch signal.Type() {
    case LinkSignalType:
      ext.HandleLinkSignal(ctx, source, node, signal.(LinkSignal))
    default:
    }
  default:
  }
}

func (ext *LockableExt) RecordUnlock(node NodeID) *NodeID {
  last_owner, exists := ext.LocksHeld[node]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a lockable we don't own")
  }
  delete(ext.LocksHeld, node)
  return last_owner
}

func (ext *LockableExt) RecordLock(node NodeID, last_owner *NodeID) {
  _, exists := ext.LocksHeld[node]
  if exists == true {
    panic("Attempted to lock a lockable we're already holding(lock cycle)")
  }
  ext.LocksHeld[node] = last_owner
}

func SaveNode(node *Node) string {
  str := ""
  if node != nil {
    str = node.ID.String()
  }
  return str
}

func RestoreNode(ctx *Context, id_str string) (*Node, error) {
  if id_str == "" {
    return nil, nil
  }
  id, err := ParseID(id_str)
  if err != nil {
    return nil, err
  }

  return LoadNode(ctx, id)
}

func SaveNodeMap(nodes NodeMap) map[string]string {
  m := map[string]string{}
  for id, node := range(nodes) {
    m[id.String()] = SaveNode(node)
  }
  return m
}

func RestoreNodeMap(ctx *Context, ids map[string]string) (NodeMap, error) {
  nodes := NodeMap{}
  for id_str_1, id_str_2 := range(ids) {
    id_1, err := ParseID(id_str_1)
    if err != nil {
      return nil, err
    }

    node_1, err := LoadNode(ctx, id_1)
    if err != nil {
      return nil, err
    }


    var node_2 *Node = nil
    if id_str_2 != "" {
      id_2, err := ParseID(id_str_2)
      if err != nil {
        return nil, err
      }
      node_2, err = LoadNode(ctx, id_2)
      if err != nil {
        return nil, err
      }
    }

    nodes[node_1.ID] = node_2
  }

  return nodes, nil
}

func SaveNodeList(nodes NodeMap) []string {
  ids := make([]string, len(nodes))
  i := 0
  for id, _ := range(nodes) {
    ids[i] = id.String()
    i += 1
  }

  return ids
}

func RestoreNodeList(ctx *Context, ids []string) (NodeMap, error) {
  nodes := NodeMap{}

  for _, id_str := range(ids) {
    node, err := RestoreNode(ctx, id_str)
    if err != nil {
      return nil, err
    }
    nodes[node.ID] = node
  }

  return nodes, nil
}

