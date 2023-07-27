package graphvent

import (
  "encoding/json"
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

func (ext *ListenerExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext.Buffer, "", "  ")
}

type LockableExt struct {
  Owner *NodeID `json:"owner"`
  Requirements []NodeID `json:"requirements"`
  Dependencies []NodeID `json:"dependencies"`
  LocksHeld map[NodeID]*NodeID `json:"locks_held"`
}

const LockableExtType = ExtType("LOCKABLE")
func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

func (ext *LockableExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext, "", "  ")
}

func NewLockableExt(owner *NodeID, requirements []NodeID, dependencies []NodeID, locks_held map[NodeID]*NodeID) *LockableExt {
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

func LoadLockableExt(ctx *Context, data []byte) (Extension, error) {
  var ext LockableExt
  err := json.Unmarshal(data, &ext)
  if err != nil {
    return nil, err
  }

  ctx.Log.Logf("db", "DB_LOADING_LOCKABLE_EXT_JSON: %+v", ext)

  return &ext, nil
}



func (ext *LockableExt) HandleLinkSignal(ctx *Context, source NodeID, node *Node, signal LinkSignal) {
  ctx.Log.Logf("lockable", "LINK_SIGNAL: %+v", signal)
}

func (ext *LockableExt) Process(ctx *Context, source NodeID, node *Node, signal Signal) {
  ctx.Log.Logf("signal", "LOCKABLE_PROCESS: %s", node.ID)

  switch signal.Direction() {
  case Up:
    owner_sent := false
    for _, dependency := range(ext.Dependencies) {
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
    for _, requirement := range(ext.Requirements) {
      err := ctx.Send(node.ID, requirement, signal)
      if err != nil {
        ctx.Log.Logf("signal", "LOCKABLE_SIGNAL_ERR: %s->%s - %e", node.ID, requirement, err)
      }
    }
  case Direct:
    switch sig := signal.(type) {
    case LinkSignal:
      ext.HandleLinkSignal(ctx, source, node, sig)
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

