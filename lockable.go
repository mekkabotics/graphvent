package graphvent

import (
  "fmt"
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

func (ext *ListenerExt) Process(context *StateContext, node *Node, signal Signal) error {
  context.Graph.Log.Logf("signal", "LISTENER_PROCESS: %s - %+v", node.ID, signal)
  select {
  case ext.Chan <- signal:
  default:
    return fmt.Errorf("LISTENER_OVERFLOW - %+v", signal)
  }
  return nil
}

func (ext *ListenerExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext.Buffer, "", "  ")
}

type LockableExt struct {
  Owner *Node
  Requirements map[NodeID]*Node
  Dependencies map[NodeID]*Node
  LocksHeld map[NodeID]*Node
}

const LockableExtType = ExtType("LOCKABLE")
func (ext *LockableExt) Type() ExtType {
  return LockableExtType
}

type LockableExtJSON struct {
  Owner string `json:"owner"`
  Requirements []string `json:"requirements"`
  Dependencies []string `json:"dependencies"`
  LocksHeld map[string]string `json:"locks_held"`
}

func (ext *LockableExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(&LockableExtJSON{
    Owner: SaveNode(ext.Owner),
    Requirements: SaveNodeList(ext.Requirements),
    Dependencies: SaveNodeList(ext.Dependencies),
    LocksHeld: SaveNodeMap(ext.LocksHeld),
  }, "", "  ")
}

func NewLockableExt(owner *Node, requirements NodeMap, dependencies NodeMap, locks_held NodeMap) *LockableExt {
  if requirements == nil {
    requirements = NodeMap{}
  }

  if dependencies == nil {
    dependencies = NodeMap{}
  }

  if locks_held == nil {
    locks_held = NodeMap{}
  }

  return &LockableExt{
    Owner: owner,
    Requirements: requirements,
    Dependencies: dependencies,
    LocksHeld: locks_held,
  }
}

func LoadLockableExt(ctx *Context, data []byte) (Extension, error) {
  var j LockableExtJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  ctx.Log.Logf("db", "DB_LOADING_LOCKABLE_EXT_JSON: %+v", j)

  owner, err := RestoreNode(ctx, j.Owner)
  if err != nil {
    return nil, err
  }

  requirements, err := RestoreNodeList(ctx, j.Requirements)
  if err != nil {
    return nil, err
  }

  dependencies, err := RestoreNodeList(ctx, j.Dependencies)
  if err != nil {
    return nil, err
  }

  locks_held, err := RestoreNodeMap(ctx, j.LocksHeld)
  if err != nil {
    return nil, err
  }


  return NewLockableExt(owner, requirements, dependencies, locks_held), nil
}

func (ext *LockableExt) Process(context *StateContext, node *Node, signal Signal) error {
  context.Graph.Log.Logf("signal", "LOCKABLE_PROCESS: %s", node.ID)

  var err error
  switch signal.Direction() {
  case Up:
    err = UseStates(context, node,
      NewACLInfo(node, []string{"dependencies", "owner"}), func(context *StateContext) error {
      owner_sent := false
      for _, dependency := range(ext.Dependencies) {
        context.Graph.Log.Logf("signal", "SENDING_TO_DEPENDENCY: %s -> %s", node.ID, dependency.ID)
        dependency.Process(context, node.ID, signal)
        if ext.Owner != nil {
          if dependency.ID == ext.Owner.ID {
            owner_sent = true
          }
        }
      }
      if ext.Owner != nil && owner_sent == false {
        if ext.Owner.ID != node.ID {
          context.Graph.Log.Logf("signal", "SENDING_TO_OWNER: %s -> %s", node.ID, ext.Owner.ID)
          return ext.Owner.Process(context, node.ID, signal)
        }
      }
      return nil
    })
  case Down:
    err = UseStates(context, node, NewACLInfo(node, []string{"requirements"}), func(context *StateContext) error {
      for _, requirement := range(ext.Requirements) {
        err := requirement.Process(context, node.ID, signal)
        if err != nil {
          return err
        }
      }
      return nil
    })
  case Direct:
    err = nil
  default:
    err = fmt.Errorf("invalid signal direction %d", signal.Direction())
  }
  if err != nil {
    return err
  }
  return nil
}

func (ext *LockableExt) RecordUnlock(node *Node) *Node {
  last_owner, exists := ext.LocksHeld[node.ID]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a lockable we don't own")
  }
  delete(ext.LocksHeld, node.ID)
  return last_owner
}

func (ext *LockableExt) RecordLock(node *Node, last_owner *Node) {
  _, exists := ext.LocksHeld[node.ID]
  if exists == true {
    panic("Attempted to lock a lockable we're already holding(lock cycle)")
  }
  ext.LocksHeld[node.ID] = last_owner
}

// Removes requirement as a requirement from lockable
func UnlinkLockables(context *StateContext, princ *Node, lockable *Node, requirement *Node) error {
  lockable_ext, err := GetExt[*LockableExt](lockable)
  if err != nil {
    return err
  }
  requirement_ext, err := GetExt[*LockableExt](requirement)
  if err != nil {
    return err
  }
  return UpdateStates(context, princ, ACLMap{
    lockable.ID: ACLInfo{Node: lockable, Resources: []string{"requirements"}},
    requirement.ID: ACLInfo{Node: requirement, Resources: []string{"dependencies"}},
  }, func(context *StateContext) error {
    var found *Node = nil
    for _, req := range(lockable_ext.Requirements) {
      if requirement.ID == req.ID {
        found = req
        break
      }
    }

    if found == nil {
      return fmt.Errorf("UNLINK_LOCKABLES_ERR: %s is not a requirement of %s", requirement.ID, lockable.ID)
    }

    delete(requirement_ext.Dependencies, lockable.ID)
    delete(lockable_ext.Requirements, requirement.ID)

    return nil
  })
}

// Link requirements as requirements to lockable
func LinkLockables(context *StateContext, princ *Node, lockable *Node, requirements []*Node) error {
  if lockable == nil {
    return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link Lockables to nil as requirements")
  }

  if len(requirements) == 0 {
    return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link no lockables in call")
  }

  lockable_ext, err := GetExt[*LockableExt](lockable)
  if err != nil {
    return err
  }

  req_exts := map[NodeID]*LockableExt{}
  for _, requirement := range(requirements) {
    if requirement == nil {
      return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link nil to a Lockable as a requirement")
    }

    if lockable.ID == requirement.ID {
      return fmt.Errorf("LOCKABLE_LINK_ERR: cannot link %s to itself", lockable.ID)
    }

    _, exists := req_exts[requirement.ID]
    if exists == true {
      return fmt.Errorf("LOCKABLE_LINK_ERR: cannot link %s twice", requirement.ID)
    }
    ext, err := GetExt[*LockableExt](requirement)
    if err != nil {
      return err
    }
    req_exts[requirement.ID] = ext
  }

  return UpdateStates(context, princ, NewACLMap(
    NewACLInfo(lockable, []string{"requirements"}),
    ACLList(requirements, []string{"dependencies"}),
  ), func(context *StateContext) error {
    // Check that all the requirements can be added
    // If the lockable is already locked, need to lock this resource as well before we can add it
    for _, requirement := range(requirements) {
      requirement_ext := req_exts[requirement.ID]
      for _, req := range(requirements) {
        if req.ID == requirement.ID {
          continue
        }

        is_req, err := checkIfRequirement(context, req.ID, requirement_ext)
        if err != nil {
          return err
        } else if is_req {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot add the same dependency", req.ID, requirement.ID)

        }
      }

      is_req, err := checkIfRequirement(context, lockable.ID, requirement_ext)
      if err != nil {
        return err
      } else if is_req {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as requirement", requirement.ID, lockable.ID)
      }

      is_req, err = checkIfRequirement(context, requirement.ID, lockable_ext)
      if err != nil {
        return err
      } else if is_req {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as dependency again", lockable.ID, requirement.ID)
      }

      if lockable_ext.Owner == nil {
        // If the new owner isn't locked, we can add the requirement
      } else if requirement_ext.Owner == nil {
        // if the new requirement isn't already locked but the owner is, the requirement needs to be locked first
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is locked, %s must be locked to add", lockable.ID, requirement.ID)
      } else {
        // If the new requirement is already locked and the owner is already locked, their owners need to match
        if requirement_ext.Owner.ID != lockable_ext.Owner.ID {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is not locked by the same owner as %s, can't link as requirement", requirement.ID, lockable.ID)
        }
      }
    }
    // Update the states of the requirements
    for _, requirement := range(requirements) {
      requirement_ext := req_exts[requirement.ID]
      requirement_ext.Dependencies[lockable.ID] = lockable
      lockable_ext.Requirements[lockable.ID] = requirement
      context.Graph.Log.Logf("lockable", "LOCKABLE_LINK: linked %s to %s as a requirement", requirement.ID, lockable.ID)
    }

    // Return no error
    return nil
  })
}

func checkIfRequirement(context *StateContext, id NodeID, cur *LockableExt) (bool, error) {
  for _, req := range(cur.Requirements) {
    if req.ID == id {
      return true, nil
    }

    req_ext, err := GetExt[*LockableExt](req)
    if err != nil {
      return false, err
    }

    var is_req bool
    err = UpdateStates(context, req, NewACLInfo(req, []string{"requirements"}), func(context *StateContext) error {
      is_req, err = checkIfRequirement(context, id, req_ext)
      return err
    })
    if err != nil {
      return false, err
    }
    if is_req == true {
      return true, nil
    }
  }

  return false, nil
}

// Lock nodes in the to_lock slice with new_owner, does not modify any states if returning an error
// Assumes that new_owner will be written to after returning, even though it doesn't get locked during the call
func LockLockables(context *StateContext, to_lock NodeMap, new_owner *Node) error {
  if to_lock == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: no map provided")
  }

  req_exts := map[NodeID]*LockableExt{}
  for _, l := range(to_lock) {
    var err error
    if l == nil {
      return fmt.Errorf("LOCKABLE_LOCK_ERR: Can not lock nil")
    }

    req_exts[l.ID], err = GetExt[*LockableExt](l)
    if err != nil {
      return err
    }
  }

  if new_owner == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: nil cannot hold locks")
  }

  new_owner_ext, err := GetExt[*LockableExt](new_owner)
  if err != nil {
    return err
  }

  // Called with no requirements to lock, success
  if len(to_lock) == 0 {
    return nil
  }

  return UpdateStates(context, new_owner, NewACLMap(
    ACLListM(to_lock, []string{"lock"}),
    NewACLInfo(new_owner, nil),
  ), func(context *StateContext) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_lock) {
      req_ext := req_exts[req.ID]
      context.Graph.Log.Logf("lockable", "LOCKABLE_LOCKING: %s from %s", req.ID, new_owner.ID)

      // If req is alreay locked, check that we can pass the lock
      if req_ext.Owner != nil {
        owner := req_ext.Owner
        if owner.ID == new_owner.ID {
          continue
        } else {
          err := UpdateStates(context, new_owner, NewACLInfo(owner, []string{"take_lock"}), func(context *StateContext)(error){
            return LockLockables(context, req_ext.Requirements, req)
          })
          if err != nil {
            return err
          }
        }
      } else {
        err :=  LockLockables(context, req_ext.Requirements, req)
        if err != nil {
          return err
        }
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_lock) {
      req_ext := req_exts[req.ID]
      old_owner := req_ext.Owner
      // If the lockable was previously unowned, update the state
      if old_owner == nil {
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s locked %s", new_owner.ID, req.ID)
        req_ext.Owner = new_owner
        new_owner_ext.RecordLock(req, old_owner)
      // Otherwise if the new owner already owns it, no need to update state
      } else if old_owner.ID == new_owner.ID {
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s already owns %s", new_owner.ID, req.ID)
      // Otherwise update the state
      } else {
        req_ext.Owner = new_owner
        new_owner_ext.RecordLock(req, old_owner)
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s took lock of %s from %s", new_owner.ID, req.ID, old_owner.ID)
      }
    }
    return nil
  })

}

func UnlockLockables(context *StateContext, to_unlock NodeMap, old_owner *Node) error {
  if to_unlock == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: no list provided")
  }

  req_exts := map[NodeID]*LockableExt{}
  for _, l := range(to_unlock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: Can not unlock nil")
    }

    var err error
    req_exts[l.ID], err = GetExt[*LockableExt](l)
    if err != nil {
      return err
    }
  }

  if old_owner == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: nil cannot hold locks")
  }

  old_owner_ext, err := GetExt[*LockableExt](old_owner)
  if err != nil {
    return err
  }


  // Called with no requirements to unlock, success
  if len(to_unlock) == 0 {
    return nil
  }

  return UpdateStates(context, old_owner, NewACLMap(
    ACLListM(to_unlock, []string{"lock"}),
    NewACLInfo(old_owner, nil),
  ), func(context *StateContext) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_unlock) {
      req_ext := req_exts[req.ID]
      context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCKING: %s from %s", req.ID, old_owner.ID)

      // Check if the owner is correct
      if req_ext.Owner != nil {
        if req_ext.Owner.ID != old_owner.ID {
          return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked by %s", req.ID, old_owner.ID)
        }
      } else {
        return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked", req.ID)
      }

      err := UnlockLockables(context, req_ext.Requirements, req)
      if err != nil {
        return err
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_unlock) {
      req_ext := req_exts[req.ID]
      new_owner := old_owner_ext.RecordUnlock(req)
      req_ext.Owner = new_owner
      if new_owner == nil {
        context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s unlocked %s", old_owner.ID, req.ID)
      } else {
        context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s passed lock of %s back to %s", old_owner.ID, req.ID, new_owner.ID)
      }
    }

    return nil
  })
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

