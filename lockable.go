package graphvent

import (
  "fmt"
  "reflect"
  "encoding/json"
)

type Listener struct {
  Lockable
  Chan chan GraphSignal
}

func (node *Listener) Type() NodeType {
  return NodeType("listener")
}

func (node *Listener) Process(context *StateContext, signal GraphSignal) error {
  context.Graph.Log.Logf("signal", "LISTENER_PROCESS: %s", node.ID())
  select {
  case node.Chan <- signal:
  default:
    return fmt.Errorf("LISTENER_OVERFLOW: %s - %s", node.ID(), signal)
  }
  return node.Lockable.Process(context, signal)
}

const LISTENER_CHANNEL_BUFFER = 1024
func NewListener(id NodeID, name string) Listener {
  return Listener{
    Lockable: NewLockable(id, name),
    Chan: make(chan GraphSignal, LISTENER_CHANNEL_BUFFER),
  }
}

var LoadListener = LoadJSONNode(func(id NodeID, j LockableJSON) (Node, error) {
  listener := NewListener(id, j.Name)
  return &listener, nil
}, RestoreLockable)

type LockableNode interface {
  Node
  LockableHandle() *Lockable
}

// Lockable is a simple Lockable implementation that can be embedded into more complex structures
type Lockable struct {
  SimpleNode
  Name string
  Owner LockableNode
  Requirements map[NodeID]LockableNode
  Dependencies map[NodeID]LockableNode
  LocksHeld map[NodeID]LockableNode
}

func (lockable *Lockable) LockableHandle() *Lockable {
  return lockable
}

func (lockable *Lockable) Type() NodeType {
  return NodeType("lockable")
}

type LockableJSON struct {
  SimpleNodeJSON
  Name string `json:"name"`
  Owner string `json:"owner"`
  Dependencies []string `json:"dependencies"`
  Requirements []string `json:"requirements"`
  LocksHeld map[string]string `json:"locks_held"`
}

func (lockable *Lockable) Serialize() ([]byte, error) {
  lockable_json := NewLockableJSON(lockable)
  return json.MarshalIndent(&lockable_json, "", "  ")
}

func NewLockableJSON(lockable *Lockable) LockableJSON {
  requirement_ids := make([]string, len(lockable.Requirements))
  req_n := 0
  for id, _ := range(lockable.Requirements) {
    requirement_ids[req_n] = id.String()
    req_n++
  }

  dependency_ids := make([]string, len(lockable.Dependencies))
  dep_n := 0
  for id, _ := range(lockable.Dependencies) {
    dependency_ids[dep_n] = id.String()
    dep_n++
  }

  owner_id := ""
  if lockable.Owner != nil {
    owner_id = lockable.Owner.ID().String()
  }

  locks_held := map[string]string{}
  for lockable_id, node := range(lockable.LocksHeld) {
    if node == nil {
      locks_held[lockable_id.String()] = ""
    } else {
      locks_held[lockable_id.String()] = node.ID().String()
    }
  }

  node_json := NewSimpleNodeJSON(&lockable.SimpleNode)

  return LockableJSON{
    SimpleNodeJSON: node_json,
    Name: lockable.Name,
    Owner: owner_id,
    Dependencies: dependency_ids,
    Requirements: requirement_ids,
    LocksHeld: locks_held,
  }
}

func (lockable *Lockable) RecordUnlock(l LockableNode) LockableNode {
  lockable_id := l.ID()
  last_owner, exists := lockable.LocksHeld[lockable_id]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a lockable we don't own")
  }
  delete(lockable.LocksHeld, lockable_id)
  return last_owner
}

func (lockable *Lockable) RecordLock(l LockableNode, last_owner LockableNode) {
  lockable_id := l.ID()
  _, exists := lockable.LocksHeld[lockable_id]
  if exists == true {
    panic("Attempted to lock a lockable we're already holding(lock cycle)")
  }

  lockable.LocksHeld[lockable_id] = last_owner
}

// Assumed that lockable is already locked for signal
func (lockable *Lockable) Process(context *StateContext, signal GraphSignal) error {
  context.Graph.Log.Logf("signal", "LOCKABLE_PROCESS: %s", lockable.ID())

  var err error
  switch signal.Direction() {
  case Up:
    err = UseStates(context, lockable,
      NewLockInfo(lockable, []string{"dependencies", "owner"}), func(context *StateContext) error {
      owner_sent := false
      for _, dependency := range(lockable.Dependencies) {
        context.Graph.Log.Logf("signal", "SENDING_TO_DEPENDENCY: %s -> %s", lockable.ID(), dependency.ID())
        Signal(context, dependency, lockable, signal)
        if lockable.Owner != nil {
          if dependency.ID() == lockable.Owner.ID() {
            owner_sent = true
          }
        }
      }
      if lockable.Owner != nil && owner_sent == false {
        if lockable.Owner.ID() != lockable.ID() {
          context.Graph.Log.Logf("signal", "SENDING_TO_OWNER: %s -> %s", lockable.ID(), lockable.Owner.ID())
          return Signal(context, lockable.Owner, lockable, signal)
        }
      }
      return nil
    })
  case Down:
    err = UseStates(context, lockable, NewLockInfo(lockable, []string{"requirements"}), func(context *StateContext) error {
      for _, requirement := range(lockable.Requirements) {
        err := Signal(context, requirement, lockable, signal)
        if err != nil {
          return err
        }
      }
      return nil
    })
  case Direct:
    err = nil
  default:
    return fmt.Errorf("invalid signal direction %d", signal.Direction())
  }
  if err != nil {
    return err
  }
  return lockable.SimpleNode.Process(context, signal)
}

// Removes requirement as a requirement from lockable
// Continues the write context with princ, getting requirents for lockable and dependencies for requirement
// Assumes that an active write context exists with princ locked so that princ's state can be used in checks
func UnlinkLockables(context *StateContext, princ Node, lockable LockableNode, requirement LockableNode) error {
  return UpdateStates(context, princ, LockMap{
    lockable.ID(): LockInfo{Node: lockable, Resources: []string{"requirements"}},
    requirement.ID(): LockInfo{Node: requirement, Resources: []string{"dependencies"}},
  }, func(context *StateContext) error {
    var found Node = nil
    for _, req := range(lockable.LockableHandle().Requirements) {
      if requirement.ID() == req.ID() {
        found = req
        break
      }
    }

    if found == nil {
      return fmt.Errorf("UNLINK_LOCKABLES_ERR: %s is not a requirement of %s", requirement.ID(), lockable.ID())
    }

    delete(requirement.LockableHandle().Dependencies, lockable.ID())
    delete(lockable.LockableHandle().Requirements, requirement.ID())

    return nil
  })
}

// Link requirements as requirements to lockable
// Continues the wrtie context with princ, getting requirements for lockable and dependencies for requirements
func LinkLockables(context *StateContext, princ Node, lockable_node LockableNode, requirements []LockableNode) error {
  if lockable_node == nil {
    return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link Lockables to nil as requirements")
  }
  lockable := lockable_node.LockableHandle()

  if len(requirements) == 0 {
    return nil
  }

  found := map[NodeID]bool{}
  for _, requirement := range(requirements) {
    if requirement == nil {
      return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link nil to a Lockable as a requirement")
    }

    if lockable.ID() == requirement.ID() {
      return fmt.Errorf("LOCKABLE_LINK_ERR: cannot link %s to itself", lockable.ID())
    }

    _, exists := found[requirement.ID()]
    if exists == true {
      return fmt.Errorf("LOCKABLE_LINK_ERR: cannot link %s twice", requirement.ID())
    }
    found[requirement.ID()] = true
  }

  return UpdateStates(context, princ, NewLockMap(
    NewLockInfo(lockable_node, []string{"requirements"}),
    LockList(requirements, []string{"dependencies"}),
  ), func(context *StateContext) error {
    // Check that all the requirements can be added
    // If the lockable is already locked, need to lock this resource as well before we can add it
    for _, requirement_node := range(requirements) {
      requirement := requirement_node.LockableHandle()
      for _, req_node := range(requirements) {
        req := req_node.LockableHandle()
        if req.ID() == requirement.ID() {
          continue
        }
        if checkIfRequirement(context, req, requirement) == true {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependenyc of %s so cannot add the same dependency", req.ID(), requirement.ID())
        }
      }
      if checkIfRequirement(context, lockable, requirement) == true {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as requirement", requirement.ID(), lockable.ID())
      }

      if checkIfRequirement(context, requirement, lockable) == true {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as dependency again", lockable.ID(), requirement.ID())
      }
      if lockable.Owner == nil {
        // If the new owner isn't locked, we can add the requirement
      } else if requirement.Owner == nil {
        // if the new requirement isn't already locked but the owner is, the requirement needs to be locked first
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is locked, %s must be locked to add", lockable.ID(), requirement.ID())
      } else {
        // If the new requirement is already locked and the owner is already locked, their owners need to match
        if requirement.Owner.ID() != lockable.Owner.ID() {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is not locked by the same owner as %s, can't link as requirement", requirement.ID(), lockable.ID())
        }
      }
    }
    // Update the states of the requirements
    for _, requirement_node := range(requirements) {
      requirement := requirement_node.LockableHandle()
      requirement.Dependencies[lockable.ID()] = lockable_node
      lockable.Requirements[lockable.ID()] = requirement_node
      context.Graph.Log.Logf("lockable", "LOCKABLE_LINK: linked %s to %s as a requirement", requirement.ID(), lockable.ID())
    }

    // Return no error
    return nil
  })
}

// Must be called withing update context
func checkIfRequirement(context *StateContext, r LockableNode, cur LockableNode) bool {
  for _, c := range(cur.LockableHandle().Requirements) {
    if c.ID() == r.ID() {
      return true
    }
    is_requirement := false
    UpdateStates(context, cur, NewLockMap(NewLockInfo(c, []string{"requirements"})), func(context *StateContext) error {
      is_requirement = checkIfRequirement(context, cur, c)
      return nil
    })

    if is_requirement {
      return true
    }
  }

  return false
}

// Lock nodes in the to_lock slice with new_owner, does not modify any states if returning an error
// Assumes that new_owner will be written to after returning, even though it doesn't get locked during the call
func LockLockables(context *StateContext, to_lock map[NodeID]LockableNode, new_owner_node LockableNode) error {
  if to_lock == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: no list provided")
  }

  for _, l := range(to_lock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_LOCK_ERR: Can not lock nil")
    }
  }

  if new_owner_node == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: nil cannot hold locks")
  }

  new_owner := new_owner_node.LockableHandle()

  // Called with no requirements to lock, success
  if len(to_lock) == 0 {
    return nil
  }

  return UpdateStates(context, new_owner, NewLockMap(
    LockListM(to_lock, []string{"lock"}),
    NewLockInfo(new_owner, nil),
  ), func(context *StateContext) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req_node := range(to_lock) {
      req := req_node.LockableHandle()
      context.Graph.Log.Logf("lockable", "LOCKABLE_LOCKING: %s from %s", req.ID(), new_owner.ID())

      // If req is alreay locked, check that we can pass the lock
      if req.Owner != nil {
        owner := req.Owner
        if owner.ID() == new_owner.ID() {
          continue
        } else {
          err := UpdateStates(context, new_owner, NewLockInfo(owner, []string{"take_lock"}), func(context *StateContext)(error){
            return LockLockables(context, req.Requirements, req)
          })
          if err != nil {
            return err
          }
        }
      } else {
        err :=  LockLockables(context, req.Requirements, req)
        if err != nil {
          return err
        }
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req_node := range(to_lock) {
      req := req_node.LockableHandle()
      old_owner := req.Owner
      // If the lockable was previously unowned, update the state
      if old_owner == nil {
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s locked %s", new_owner.ID(), req.ID())
        req.Owner = new_owner_node
        new_owner.RecordLock(req, old_owner)
      // Otherwise if the new owner already owns it, no need to update state
      } else if old_owner.ID() == new_owner.ID() {
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s already owns %s", new_owner.ID(), req.ID())
      // Otherwise update the state
      } else {
        req.Owner = new_owner
        new_owner.RecordLock(req, old_owner)
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s took lock of %s from %s", new_owner.ID(), req.ID(), old_owner.ID())
      }
    }
    return nil
  })

}

func UnlockLockables(context *StateContext, to_unlock map[NodeID]LockableNode, old_owner_node LockableNode) error {
  if to_unlock == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: no list provided")
  }

  for _, l := range(to_unlock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: Can not unlock nil")
    }
  }

  if old_owner_node == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: nil cannot hold locks")
  }

  old_owner := old_owner_node.LockableHandle()

  // Called with no requirements to unlock, success
  if len(to_unlock) == 0 {
    return nil
  }

  return UpdateStates(context, old_owner, NewLockMap(
    LockListM(to_unlock, []string{"lock"}),
    NewLockInfo(old_owner, nil),
  ), func(context *StateContext) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req_node := range(to_unlock) {
      req := req_node.LockableHandle()
      context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCKING: %s from %s", req.ID(), old_owner.ID())

      // Check if the owner is correct
      if req.Owner != nil {
        if req.Owner.ID() != old_owner.ID() {
          return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked by %s", req.ID(), old_owner.ID())
        }
      } else {
        return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked", req.ID())
      }

      err := UnlockLockables(context, req.Requirements, req)
      if err != nil {
        return err
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req_node := range(to_unlock) {
      req := req_node.LockableHandle()
      new_owner := old_owner.RecordUnlock(req)
      req.Owner = new_owner
      if new_owner == nil {
        context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s unlocked %s", old_owner.ID(), req.ID())
      } else {
        context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s passed lock of %s back to %s", old_owner.ID(), req.ID(), new_owner.ID())
      }
    }

    return nil
  })
}

var LoadLockable = LoadJSONNode(func(id NodeID, j LockableJSON) (Node, error) {
  lockable := NewLockable(id, j.Name)
  return &lockable, nil
}, RestoreLockable)

func NewLockable(id NodeID, name string) Lockable {
  return Lockable{
    SimpleNode: NewSimpleNode(id),
    Name: name,
    Owner: nil,
    Requirements: map[NodeID]LockableNode{},
    Dependencies: map[NodeID]LockableNode{},
    LocksHeld: map[NodeID]LockableNode{},
  }
}

// Helper function to load links when loading a struct that embeds Lockable
func RestoreLockable(ctx * Context, lockable LockableNode, j LockableJSON, nodes NodeMap) error {
  lockable_ptr := lockable.LockableHandle()
  if j.Owner != "" {
    owner_id, err := ParseID(j.Owner)
    if err != nil {
      return err
    }
    owner_node, err := LoadNodeRecurse(ctx, owner_id, nodes)
    if err != nil {
      return err
    }
    owner, ok := owner_node.(LockableNode)
    if ok == false {
      return fmt.Errorf("%s is not a Lockable", j.Owner)
    }
    lockable_ptr.Owner = owner
  }

  for _, dep_str := range(j.Dependencies) {
    dep_id, err := ParseID(dep_str)
    if err != nil {
      return err
    }
    dep_node, err := LoadNodeRecurse(ctx, dep_id, nodes)
    if err != nil {
      return err
    }
    dep, ok := dep_node.(LockableNode)
    if ok == false {
      return fmt.Errorf("%+v is not a Lockable as expected", dep_node)
    }
    ctx.Log.Logf("db", "LOCKABLE_LOAD_DEPENDENCY: %s - %s - %+v", lockable.ID(), dep_id, reflect.TypeOf(dep))
    lockable_ptr.Dependencies[dep_id] = dep
  }

  for _, req_str := range(j.Requirements) {
    req_id, err := ParseID(req_str)
    if err != nil {
      return err
    }
    req_node, err := LoadNodeRecurse(ctx, req_id, nodes)
    if err != nil {
      return err
    }
    req, ok := req_node.(LockableNode)
    if ok == false {
      return fmt.Errorf("%+v is not a Lockable as expected", req_node)
    }
    lockable_ptr.Requirements[req_id] = req
  }

  for l_id_str, h_str := range(j.LocksHeld) {
    l_id, err := ParseID(l_id_str)
    l, err := LoadNodeRecurse(ctx, l_id, nodes)
    if err != nil {
      return err
    }
    l_l, ok := l.(LockableNode)
    if ok == false {
      return fmt.Errorf("%s is not a Lockable", l.ID())
    }

    var h_l LockableNode
    if h_str != "" {
      h_id, err := ParseID(h_str)
      if err != nil {
        return err
      }
      h_node, err := LoadNodeRecurse(ctx, h_id, nodes)
      if err != nil {
        return err
      }
      h, ok := h_node.(LockableNode)
      if ok == false {
        return err
      }
      h_l = h
    }
    lockable_ptr.RecordLock(l_l, h_l)
  }

  return RestoreSimpleNode(ctx, lockable, j.SimpleNodeJSON, nodes)
}
