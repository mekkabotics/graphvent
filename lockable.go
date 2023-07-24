package graphvent

import (
  "fmt"
  "encoding/json"
)

// A Lockable represents a Node that can be locked and hold other Nodes locks
type Lockable interface {
  // All Lockables are nodes
  Node
  //// State Modification Function
  // Record that lockable was returned to it's owner and is no longer held by this Node
  // Returns the previous owner of the lockable
  RecordUnlock(lockable Lockable) Lockable
  // Record that lockable was locked by this node, and that it should be returned to last_owner
  RecordLock(lockable Lockable, last_owner Lockable)
  // Link a requirement to this Node
  AddRequirement(requirement Lockable)
  // Remove a requirement linked to this Node
  RemoveRequirement(requirement Lockable)
  // Link a dependency to this Node
  AddDependency(dependency Lockable)
  // Remove a dependency linked to this Node
  RemoveDependency(dependency Lockable)
  // 
  SetOwner(new_owner Lockable)

  //// State Reading Functions
  Name() string
  // Called when new_owner wants to take lockable's lock but it's owned by this node
  // A true return value means that the lock can be passed
  AllowedToTakeLock(new_owner Lockable, lockable Lockable) bool
  // Get all the linked requirements to this node
  Requirements() []Lockable
  // Get all the linked dependencies to this node
  Dependencies() []Lockable
  // Get the node's Owner
  Owner() Lockable
  // Called during the lock process after locking the state and before updating the Node's state
  // a non-nil return value will abort the lock attempt
  CanLock(new_owner Lockable) error
  // Called during the unlock process after locking the state and before updating the Node's state
  // a non-nil return value will abort the unlock attempt
  CanUnlock(old_owner Lockable) error
}

// SimpleLockable is a simple Lockable implementation that can be embedded into more complex structures
type SimpleLockable struct {
  GraphNode
  name string
  owner Lockable
  requirements []Lockable
  dependencies []Lockable
  locks_held map[NodeID]Lockable
}

func (state * SimpleLockable) Type() NodeType {
  return NodeType("simple_lockable")
}

type SimpleLockableJSON struct {
  GraphNodeJSON
  Name string `json:"name"`
  Owner string `json:"owner"`
  Dependencies []string `json:"dependencies"`
  Requirements []string `json:"requirements"`
  LocksHeld map[string]string `json:"locks_held"`
}

func (lockable * SimpleLockable) Serialize() ([]byte, error) {
  lockable_json := NewSimpleLockableJSON(lockable)
  return json.MarshalIndent(&lockable_json, "", "  ")
}

func NewSimpleLockableJSON(lockable *SimpleLockable) SimpleLockableJSON {
  requirement_ids := make([]string, len(lockable.requirements))
  for i, requirement := range(lockable.requirements) {
    requirement_ids[i] = requirement.ID().String()
  }

  dependency_ids := make([]string, len(lockable.dependencies))
  for i, dependency := range(lockable.dependencies) {
    dependency_ids[i] = dependency.ID().String()
  }

  owner_id := ""
  if lockable.owner != nil {
    owner_id = lockable.owner.ID().String()
  }

  locks_held := map[string]string{}
  for lockable_id, node := range(lockable.locks_held) {
    if node == nil {
      locks_held[lockable_id.String()] = ""
    } else {
      locks_held[lockable_id.String()] = node.ID().String()
    }
  }

  node_json := NewGraphNodeJSON(&lockable.GraphNode)

  return SimpleLockableJSON{
    GraphNodeJSON: node_json,
    Name: lockable.name,
    Owner: owner_id,
    Dependencies: dependency_ids,
    Requirements: requirement_ids,
    LocksHeld: locks_held,
  }
}

func (lockable * SimpleLockable) Name() string {
  return lockable.name
}

func (lockable * SimpleLockable) RecordUnlock(l Lockable) Lockable {
  lockable_id := l.ID()
  last_owner, exists := lockable.locks_held[lockable_id]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a lockable we don't own")
  }
  delete(lockable.locks_held, lockable_id)
  return last_owner
}

func (lockable * SimpleLockable) RecordLock(l Lockable, last_owner Lockable) {
  lockable_id := l.ID()
  _, exists := lockable.locks_held[lockable_id]
  if exists == true {
    panic("Attempted to lock a lockable we're already holding(lock cycle)")
  }

  lockable.locks_held[lockable_id] = last_owner
}

// Nothing can take a lock from a simple lockable
func (lockable * SimpleLockable) AllowedToTakeLock(l Lockable, new_owner Lockable) bool {
  return false
}

func (lockable * SimpleLockable) Owner() Lockable {
  return lockable.owner
}

func (lockable * SimpleLockable) SetOwner(owner Lockable) {
  lockable.owner = owner
}

func (lockable * SimpleLockable) Requirements() []Lockable {
  return lockable.requirements
}

func (lockable * SimpleLockable) AddRequirement(requirement Lockable) {
  if requirement == nil {
    panic("Will not connect nil to the DAG")
  }
  lockable.requirements = append(lockable.requirements, requirement)
}

func (lockable * SimpleLockable) Dependencies() []Lockable {
  return lockable.dependencies
}

func (lockable * SimpleLockable) AddDependency(dependency Lockable) {
  if dependency == nil {
    panic("Will not connect nil to the DAG")
  }

  lockable.dependencies = append(lockable.dependencies, dependency)
}

func (lockable * SimpleLockable) RemoveDependency(dependency Lockable) {
  idx := -1

  for i, dep := range(lockable.dependencies) {
    if dep.ID() == dependency.ID() {
      idx = i
      break
    }
  }

  if idx == -1 {
    panic(fmt.Sprintf("%s is not a dependency of %s", dependency.ID(), lockable.Name()))
  }

  dep_len := len(lockable.dependencies)
  lockable.dependencies[idx] = lockable.dependencies[dep_len-1]
  lockable.dependencies = lockable.dependencies[0:(dep_len-1)]
}

func (lockable * SimpleLockable) RemoveRequirement(requirement Lockable) {
  idx := -1
  for i, req := range(lockable.requirements) {
    if req.ID() == requirement.ID() {
      idx = i
      break
    }
  }

  if idx == -1 {
    panic(fmt.Sprintf("%s is not a requirement of %s", requirement.ID(), lockable.Name()))
  }

  req_len := len(lockable.requirements)
  lockable.requirements[idx] = lockable.requirements[req_len-1]
  lockable.requirements = lockable.requirements[0:(req_len-1)]
}

func (lockable * SimpleLockable) CanLock(new_owner Lockable) error {
  return nil
}

func (lockable * SimpleLockable) CanUnlock(new_owner Lockable) error {
  return nil
}

// Assumed that lockable is already locked for signal
func (lockable * SimpleLockable) Signal(context *StateContext, princ Node, signal GraphSignal) error {
  err := lockable.GraphNode.Signal(context, princ, signal)
  if err != nil {
    return err
  }

  switch signal.Direction() {
  case Up:
    err = UseStates(context, lockable,
      NewLockInfo(lockable, []string{"dependencies", "owner"}), func(context *StateContext) error {
      owner_sent := false
      for _, dependency := range(lockable.dependencies) {
        context.Graph.Log.Logf("signal", "SENDING_TO_DEPENDENCY: %s -> %s", lockable.ID(), dependency.ID())
        dependency.Signal(context, lockable, signal)
        if lockable.owner != nil {
          if dependency.ID() == lockable.owner.ID() {
            owner_sent = true
          }
        }
      }
      if lockable.owner != nil && owner_sent == false {
        if lockable.owner.ID() != lockable.ID() {
          context.Graph.Log.Logf("signal", "SENDING_TO_OWNER: %s -> %s", lockable.ID(), lockable.owner.ID())
          return lockable.owner.Signal(context, lockable, signal)
        }
      }
      return nil
    })
  case Down:
    err = UseStates(context, lockable, NewLockInfo(lockable, []string{"requirements"}), func(context *StateContext) error {
      for _, requirement := range(lockable.requirements) {
        err := requirement.Signal(context, lockable, signal)
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
  return err
}

// Removes requirement as a requirement from lockable
// Continues the write context with princ, getting requirents for lockable and dependencies for requirement
// Assumes that an active write context exists with princ locked so that princ's state can be used in checks
func UnlinkLockables(context *StateContext, princ Node, lockable Lockable, requirement Lockable) error {
  return UpdateStates(context, princ, LockMap{
    lockable.ID(): LockInfo{Node: lockable, Resources: []string{"requirements"}},
    requirement.ID(): LockInfo{Node: requirement, Resources: []string{"dependencies"}},
  }, func(context *StateContext) error {
    var found Node = nil
    for _, req := range(lockable.Requirements()) {
      if requirement.ID() == req.ID() {
        found = req
        break
      }
    }

    if found == nil {
      return fmt.Errorf("UNLINK_LOCKABLES_ERR: %s is not a requirement of %s", requirement.ID(), lockable.ID())
    }

    requirement.RemoveDependency(lockable)
    lockable.RemoveRequirement(requirement)

    return nil
  })
}

// Link requirements as requirements to lockable
// Continues the wrtie context with princ, getting requirements for lockable and dependencies for requirements
func LinkLockables(context *StateContext, princ Node, lockable Lockable, requirements []Lockable) error {
  if lockable == nil {
    return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link Lockables to nil as requirements")
  }

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
    NewLockInfo(lockable, []string{"requirements"}),
    LockList(requirements, []string{"dependencies"}),
  ), func(context *StateContext) error {
    // Check that all the requirements can be added
    // If the lockable is already locked, need to lock this resource as well before we can add it
    for _, requirement := range(requirements) {
      for _, req := range(requirements) {
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
      if lockable.Owner() == nil {
        // If the new owner isn't locked, we can add the requirement
      } else if requirement.Owner() == nil {
        // if the new requirement isn't already locked but the owner is, the requirement needs to be locked first
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is locked, %s must be locked to add", lockable.ID(), requirement.ID())
      } else {
        // If the new requirement is already locked and the owner is already locked, their owners need to match
        if requirement.Owner().ID() != lockable.Owner().ID() {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is not locked by the same owner as %s, can't link as requirement", requirement.ID(), lockable.ID())
        }
      }
    }
    // Update the states of the requirements
    for _, requirement := range(requirements) {
      requirement.AddDependency(lockable)
      lockable.AddRequirement(requirement)
      context.Graph.Log.Logf("lockable", "LOCKABLE_LINK: linked %s to %s as a requirement", requirement.ID(), lockable.ID())
    }

    // Return no error
    return nil
  })
}

// Must be called withing update context
func checkIfRequirement(context *StateContext, r Lockable, cur Lockable) bool {
  for _, c := range(cur.Requirements()) {
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
func LockLockables(context *StateContext, to_lock []Lockable, new_owner Lockable) error {
  if to_lock == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: no list provided")
  }

  for _, l := range(to_lock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_LOCK_ERR: Can not lock nil")
    }
  }

  if new_owner == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: nil cannot hold locks")
  }

  // Called with no requirements to lock, success
  if len(to_lock) == 0 {
    return nil
  }

  return UpdateStates(context, new_owner, NewLockMap(
    LockList(to_lock, []string{"lock"}),
    NewLockInfo(new_owner, nil),
  ), func(context *StateContext) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_lock) {
      context.Graph.Log.Logf("lockable", "LOCKABLE_LOCKING: %s from %s", req.ID(), new_owner.ID())

      // Check custom lock conditions
      err := req.CanLock(new_owner)
      if err != nil {
        return err
      }

      // If req is alreay locked, check that we can pass the lock
      if req.Owner() != nil {
        owner := req.Owner()
        if owner.ID() == new_owner.ID() {
          continue
        } else {
          err := UpdateStates(context, new_owner, NewLockMap(
            NewLockInfo(owner, []string{"take_lock"})
          ), func(context *StateContext)(error){
            return LockLockables(context, req.Requirements(), req)
          })
          if err != nil {
            return err
          }
        }
      } else {
        err :=  LockLockables(context, req.Requirements(), req)
        if err != nil {
          return err
        }
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_lock) {
      old_owner := req.Owner()
      // If the lockable was previously unowned, update the state
      if old_owner == nil {
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s locked %s", new_owner.ID(), req.ID())
        req.SetOwner(new_owner)
        new_owner.RecordLock(req, old_owner)
      // Otherwise if the new owner already owns it, no need to update state
      } else if old_owner.ID() == new_owner.ID() {
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s already owns %s", new_owner.ID(), req.ID())
      // Otherwise update the state
      } else {
        req.SetOwner(new_owner)
        new_owner.RecordLock(req, old_owner)
        context.Graph.Log.Logf("lockable", "LOCKABLE_LOCK: %s took lock of %s from %s", new_owner.ID(), req.ID(), old_owner.ID())
      }
    }
    return nil
  })

}

func UnlockLockables(context *StateContext, to_unlock []Lockable, old_owner Lockable) error {
  if to_unlock == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: no list provided")
  }

  for _, l := range(to_unlock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: Can not unlock nil")
    }
  }

  if old_owner == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: nil cannot hold locks")
  }

  // Called with no requirements to unlock, success
  if len(to_unlock) == 0 {
    return nil
  }

  return UpdateStates(context, old_owner, NewLockMap(
    LockList(to_unlock, []string{"lock"}),
    NewLockInfo(old_owner, nil),
  ), func(context *StateContext) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_unlock) {
      context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCKING: %s from %s", req.ID(), old_owner.ID())

      // Check if the owner is correct
      if req.Owner() != nil {
        if req.Owner().ID() != old_owner.ID() {
          return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked by %s", req.ID(), old_owner.ID())
        }
      } else {
        return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked", req.ID())
      }

      // Check custom unlock conditions
      err := req.CanUnlock(old_owner)
      if err != nil {
        return err
      }

      err = UnlockLockables(context, req.Requirements(), req)
      if err != nil {
        return err
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_unlock) {
      new_owner := old_owner.RecordUnlock(req)
      req.SetOwner(new_owner)
      if new_owner == nil {
        context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s unlocked %s", old_owner.ID(), req.ID())
      } else {
        context.Graph.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s passed lock of %s back to %s", old_owner.ID(), req.ID(), new_owner.ID())
      }
    }

    return nil
  })
}

// Load function for SimpleLockable
func LoadSimpleLockable(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j SimpleLockableJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  lockable := NewSimpleLockable(id, j.Name)
  nodes[id] = &lockable

  err = RestoreSimpleLockable(ctx, &lockable, j, nodes)
  if err != nil {
    return nil, err
  }

  return &lockable, nil
}

func NewSimpleLockable(id NodeID, name string) SimpleLockable {
  return SimpleLockable{
    GraphNode: NewGraphNode(id),
    name: name,
    owner: nil,
    requirements: []Lockable{},
    dependencies: []Lockable{},
    locks_held: map[NodeID]Lockable{},
  }
}

// Helper function to load links when loading a struct that embeds SimpleLockable
func RestoreSimpleLockable(ctx * Context, lockable Lockable, j SimpleLockableJSON, nodes NodeMap) error {
  if j.Owner != "" {
    owner_id, err := ParseID(j.Owner)
    if err != nil {
      return err
    }
    owner_node, err := LoadNodeRecurse(ctx, owner_id, nodes)
    if err != nil {
      return err
    }
    owner, ok := owner_node.(Lockable)
    if ok == false {
      return fmt.Errorf("%s is not a Lockable", j.Owner)
    }
    lockable.SetOwner(owner)
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
    dep, ok := dep_node.(Lockable)
    if ok == false {
      return fmt.Errorf("%+v is not a Lockable as expected", dep_node)
    }
    lockable.AddDependency(dep)
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
    req, ok := req_node.(Lockable)
    if ok == false {
      return fmt.Errorf("%+v is not a Lockable as expected", req_node)
    }
    lockable.AddRequirement(req)
  }

  for l_id_str, h_str := range(j.LocksHeld) {
    l_id, err := ParseID(l_id_str)
    l, err := LoadNodeRecurse(ctx, l_id, nodes)
    if err != nil {
      return err
    }
    l_l, ok := l.(Lockable)
    if ok == false {
      return fmt.Errorf("%s is not a Lockable", l.ID())
    }

    var h_l Lockable = nil
    if h_str != "" {
      h_id, err := ParseID(h_str)
      if err != nil {
        return err
      }
      h_node, err := LoadNodeRecurse(ctx, h_id, nodes)
      if err != nil {
        return err
      }
      h, ok := h_node.(Lockable)
      if ok == false {
        return err
      }
      h_l = h
    }
    lockable.RecordLock(l_l, h_l)
  }

  return RestoreGraphNode(ctx, lockable, j.GraphNodeJSON, nodes)
}
