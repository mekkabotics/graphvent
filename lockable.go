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
  Name string `json:"name"`
  Owner *NodeID `json:"owner"`
  Dependencies []NodeID `json:"dependencies"`
  Requirements []NodeID `json:"requirements"`
  LocksHeld map[NodeID]*NodeID `json:"locks_held"`
}

func (lockable * SimpleLockable) Serialize() ([]byte, error) {
  lockable_json := NewSimpleLockableJSON(lockable)
  return json.MarshalIndent(&lockable_json, "", "  ")

}

func NewSimpleLockableJSON(lockable *SimpleLockable) SimpleLockableJSON {
  requirement_ids := make([]NodeID, len(lockable.requirements))
  for i, requirement := range(lockable.requirements) {
    requirement_ids[i] = requirement.ID()
  }

  dependency_ids := make([]NodeID, len(lockable.dependencies))
  for i, dependency := range(lockable.dependencies) {
    dependency_ids[i] = dependency.ID()
  }

  var owner_id *NodeID = nil
  if lockable.owner != nil {
    new_str := lockable.owner.ID()
    owner_id = &new_str
  }

  locks_held := map[NodeID]*NodeID{}
  for lockable_id, node := range(lockable.locks_held) {
    if node == nil {
      locks_held[lockable_id] = nil
    } else {
      str := node.ID()
      locks_held[lockable_id] = &str
    }
  }
  return SimpleLockableJSON{
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

// lockable must already be locked for read
func (lockable * SimpleLockable) Signal(ctx *Context, signal GraphSignal, nodes NodeMap) error {
  err := lockable.GraphNode.Signal(ctx, signal, nodes)
  if err != nil {
    return err
  }
  if signal.Direction() == Up {
    // Child->Parent, lockable updates dependency lockables
    owner_sent := false
    UseMoreStates(ctx, NodeList(lockable.dependencies), nodes, func(nodes NodeMap) error {
      for _, dependency := range(lockable.dependencies){
        ctx.Log.Logf("signal", "SENDING_TO_DEPENDENCY: %s -> %s", lockable.ID(), dependency.ID())
        dependency.Signal(ctx, signal, nodes)
        if lockable.owner != nil {
          if dependency.ID() == lockable.owner.ID() {
            owner_sent = true
          }
        }
      }
      return nil
    })
    if lockable.owner != nil && owner_sent == false {
      if lockable.owner.ID() != lockable.ID() {
        ctx.Log.Logf("signal", "SENDING_TO_OWNER: %s -> %s", lockable.ID(), lockable.owner.ID())
        UseMoreStates(ctx, []Node{lockable.owner}, nodes, func(nodes NodeMap) error {
          return lockable.owner.Signal(ctx, signal, nodes)
        })
      }
    }
  } else if signal.Direction() == Down {
    // Parent->Child, lockable updates lock holder
    UseMoreStates(ctx, NodeList(lockable.requirements), nodes, func(nodes NodeMap) error {
      for _, requirement := range(lockable.requirements) {
        err := requirement.Signal(ctx, signal, nodes)
        if err != nil {
          return err
        }
      }
      return nil
    })

  } else if signal.Direction() == Direct {
  } else {
    panic(fmt.Sprintf("Invalid signal direction: %d", signal.Direction()))
  }
  return nil
}

// Requires lockable and requirement's states to be locked for write
func UnlinkLockables(ctx * Context, lockable Lockable, requirement Lockable) error {
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
}

// Requires lockable and requirements to be locked for write, nodes passed because requirement check recursively locks
func LinkLockables(ctx * Context, lockable Lockable, requirements []Lockable, nodes NodeMap) error {
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

  // Check that all the requirements can be added
  // If the lockable is already locked, need to lock this resource as well before we can add it
  for _, requirement := range(requirements) {
    for _, req := range(requirements) {
      if req.ID() == requirement.ID() {
        continue
      }
      if checkIfRequirement(ctx, req, requirement, nodes) == true {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependenyc of %s so cannot add the same dependency", req.ID(), requirement.ID())
      }
    }
    if checkIfRequirement(ctx, lockable, requirement, nodes) == true {
      return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as requirement", requirement.ID(), lockable.ID())
    }

    if checkIfRequirement(ctx, requirement, lockable, nodes) == true {
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
    ctx.Log.Logf("lockable", "LOCKABLE_LINK: linked %s to %s as a requirement", requirement.ID(), lockable.ID())
  }

  // Return no error
  return nil
}

// Must be called withing update context
func checkIfRequirement(ctx * Context, r Lockable, cur Lockable, nodes NodeMap) bool {
  for _, c := range(cur.Requirements()) {
    if c.ID() == r.ID() {
      return true
    }
    is_requirement := false
    UpdateMoreStates(ctx, []Node{c}, nodes, func(nodes NodeMap) (error) {
      is_requirement = checkIfRequirement(ctx, cur, c, nodes)
      return nil
    })

    if is_requirement {
      return true
    }
  }

  return false
}

func LockLockables(ctx * Context, to_lock []Lockable, new_owner Lockable, nodes NodeMap) error {
  if to_lock == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: no list provided")
  }

  node_list := make([]Node, len(to_lock))
  for i, l := range(to_lock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_LOCK_ERR: Can not lock nil")
    }
    node_list[i] = l
  }
  if new_owner == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: nil cannot hold locks")
  }

  // Called with no requirements to lock, success
  if len(to_lock) == 0 {
    return nil
  }

  err := UpdateMoreStates(ctx, node_list, nodes, func(nodes NodeMap) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_lock) {
      ctx.Log.Logf("lockable", "LOCKABLE_LOCKING: %s from %s", req.ID(), new_owner.ID())

      // Check custom lock conditions
      err := req.CanLock(new_owner)
      if err != nil {
        return err
      }

      // If req is alreay locked, check that we can pass the lock
      if req.Owner() != nil {
        owner := req.Owner()
        if owner.ID() == new_owner.ID() {
          return fmt.Errorf("LOCKABLE_LOCK_ERR: %s already owns %s, cannot lock again", new_owner.ID(), req.ID())
        } else if owner.ID() == req.ID() {
          if req.AllowedToTakeLock(new_owner, req) == false {
            return fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", new_owner.ID(), req.ID(), owner.ID())
          }
          err := LockLockables(ctx, req.Requirements(), req, nodes)
          if err != nil {
            return err
          }
        } else {
          err := UpdateMoreStates(ctx, []Node{owner}, nodes, func(nodes NodeMap)(error){
            if owner.AllowedToTakeLock(new_owner, req) == false {
              return fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", new_owner.ID(), req.ID(), owner.ID())
            }
            err := LockLockables(ctx, req.Requirements(), req, nodes)
            return err
          })
          if err != nil {
            return err
          }
        }
      } else {
        err := LockLockables(ctx, req.Requirements(), req, nodes)
        if err != nil {
          return err
        }
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_lock) {
      old_owner := req.Owner()
      req.SetOwner(new_owner)
      new_owner.RecordLock(req, old_owner)
      if old_owner == nil {
        ctx.Log.Logf("lockable", "LOCKABLE_LOCK: %s locked %s", new_owner.ID(), req.ID())
      } else {
        ctx.Log.Logf("lockable", "LOCKABLE_LOCK: %s took lock of %s from %s", new_owner.ID(), req.ID(), old_owner.ID())
      }
    }
    return nil
  })
  return err
}

func UnlockLockables(ctx * Context, to_unlock []Lockable, old_owner Lockable, nodes NodeMap) error {
  if to_unlock == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: no list provided")
  }
  for _, l := range(to_unlock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: Can not lock nil")
    }
  }
  if old_owner == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: nil cannot hold locks")
  }

  // Called with no requirements to lock, success
  if len(to_unlock) == 0 {
    return nil
  }

  node_list := make([]Node, len(to_unlock))
  for i, l := range(to_unlock) {
    node_list[i] = l
  }

  err := UpdateMoreStates(ctx, node_list, nodes, func(nodes NodeMap) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_unlock) {
      ctx.Log.Logf("lockable", "LOCKABLE_UNLOCKING: %s from %s", req.ID(), old_owner.ID())

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

      err = UnlockLockables(ctx, req.Requirements(), req, nodes)
      if err != nil {
        return err
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_unlock) {
      new_owner := old_owner.RecordUnlock(req)
      req.SetOwner(new_owner)
      if new_owner == nil {
        ctx.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s unlocked %s", old_owner.ID(), req.ID())
      } else {
        ctx.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s passed lock of %s back to %s", old_owner.ID(), req.ID(), new_owner.ID())
      }
    }
    return nil
  })
  return err
}


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

func RestoreSimpleLockable(ctx * Context, lockable Lockable, j SimpleLockableJSON, nodes NodeMap) error {
  if j.Owner != nil {
    o, err := LoadNodeRecurse(ctx, *j.Owner, nodes)
    if err != nil {
      return err
    }
    o_l, ok := o.(Lockable)
    if ok == false {
      return fmt.Errorf("%s is not a Lockable", o.ID())
    }
    lockable.SetOwner(o_l)
  }

  for _, dep := range(j.Dependencies) {
    dep_node, err := LoadNodeRecurse(ctx, dep, nodes)
    if err != nil {
      return err
    }
    dep_l, ok := dep_node.(Lockable)
    if ok == false {
      return fmt.Errorf("%+v is not a Lockable as expected", dep_node)
    }
    lockable.AddDependency(dep_l)
  }

  for _, req := range(j.Requirements) {
    req_node, err := LoadNodeRecurse(ctx, req, nodes)
    if err != nil {
      return err
    }
    req_l, ok := req_node.(Lockable)
    if ok == false {
      return fmt.Errorf("%+v is not a Lockable as expected", req_node)
    }
    lockable.AddRequirement(req_l)
  }

  for l_id, h_id := range(j.LocksHeld) {
    l, err := LoadNodeRecurse(ctx, l_id, nodes)
    if err != nil {
      return err
    }
    l_l, ok := l.(Lockable)
    if ok == false {
      return fmt.Errorf("%s is not a Lockable", l.ID())
    }

    var h_l Lockable = nil
    if h_id != nil {
      h_node, err := LoadNodeRecurse(ctx, *h_id, nodes)
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

  return nil
}
