package graphvent

import (
  "fmt"
  "encoding/json"
)

// LockableState is the interface that any node that wants to posses locks must implement
//
// ReturnLock returns the node that held the lockable pointed to by ID before this node and
// removes the mapping from it's state, or nil if the lockable was unlocked previously
//
// AllowedToTakeLock returns true if the node pointed to by ID is allowed to take a lock from this node
//
type LockableState interface {
  NodeState

  ReturnLock(lockable_id NodeID) Lockable
  AllowedToTakeLock(node_id NodeID, lockable_id NodeID) bool
  RecordLockHolder(lockable_id NodeID, lock_holder Lockable)

  Requirements() []Lockable
  AddRequirement(requirement Lockable)
  Dependencies() []Lockable
  AddDependency(dependency Lockable)
  Owner() Lockable
  SetOwner(owner Lockable)
}

// BaseLockableStates are a minimum collection of variables for a basic implementation of a LockHolder
// Include in any state structs that should be lockable
type BaseLockableState struct {
  name string
  owner Lockable
  requirements []Lockable
  dependencies []Lockable
  locks_held map[NodeID]Lockable
}

type BaseLockableStateJSON struct {
  Name string `json:"name"`
  Owner *NodeID `json:"owner"`
  Dependencies []NodeID `json:"dependencies"`
  Requirements []NodeID `json:"requirements"`
  LocksHeld map[NodeID]*NodeID `json:"locks_held"`
}

func (state * BaseLockableState) MarshalJSON() ([]byte, error) {
  requirement_ids := make([]NodeID, len(state.requirements))
  for i, requirement := range(state.requirements) {
    requirement_ids[i] = requirement.ID()
  }

  dependency_ids := make([]NodeID, len(state.dependencies))
  for i, dependency := range(state.dependencies) {
    dependency_ids[i] = dependency.ID()
  }

  var owner_id *NodeID = nil
  if state.owner != nil {
    new_str := state.owner.ID()
    owner_id = &new_str
  }

  locks_held := map[NodeID]*NodeID{}
  for lockable_id, node := range(state.locks_held) {
    if node == nil {
      locks_held[lockable_id] = nil
    } else {
      str := node.ID()
      locks_held[lockable_id] = &str
    }
  }

  return json.Marshal(&BaseLockableStateJSON{
    Name: state.name,
    Owner: owner_id,
    Dependencies: dependency_ids,
    Requirements: requirement_ids,
    LocksHeld: locks_held,
  })
}

func (state * BaseLockableState) Name() string {
  return state.name
}

// Locks cannot be passed between base lockables, so the answer to
// "who used to own this lock held by a base lockable" is always "nobody"
func (state * BaseLockableState) ReturnLock(lockable_id NodeID) Lockable {
  node, exists := state.locks_held[lockable_id]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a lockable we don't own")
  }
  delete(state.locks_held, lockable_id)
  return node
}

// Nothing can take a lock from a base lockable either
func (state * BaseLockableState) AllowedToTakeLock(node_id NodeID, lockable_id NodeID) bool {
  _, exists := state.locks_held[lockable_id]
  if exists == false {
    panic ("Trying to give away lock we don't own")
  }
  return false
}

func (state * BaseLockableState) RecordLockHolder(lockable_id NodeID, lock_holder Lockable) {
  _, exists := state.locks_held[lockable_id]
  if exists == true {
    panic("Attempted to lock a lockable we're already holding(lock cycle)")
  }

  state.locks_held[lockable_id] = lock_holder
}

func (state * BaseLockableState) Owner() Lockable {
  return state.owner
}

func (state * BaseLockableState) SetOwner(owner Lockable) {
  state.owner = owner
}

func (state * BaseLockableState) Requirements() []Lockable {
  return state.requirements
}

func (state * BaseLockableState) AddRequirement(requirement Lockable) {
  if requirement == nil {
    panic("Will not connect nil to the DAG")
  }
  state.requirements = append(state.requirements, requirement)
}

func (state * BaseLockableState) Dependencies() []Lockable {
  return state.dependencies
}

func (state * BaseLockableState) AddDependency(dependency Lockable) {
  if dependency == nil {
    panic("Will not connect nil to the DAG")
  }

  state.dependencies = append(state.dependencies, dependency)
}

func LinkLockables(ctx * GraphContext, lockable Lockable, requirements []Lockable) error {
  if lockable == nil {
    return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link Lockables to nil as requirements")
  }

  for _, requirement := range(requirements) {
    if requirement == nil {
      return fmt.Errorf("LOCKABLE_LINK_ERR: Will not link nil to a Lockable as a requirement")
    }

    if lockable.ID() == requirement.ID() {
      return fmt.Errorf("LOCKABLE_LINK_ERR: cannot link %s to itself", lockable.ID())
    }
  }

  nodes := make([]GraphNode, len(requirements) + 1)
  nodes[0] = lockable
  for i, node := range(requirements) {
    nodes[i+1] = node
  }
  err := UpdateStates(ctx, nodes, func(states []NodeState) ([]NodeState, error) {
    // Check that all the requirements can be added
    lockable_state := states[0].(LockableState)
    // If the lockable is already locked, need to lock this resource as well before we can add it
    for i, requirement := range(requirements) {
      requirement_state := states[i+1].(LockableState)
      if checkIfRequirement(ctx, lockable.ID(), requirement_state, requirement.ID()) == true {
        return nil, fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as requirement", requirement.ID(), lockable.ID())
      }

      if checkIfRequirement(ctx, requirement.ID(), lockable_state, lockable.ID()) == true {
        return nil, fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as dependency again", lockable.ID(), requirement.ID())
      }
      if lockable_state.Owner() == nil {
        // If the new owner isn't locked, we can add the requirement
      } else if requirement_state.Owner() == nil {
        // if the new requirement isn't already locked but the owner is, the requirement needs to be locked first
        return nil, fmt.Errorf("LOCKABLE_LINK_ERR: %s is locked, %s must be locked to add", lockable.ID(), requirement.ID())
      } else {
        // If the new requirement is already locked and the owner is already locked, their owners need to match
        if requirement_state.Owner().ID() != lockable_state.Owner().ID() {
          return nil, fmt.Errorf("LOCKABLE_LINK_ERR: %s is not locked by the same owner as %s, can't link as requirement", requirement.ID(), lockable.ID())
        }
      }
    }
    // Update the states of the requirements
    for i, requirement := range(requirements) {
      requirement_state := states[i+1].(LockableState)
      requirement_state.AddDependency(lockable)
      lockable_state.AddRequirement(requirement)
    }

    // Return no error
    return states, nil
  })

  return err
}

func NewBaseLockableState(name string) BaseLockableState {
  state := BaseLockableState{
    locks_held: map[NodeID]Lockable{},
    name: name,
    owner: nil,
    requirements: []Lockable{},
    dependencies: []Lockable{},
  }

  return state
}

type Lockable interface {
  GraphNode
  // Called when locking the node to allow for custom lock behaviour
  Lock(node GraphNode, state LockableState)
  // Called to check if the node can lock
  CanLock(node GraphNode, state LockableState) error
  // Called when unlocking the node to allow for custom lock behaviour
  Unlock(node GraphNode, state LockableState)
  // Called to check if the node can unlock
  CanUnlock(node GraphNode, state LockableState) error
}

func (lockable * BaseLockable) PropagateUpdate(ctx * GraphContext, signal GraphSignal) {
  UseStates(ctx, []GraphNode{lockable}, func(states []NodeState) (error){
    lockable_state := states[0].(LockableState)
    if signal.Direction() == Up {
      // Child->Parent, lockable updates dependency lockables
      owner_sent := false
      for _, dependency := range lockable_state.Dependencies() {
        SendUpdate(ctx, dependency, signal)
        if lockable_state.Owner() != nil {
          if dependency.ID() != lockable_state.Owner().ID() {
            owner_sent = true
          }
        }
      }
      if lockable_state.Owner() != nil && owner_sent == false {
        SendUpdate(ctx, lockable_state.Owner(), signal)
      }
    } else if signal.Direction() == Down {
      // Parent->Child, lockable updates lock holder
      for _, requirement := range(lockable_state.Requirements()) {
        SendUpdate(ctx, requirement, signal)
      }

    } else if signal.Direction() == Direct {
    } else {
      panic(fmt.Sprintf("Invalid signal direction: %d", signal.Direction()))
    }
    return nil
  })
}

func checkIfRequirement(ctx * GraphContext, r_id NodeID, cur LockableState, cur_id NodeID) bool {
  for _, c := range(cur.Requirements()) {
    if c.ID() == r_id {
      return true
    }
    is_requirement := false
    UseStates(ctx, []GraphNode{c}, func(states []NodeState) (error) {
      requirement_state := states[0].(LockableState)
      is_requirement = checkIfRequirement(ctx, cur_id, requirement_state, c.ID())
      return nil
    })

    if is_requirement {
      return true
    }
  }

  return false
}

func LockLockables(ctx * GraphContext, to_lock []Lockable, holder Lockable, holder_state LockableState, owner_states map[NodeID]LockableState) error {
  if to_lock == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: no list provided")
  }
  for _, l := range(to_lock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_LOCK_ERR: Can not lock nil")
    }
  }
  if holder == nil {
    return fmt.Errorf("LOCKABLE_LOCK_ERR: nil cannot hold locks")
  }

  // Called with no requirements to lock, success
  if len(to_lock) == 0 {
    return nil
  }

  if holder_state == nil {
    if len(to_lock) != 1 {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: if holder_state is nil, can only self-lock")
    } else if holder.ID() != to_lock[0].ID() {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: if holder_state is nil, can only self-lock")
    }
  }


  node_list := make([]GraphNode, len(to_lock))
  for i, l := range(to_lock) {
    node_list[i] = l
  }

  err := UpdateStates(ctx, node_list, func(states []NodeState) ([]NodeState, error) {
    // First loop is to check that the states can be locked, and locks all requirements
    for i, state := range(states) {
      req := to_lock[i]
      req_state, ok := state.(LockableState)
      ctx.Log.Logf("lockable", "LOCKABLE_LOCKING: %s from %s", req.ID(), holder.ID())
      if ok == false {
        return nil, fmt.Errorf("LOCKABLE_LOCK_ERR: %s(requirement of %s) does not have a LockableState", req.ID(), holder.ID())
      }

      // Check custom lock conditions
      err := req.CanLock(holder, req_state)
      if err != nil {
        return nil, err
      }

      // If req is alreay locked, check that we can pass the lock
      if req_state.Owner() != nil {
        owner := req_state.Owner()
        // Check if reqs owner will let holder take the lock from it
        // The owner is either the same node, a node higher up in the dependency tree, or node outside the dependency tree(must be enforeced when linking dependencies)
        // If the owner is the same node, we already have all the states we need to check lock passing
        // If the owner is higher up in the dependency tree, we've either already got it's state getting to this node, or we won't try to get it's state as a dependency to lock this node, so we can grab the state and add it to a map
        // If the owner is outside the dependency tree, then we won't try to grab it's lock trying to lock this node recursively
        // So if the owner is the same node we don't need a new state, but if the owner is a different node then we need to grab it's state and add it to the list
        if owner.ID() == req.ID() {
          if req_state.AllowedToTakeLock(holder.ID(), req.ID()) == false {
            return nil, fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", holder.ID(), req.ID(), owner.ID())
          }
          // RECURSE: At this point either:
          // 1) req has no children and the next LockLockables will return instantly
          //   a) in this case, we're holding every state mutex up to the resource being locked
          //      and all the owners passing a lock, so we can start to change state
          // 2) req has children, and we will recurse(checking that locking is allowed) until we reach a leaf and can release the locks as we change state. The call will either return nil if state has changed, on an error if no state has changed
          err := LockLockables(ctx, req_state.Requirements(), req, req_state, owner_states)
          if err != nil {
            return nil, err
          }
        } else {
          owner_state, exists := owner_states[owner.ID()]
          if exists == false {
            err := UseStates(ctx, []GraphNode{req_state.Owner()}, func(states []NodeState)(error){
              owner_state, ok := states[0].(LockableState)
              if ok == false {
                return fmt.Errorf("LOCKABLE_LOCK_ERR: %s does not have a LockableState", owner.ID())
              }

              if owner_state.AllowedToTakeLock(holder.ID(), req.ID()) == false {
                return fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", holder.ID(), req.ID(), owner.ID())
              }
              owner_states[owner.ID()] = owner_state
              err := LockLockables(ctx, req_state.Requirements(), req, req_state, owner_states)
              return err
            })
            if err != nil {
              return nil, err
            }
          } else {
            if owner_state.AllowedToTakeLock(holder.ID(), req.ID()) == false {
              return nil, fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", holder.ID(), req.ID(), owner.ID())
            }
            err := LockLockables(ctx, req_state.Requirements(), req, req_state, owner_states)
            if err != nil {
              return nil, err
            }
          }
        }
      } else {
        err := LockLockables(ctx, req_state.Requirements(), req, req_state, owner_states)
        if err != nil {
          return nil, err
        }
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for i, state := range(states) {
      req := to_lock[i]
      req_state := state.(LockableState)
      old_owner := req_state.Owner()
      req_state.SetOwner(holder)
      if req.ID() == holder.ID() {
        req_state.RecordLockHolder(req.ID(), old_owner)
      } else {
        holder_state.RecordLockHolder(req.ID(), old_owner)
      }
      req.Lock(holder, req_state)
      if old_owner == nil {
        ctx.Log.Logf("lockable", "LOCKABLE_LOCK: %s locked %s", holder.ID(), req.ID())
      } else {
        ctx.Log.Logf("lockable", "LOCKABLE_LOCK: %s took lock of %s from %s", holder.ID(), req.ID(), old_owner.ID())
      }
    }
    return states, nil
  })
  return err
}

func UnlockLockables(ctx * GraphContext, to_unlock []Lockable, holder Lockable, holder_state LockableState, owner_states map[NodeID]LockableState) error {
  if to_unlock == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: no list provided")
  }
  for _, l := range(to_unlock) {
    if l == nil {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: Can not lock nil")
    }
  }
  if holder == nil {
    return fmt.Errorf("LOCKABLE_UNLOCK_ERR: nil cannot hold locks")
  }

  // Called with no requirements to lock, success
  if len(to_unlock) == 0 {
    return nil
  }

  if holder_state == nil {
    if len(to_unlock) != 1 {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: if holder_state is nil, can only self-lock")
    } else if holder.ID() != to_unlock[0].ID() {
      return fmt.Errorf("LOCKABLE_UNLOCK_ERR: if holder_state is nil, can only self-lock")
    }
  }

  node_list := make([]GraphNode, len(to_unlock))
  for i, l := range(to_unlock) {
    node_list[i] = l
  }

  err := UpdateStates(ctx, node_list, func(states []NodeState) ([]NodeState, error) {
    // First loop is to check that the states can be locked, and locks all requirements
    for i, state := range(states) {
      req := to_unlock[i]
      req_state, ok := state.(LockableState)
      ctx.Log.Logf("lockable", "LOCKABLE_UNLOCKING: %s from %s", req.ID(), holder.ID())
      if ok == false {
        return nil, fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s(requirement of %s) does not have a LockableState", req.ID(), holder.ID())
      }

      // Check if the owner is correct
      if req_state.Owner() != nil {
        if req_state.Owner().ID() != holder.ID() {
          return nil, fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked by %s", req.ID(), holder.ID())
        }
      } else {
        return nil, fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked", req.ID())
      }

      // Check custom unlock conditions
      err := req.CanUnlock(holder, req_state)
      if err != nil {
        return nil, err
      }

      err = UnlockLockables(ctx, req_state.Requirements(), req, req_state, owner_states)
      if err != nil {
        return nil, err
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for i, state := range(states) {
      req := to_unlock[i]
      req_state := state.(LockableState)
      var new_owner Lockable = nil
      if holder_state == nil {
        new_owner = req_state.ReturnLock(req.ID())
      } else {
        new_owner = holder_state.ReturnLock(req.ID())
      }
      req_state.SetOwner(new_owner)
      req.Unlock(holder, req_state)
      if new_owner == nil {
        ctx.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s unlocked %s", holder.ID(), req.ID())
      } else {
        ctx.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s passed lock of %s back to %s", holder.ID(), req.ID(), new_owner.ID())
      }
    }
    return states, nil
  })
  return err
}

// BaseLockables represent simple lockables in the DAG that can be used to create a hierarchy of locks that store names
type BaseLockable struct {
  BaseNode
}

//BaseLockables don't check anything special when locking/unlocking
func (lockable * BaseLockable) CanLock(node GraphNode, state LockableState) error {
  return nil
}

func (lockable * BaseLockable) CanUnlock(node GraphNode, state LockableState) error {
  return nil
}

//BaseLockables don't check anything special when locking/unlocking
func (lockable * BaseLockable) Lock(node GraphNode, state LockableState) {
  return
}

func (lockable * BaseLockable) Unlock(node GraphNode, state LockableState) {
  return
}

func NewBaseLockable(ctx * GraphContext, state LockableState) (BaseLockable, error) {
  base_node, err := NewNode(ctx, state)
  if err != nil {
    return BaseLockable{}, err
  }

  lockable := BaseLockable{
    BaseNode: base_node,
  }

  return lockable, nil
}

func NewSimpleBaseLockable(ctx * GraphContext, name string, requirements []Lockable) (*BaseLockable, error) {
  state := NewBaseLockableState(name)
  lockable, err := NewBaseLockable(ctx, &state)
  if err != nil {
    return nil, err
  }
  lockable_ptr := &lockable
  err = LinkLockables(ctx, lockable_ptr, requirements)
  if err != nil {
    return nil, err
  }

  return lockable_ptr, nil
}
