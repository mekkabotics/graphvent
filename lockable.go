package graphvent

import (
  "fmt"
  "encoding/json"
)

// LockHolderState is the interface that any node that wants to posses locks must implement
//
// ReturnLock returns the node that held the resource pointed to by ID before this node and
// removes the mapping from it's state, or nil if the resource was unlocked previously
//
// AllowedToTakeLock returns true if the node pointed to by ID is allowed to take a lock from this node
//
// RecordLockHolder records that resource_id needs to be passed back to lock_holder
type LockHolderState interface {
  ReturnLock(resource_id NodeID) GraphNode
  AllowedToTakeLock(node_id NodeID, resource_id NodeID) bool
  RecordLockHolder(resource_id NodeID, lock_holder GraphNode)
}

// LockableState is the interface that a lockables state must have to allow it to connect to the DAG
type LockableState interface {
  LockHolderState
  Name() string
  Requirements() []Lockable
  AddRequirement(requirement Lockable)
  Dependencies() []Lockable
  AddDependency(dependency Lockable)
  Owner() GraphNode
  SetOwner(owner GraphNode)
}

type BaseLockHolderState struct {
  delegation_map map[NodeID] GraphNode
}

type BaseLockHolderStateJSON struct {
    Delegations map[NodeID]*NodeID `json:"delegations"`
}

func (state * BaseLockHolderState) MarshalJSON() ([]byte, error) {
  delegations := map[NodeID]*NodeID{}
  for lockable_id, node := range(state.delegation_map) {
    if node == nil {
      delegations[lockable_id] = nil
    } else {
      str := node.ID()
      delegations[lockable_id] = &str
    }
  }
  return json.Marshal(&BaseLockHolderStateJSON{
    Delegations: delegations,
  })
}

// BaseLockableStates are a minimum collection of variables for a basic implementation of a LockHolder
// Include in any state structs that should be lockable
type BaseLockableState struct {
  BaseLockHolderState
  name string
  owner GraphNode
  requirements []Lockable
  dependencies []Lockable
}

type BaseLockableStateJSON struct {
  Name string `json:"name"`
  Owner *NodeID `json:"owner"`
  Dependencies []NodeID `json:"dependencies"`
  Requirements []NodeID `json:"requirements"`
  HolderState *BaseLockHolderState `json:"holder_state"`
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

  return json.Marshal(&BaseLockableStateJSON{
    Name: state.name,
    Owner: owner_id,
    Dependencies: dependency_ids,
    Requirements: requirement_ids,
    HolderState: &state.BaseLockHolderState,
  })
}

func (state * BaseLockableState) Name() string {
  return state.name
}

// Locks cannot be passed between base lockables, so the answer to
// "who used to own this lock held by a base lockable" is always "nobody"
func (state * BaseLockHolderState) ReturnLock(resource_id NodeID) GraphNode {
  node, exists := state.delegation_map[resource_id]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a resource we don't own")
  }
  delete(state.delegation_map, resource_id)
  return node
}

// Nothing can take a lock from a base lockable either
func (state * BaseLockHolderState) AllowedToTakeLock(node_id NodeID, resource_id NodeID) bool {
  _, exists := state.delegation_map[resource_id]
  if exists == false {
    panic ("Trying to give away lock we don't own")
  }
  return false
}

func (state * BaseLockHolderState) RecordLockHolder(resource_id NodeID, lock_holder GraphNode) {
  _, exists := state.delegation_map[resource_id]
  if exists == true {
    panic("Attempted to lock a resource we're already holding(lock cycle)")
  }

  state.delegation_map[resource_id] = lock_holder
}

func (state * BaseLockableState) Owner() GraphNode {
  return state.owner
}

func (state * BaseLockableState) SetOwner(owner GraphNode) {
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

func NewLockHolderState() BaseLockHolderState {
  return BaseLockHolderState{
    delegation_map: map[NodeID]GraphNode{},
  }
}

func NewLockableState(name string) BaseLockableState {
  return BaseLockableState{
    BaseLockHolderState: NewLockHolderState(),
    name: name,
    owner: nil,
    requirements: []Lockable{},
    dependencies: []Lockable{},
  }
}

// Link a lockable with a requirement
func LinkLockables(ctx * GraphContext, lockable Lockable, requirement Lockable) error {
  if lockable == nil || requirement == nil {
    return fmt.Errorf("Will not connect nil to DAG")
  }

  if lockable.ID() == requirement.ID() {
    return fmt.Errorf("Will not link %s as requirement of itself", lockable.ID())
  }

  _, err := UpdateStates(ctx, []GraphNode{lockable, requirement}, func(states []NodeState) ([]NodeState, interface{}, error) {
    lockable_state := states[0].(LockableState)
    requirement_state := states[1].(LockableState)

    if checkIfRequirement(ctx, lockable_state, lockable.ID(), requirement_state, requirement.ID()) == true {
      return nil, nil, fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as requirement", requirement.ID(), lockable.ID())
    }

    if checkIfRequirement(ctx, requirement_state, requirement.ID(), lockable_state, lockable.ID()) == true {
      return nil, nil, fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as dependency again", lockable.ID(), requirement.ID())
    }

    lockable_state.AddRequirement(requirement)
    requirement_state.AddDependency(lockable)
    return []NodeState{lockable_state, requirement_state}, nil, nil
  })
  return err
}

type Lockable interface {
  GraphNode
  // Called when locking the node to allow for custom lock behaviour
  Lock(node GraphNode, state LockableState) error
  // Called when unlocking the node to allow for custom lock behaviour
  Unlock(node GraphNode, state LockableState) error
}

// Lockables propagate update up to multiple dependencies, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (lockable * BaseLockable) PropagateUpdate(ctx * GraphContext, signal GraphSignal) {
  UseStates(ctx, []GraphNode{lockable}, func(states []NodeState) (interface{}, error){
    lockable_state := states[0].(LockableState)
    if signal.Direction() == Up {
      // Child->Parent, lockable updates dependency lockables
      for _, dependency := range lockable_state.Dependencies() {
        SendUpdate(ctx, dependency, signal)
      }
    } else if signal.Direction() == Down {
      // Parent->Child, lockable updates lock holder
      if lockable_state.Owner() != nil {
        SendUpdate(ctx, lockable_state.Owner(), signal)
      }

      for _, requirement := range(lockable_state.Requirements()) {
        SendUpdate(ctx, requirement, signal)
      }
    } else if signal.Direction() == Direct {
    } else {
      panic(fmt.Sprintf("Invalid signal direction: %d", signal.Direction()))
    }
    return nil, nil
  })
}

func checkIfRequirement(ctx * GraphContext, r LockableState, r_id NodeID, cur LockableState, cur_id NodeID) bool {
  for _, c := range(cur.Requirements()) {
    if c.ID() == r_id {
      return true
    }
    val, _ := UseStates(ctx, []GraphNode{c}, func(states []NodeState) (interface{}, error) {
      requirement_state := states[0].(LockableState)
      return checkIfRequirement(ctx, cur, cur_id, requirement_state, c.ID()), nil
    })

    is_requirement := val.(bool)
    if is_requirement {
      return true
    }
  }

  return false
}

func UnlockLockable(ctx * GraphContext, lockable Lockable, node GraphNode, node_state LockHolderState) error {
  if node == nil || lockable == nil{
    panic("Cannot unlock without a specified node and lockable")
  }
  _, err := UpdateStates(ctx, []GraphNode{lockable}, func(states []NodeState) ([]NodeState, interface{}, error) {
    if lockable.ID() == node.ID() {
      if node_state != nil {
        panic("node_state must be nil if unlocking lockable from itself")
      }
      node_state = states[0].(LockHolderState)
    }
    lockable_state := states[0].(LockableState)

    if lockable_state.Owner() == nil {
      return nil, nil, fmt.Errorf("Lockable already unlocked")
    }

    if lockable_state.Owner().ID() != node.ID() {
      return nil, nil, fmt.Errorf("Lockable %s not locked by %s", lockable.ID(), node.ID())
    }

    var lock_err error = nil
    for _, requirement := range(lockable_state.Requirements()) {
      var err error = nil
      err = UnlockLockable(ctx, requirement, node, node_state)
      if err != nil {
        lock_err = err
        break
      }
    }

    if lock_err != nil {
      return nil, nil, fmt.Errorf("Lockable %s failed to unlock: %e", lockable.ID(), lock_err)
    }

    new_owner := node_state.ReturnLock(lockable.ID())
    lockable_state.SetOwner(new_owner)
    err := lockable.Unlock(node, lockable_state)
    if err != nil {
      return nil, nil, fmt.Errorf("Lockable %s failed custom Unlock: %e", lockable.ID(), err)
    }

    if lockable_state.Owner() == nil {
      ctx.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s unlocked %s", node.ID(), lockable.ID())
    } else {
      ctx.Log.Logf("lockable", "LOCKABLE_UNLOCK: %s passed lock of %s back to %s", node.ID(), lockable.ID(), lockable_state.Owner().ID())
    }

    return []NodeState{lockable_state}, nil, nil
  })

  return err
}

func LockLockable(ctx * GraphContext, lockable Lockable, node GraphNode, node_state LockHolderState) error {
  if node == nil || lockable == nil {
    panic("Cannot lock without a specified node and lockable")
  }
  ctx.Log.Logf("resource", "LOCKING: %s from %s", lockable.ID(), node.ID())

  _, err := UpdateStates(ctx, []GraphNode{lockable}, func(states []NodeState) ([]NodeState, interface{}, error) {
    if lockable.ID() == node.ID() {
      if node_state != nil {
        panic("node_state must be nil if locking lockable from itself")
      }
      node_state = states[0].(LockHolderState)
    }
    lockable_state := states[0].(LockableState)
    if lockable_state.Owner() != nil {
      var lock_pass_allowed bool = false

      if lockable_state.Owner().ID() == lockable.ID() {
        lock_pass_allowed = lockable_state.AllowedToTakeLock(node.ID(), lockable.ID())
      } else {
        tmp, _ := UseStates(ctx, []GraphNode{lockable_state.Owner()}, func(states []NodeState)(interface{}, error){
          return states[0].(LockHolderState).AllowedToTakeLock(node.ID(), lockable.ID()), nil
        })
        lock_pass_allowed = tmp.(bool)
      }


      if lock_pass_allowed == false {
        return nil, nil, fmt.Errorf("%s is not allowed to take a lock from %s", node.ID(), lockable_state.Owner().ID())
      }
    }

    err := lockable.Lock(node, lockable_state)
    if err != nil {
      return nil, nil, fmt.Errorf("Failed to lock lockable: %e", err)
    }

    var lock_err error = nil
    locked_requirements := []Lockable{}
    for _, requirement := range(lockable_state.Requirements()) {
      err = LockLockable(ctx, requirement, node, node_state)
      if err != nil {
        lock_err = err
        break
      }
      locked_requirements = append(locked_requirements, requirement)
    }

    if lock_err != nil {
      for _, locked_lockable := range(locked_requirements) {
        err = UnlockLockable(ctx, locked_lockable, node, node_state)
        if err != nil {
          panic(err)
        }
      }
      return nil, nil, fmt.Errorf("Lockable failed to lock: %e", lock_err)
    }

    old_owner := lockable_state.Owner()
    lockable_state.SetOwner(node)
    node_state.RecordLockHolder(lockable.ID(), old_owner)

    if old_owner == nil {
      ctx.Log.Logf("lockable", "LOCKABLE_LOCK: %s locked %s", node.ID(), lockable.ID())
    } else {
      ctx.Log.Logf("lockable", "LOCKABLE_LOCK: %s took lock of %s from %s", node.ID(), lockable.ID(), old_owner.ID())
    }

    return []NodeState{lockable_state}, nil, nil
  })

  return err
}

// BaseLockables represent simple lockables in the DAG that can be used to create a hierarchy of locks that store names
type BaseLockable struct {
  BaseNode
}

//BaseLockables don't check anything special when locking/unlocking
func (lockable * BaseLockable) Lock(node GraphNode, state LockableState) error {
  return nil
}

func (lockable * BaseLockable) Unlock(node GraphNode, state LockableState) error {
  return nil
}

func NewLockable(ctx * GraphContext, name string, requirements []Lockable) (* BaseLockable, error) {
  state := NewLockableState(name)
  lockable := &BaseLockable{
    BaseNode: NewNode(ctx, RandID(), &state),
  }

  for _, requirement := range(requirements) {
    err := LinkLockables(ctx, lockable, requirement)
    if err != nil {
      return nil, err
    }
  }

  return lockable, nil
}
