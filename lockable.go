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
  _type string
  name string
  owner Lockable
  requirements []Lockable
  dependencies []Lockable
  locks_held map[NodeID]Lockable
}

type BaseLockableStateJSON struct {
  Type string `json:"type"`
  Name string `json:"name"`
  Owner *NodeID `json:"owner"`
  Dependencies []NodeID `json:"dependencies"`
  Requirements []NodeID `json:"requirements"`
  LocksHeld map[NodeID]*NodeID `json:"locks_held"`
}

func (state * BaseLockableState) Type() string {
  return state._type
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
    Type: state._type,
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

  if len(requirements) == 0 {
    return nil
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
  err := UpdateStates(ctx, nodes, func(states NodeStateMap) error {
    // Check that all the requirements can be added
    lockable_state := states[lockable.ID()].(LockableState)
    // If the lockable is already locked, need to lock this resource as well before we can add it
    for _, requirement := range(requirements) {
      requirement_state := states[requirement.ID()].(LockableState)
      for _, req := range(requirements) {
        if req.ID() == requirement.ID() {
          continue
        }
        if checkIfRequirement(ctx, req.ID(), requirement_state, requirement.ID(), states) == true {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependenyc of %s so cannot add the same dependency", req.ID(), requirement.ID())
        }
      }
      if checkIfRequirement(ctx, lockable.ID(), requirement_state, requirement.ID(), states) == true {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as requirement", requirement.ID(), lockable.ID())
      }

      if checkIfRequirement(ctx, requirement.ID(), lockable_state, lockable.ID(), states) == true {
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is a dependency of %s so cannot link as dependency again", lockable.ID(), requirement.ID())
      }
      if lockable_state.Owner() == nil {
        // If the new owner isn't locked, we can add the requirement
      } else if requirement_state.Owner() == nil {
        // if the new requirement isn't already locked but the owner is, the requirement needs to be locked first
        return fmt.Errorf("LOCKABLE_LINK_ERR: %s is locked, %s must be locked to add", lockable.ID(), requirement.ID())
      } else {
        // If the new requirement is already locked and the owner is already locked, their owners need to match
        if requirement_state.Owner().ID() != lockable_state.Owner().ID() {
          return fmt.Errorf("LOCKABLE_LINK_ERR: %s is not locked by the same owner as %s, can't link as requirement", requirement.ID(), lockable.ID())
        }
      }
    }
    // Update the states of the requirements
    for _, requirement := range(requirements) {
      requirement_state := states[requirement.ID()].(LockableState)
      requirement_state.AddDependency(lockable)
      lockable_state.AddRequirement(requirement)
      ctx.Log.Logf("lockable", "LOCKABLE_LINK: linked %s to %s as a requirement", requirement.ID(), lockable.ID())
    }

    // Return no error
    return nil
  })


  return err
}

func NewBaseLockableState(name string, _type string) BaseLockableState {
  state := BaseLockableState{
    locks_held: map[NodeID]Lockable{},
    _type: _type,
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
  UseStates(ctx, []GraphNode{lockable}, func(states NodeStateMap) error {
    lockable_state := states[lockable.ID()].(LockableState)
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

func checkIfRequirement(ctx * GraphContext, r_id NodeID, cur LockableState, cur_id NodeID, states NodeStateMap) bool {
  for _, c := range(cur.Requirements()) {
    if c.ID() == r_id {
      return true
    }
    is_requirement := false
    UpdateMoreStates(ctx, []GraphNode{c}, states, func(states NodeStateMap) (error) {
      requirement_state := states[c.ID()].(LockableState)
      is_requirement = checkIfRequirement(ctx, cur_id, requirement_state, c.ID(), states)
      return nil
    })

    if is_requirement {
      return true
    }
  }

  return false
}

func LockLockables(ctx * GraphContext, to_lock []Lockable, holder Lockable, holder_state LockableState, states NodeStateMap) error {
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

  err := UpdateMoreStates(ctx, node_list, states, func(states NodeStateMap) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_lock) {
      state := states[req.ID()]
      req_state, ok := state.(LockableState)
      ctx.Log.Logf("lockable", "LOCKABLE_LOCKING: %s from %s", req.ID(), holder.ID())
      if ok == false {
        return fmt.Errorf("LOCKABLE_LOCK_ERR: %s(requirement of %s) does not have a LockableState", req.ID(), holder.ID())
      }

      // Check custom lock conditions
      err := req.CanLock(holder, req_state)
      if err != nil {
        return err
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
            return fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", holder.ID(), req.ID(), owner.ID())
          }
          // RECURSE: At this point either:
          // 1) req has no children and the next LockLockables will return instantly
          //   a) in this case, we're holding every state mutex up to the resource being locked
          //      and all the owners passing a lock, so we can start to change state
          // 2) req has children, and we will recurse(checking that locking is allowed) until we reach a leaf and can release the locks as we change state. The call will either return nil if state has changed, on an error if no state has changed
          err := LockLockables(ctx, req_state.Requirements(), req, req_state, states)
          if err != nil {
            return err
          }
        } else {
          err := UpdateMoreStates(ctx, []GraphNode{owner}, states, func(states NodeStateMap)(error){
            owner_state, ok := states[owner.ID()].(LockableState)
            if ok == false {
              return fmt.Errorf("LOCKABLE_LOCK_ERR: %s does not have a LockableState", owner.ID())
            }

            if owner_state.AllowedToTakeLock(holder.ID(), req.ID()) == false {
              return fmt.Errorf("LOCKABLE_LOCK_ERR: %s is not allowed to take %s's lock from %s", holder.ID(), req.ID(), owner.ID())
            }
            err := LockLockables(ctx, req_state.Requirements(), req, req_state, states)
            return err
          })
          if err != nil {
            return err
          }
        }
      } else {
        err := LockLockables(ctx, req_state.Requirements(), req, req_state, states)
        if err != nil {
          return err
        }
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_lock) {
      req_state := states[req.ID()].(LockableState)
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
    return nil
  })
  return err
}

func UnlockLockables(ctx * GraphContext, to_unlock []Lockable, holder Lockable, holder_state LockableState, states NodeStateMap) error {
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

  err := UpdateMoreStates(ctx, node_list, states, func(states NodeStateMap) error {
    // First loop is to check that the states can be locked, and locks all requirements
    for _, req := range(to_unlock) {
      req_state, ok := states[req.ID()].(LockableState)
      ctx.Log.Logf("lockable", "LOCKABLE_UNLOCKING: %s from %s", req.ID(), holder.ID())
      if ok == false {
        return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s(requirement of %s) does not have a LockableState", req.ID(), holder.ID())
      }

      // Check if the owner is correct
      if req_state.Owner() != nil {
        if req_state.Owner().ID() != holder.ID() {
          return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked by %s", req.ID(), holder.ID())
        }
      } else {
        return fmt.Errorf("LOCKABLE_UNLOCK_ERR: %s is not locked", req.ID())
      }

      // Check custom unlock conditions
      err := req.CanUnlock(holder, req_state)
      if err != nil {
        return err
      }

      err = UnlockLockables(ctx, req_state.Requirements(), req, req_state, states)
      if err != nil {
        return err
      }
    }

    // At this point state modification will be started, so no errors can be returned
    for _, req := range(to_unlock) {
      req_state := states[req.ID()].(LockableState)
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
    return nil
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

func LoadBaseLockable(ctx * GraphContext, id NodeID) (GraphNode, error) {
  // call LoadNodeRecurse on any connected nodes to ensure they're loaded and return the id
  base_node := RestoreNode(ctx, id)
  lockable := BaseLockable{
    BaseNode: base_node,
  }

  return &lockable, nil
}

func LoadBaseLockableState(ctx * GraphContext, id NodeID, data []byte, loaded_nodes map[NodeID]GraphNode)(NodeState, error){
  var j BaseLockableStateJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  var owner Lockable = nil
  if j.Owner != nil {
    o, err := LoadNodeRecurse(ctx, *j.Owner, loaded_nodes)
    if err != nil {
      return nil, err
    }
    o_l, ok := o.(Lockable)
    if ok == false {
      return nil, err
    }
    owner = o_l
  }

  state := BaseLockableState{
    _type: "base_lockable",
    name: j.Name,
    owner: owner,
    dependencies: make([]Lockable, len(j.Dependencies)),
    requirements: make([]Lockable, len(j.Requirements)),
    locks_held: map[NodeID]Lockable{},
  }

  for i, dep := range(j.Dependencies) {
    dep_node, err := LoadNodeRecurse(ctx, dep, loaded_nodes)
    if err != nil {
      return nil, err
    }
    dep_l, ok := dep_node.(Lockable)
    if ok == false {
      return nil, fmt.Errorf("%+v is not a Lockable as expected", dep_node)
    }
    state.dependencies[i] = dep_l
  }

  for i, req := range(j.Requirements) {
    req_node, err := LoadNodeRecurse(ctx, req, loaded_nodes)
    if err != nil {
      return nil, err
    }
    req_l, ok := req_node.(Lockable)
    if ok == false {
      return nil, fmt.Errorf("%+v is not a Lockable as expected", req_node)
    }
    state.requirements[i] = req_l
  }

  for l_id, h_id := range(j.LocksHeld) {
    _, err := LoadNodeRecurse(ctx, l_id, loaded_nodes)
    if err != nil {
      return nil, err
    }
    var h_l Lockable = nil
    if h_id != nil {
      h_node, err := LoadNodeRecurse(ctx, *h_id, loaded_nodes)
      if err != nil {
        return nil, err
      }
      h, ok := h_node.(Lockable)
      if ok == false {
        return nil, err
      }
      h_l = h
    }
    state.locks_held[l_id] = h_l
  }
  return &state, nil
}

func LoadNode(ctx * GraphContext, id NodeID) (GraphNode, error) {
  // Initialize an empty list of loaded nodes, then start loading them from id
  loaded_nodes := map[NodeID]GraphNode{}
  return LoadNodeRecurse(ctx, id, loaded_nodes)
}

type DBJSONBase struct {
  Type string `json:"type"`
}

// Check if a node is already loaded, load it's state bytes from the DB and parse the type if it's not already loaded
// Call the node load function related to the type, which will call this parse function recusively as needed
func LoadNodeRecurse(ctx * GraphContext, id NodeID, loaded_nodes map[NodeID]GraphNode) (GraphNode, error) {
  node, exists := loaded_nodes[id]
  if exists == false {
    state_bytes, err := ReadDBState(ctx, id)
    if err != nil {
      return nil, err
    }

    var base DBJSONBase
    err = json.Unmarshal(state_bytes, &base)
    if err != nil {
      return nil, err
    }

    ctx.Log.Logf("graph", "GRAPH_DB_LOAD: %s(%s)", base.Type, id)

    node_fn, exists := ctx.NodeLoadFuncs[base.Type]
    if exists == false {
      return nil, fmt.Errorf("%s is not a known node type", base.Type)
    }

    node, err = node_fn(ctx, id)
    if err != nil {
      return nil, err
    }

    loaded_nodes[id] = node

    state_fn, exists := ctx.StateLoadFuncs[base.Type]
    if exists == false {
      return nil, fmt.Errorf("%s is not a known node state type", base.Type)
    }

    state, err := state_fn(ctx, id, state_bytes, loaded_nodes)
    if err != nil {
      return nil, err
    }

    node.SetState(state)
  }
  return node, nil
}

func NewSimpleBaseLockable(ctx * GraphContext, name string, requirements []Lockable) (*BaseLockable, error) {
  state := NewBaseLockableState(name, "base_lockable")
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
