package graphvent

import (
  "fmt"
)

// Link a resource with a child
func LinkResource(ctx * GraphContext, resource Resource, child Resource) error {
  if resource == nil || child == nil {
    return fmt.Errorf("Will not connect nil to DAG")
  }
  _, err := UpdateStates(ctx, []GraphNode{resource, child}, func(states []NodeState) ([]NodeState, interface{}, error) {
    resource_state := states[0].(ResourceState)
    child_state := states[1].(ResourceState)

    if checkIfChild(ctx, resource_state, resource.ID(), child_state, child.ID()) == true {
      return nil, nil, fmt.Errorf("RESOURCE_LINK_ERR: %s is a parent of %s so cannot link as child", child.ID(), resource.ID())
    }

    resource_state.children = append(resource_state.children, child)
    child_state.parents = append(child_state.parents, resource)
    return []NodeState{resource_state, child_state}, nil, nil
  })
  return err
}

// Link multiple children to a resource
func LinkResources(ctx * GraphContext, resource Resource, children []Resource) error {
  if resource == nil || children == nil {
    return fmt.Errorf("Invalid input")
  }

  found := map[NodeID]bool{}
  child_nodes := make([]GraphNode, len(children))
  for i, child := range(children) {
    if child == nil {
      return fmt.Errorf("Will not connect nil to DAG")
    }
    _, exists := found[child.ID()]
    if exists == true {
      return fmt.Errorf("Will not connect the same child twice")
    }
    found[child.ID()] = true
    child_nodes[i] = child
  }

  _, err := UpdateStates(ctx, append([]GraphNode{resource}, child_nodes...), func(states []NodeState) ([]NodeState, interface{}, error) {
    resource_state := states[0].(ResourceState)

    new_states := make([]ResourceState, len(states))
    for i, state := range(states) {
      new_states[i] = state.(ResourceState)
    }

    for i, state := range(states[1:]) {
      child_state := state.(ResourceState)

      if checkIfChild(ctx, resource_state, resource.ID(), child_state, children[i].ID()) == true {
        return nil, nil, fmt.Errorf("RESOURCES_LINK_ERR: %s is a parent of %s so cannot link as child", children[i].ID() , resource.ID())
      }

      new_states[0].children = append(new_states[0].children, children[i])
      new_states[i+1].parents = append(new_states[i+1].parents, resource)
    }
    ret_states := make([]NodeState, len(states))
    for i, state := range(new_states) {
      ret_states[i] = state
    }
    return ret_states, nil, nil
  })

  return err
}

type ResourceState struct {
  name string
  owner GraphNode
  children []Resource
  parents []Resource
}

func (state ResourceState) Serialize() []byte {
  return []byte(state.name)
}

// Locks cannot be passed between resources, so the answer to
// "who used to own this lock held by a resource" is always "nobody"
func (state ResourceState) OriginalLockHolder(id NodeID) GraphNode {
  return nil
}

// Nothing can take a lock from a resource
func (state ResourceState) AllowedToTakeLock(id NodeID) bool {
  return false
}

func (state ResourceState) RecordLockHolder(id NodeID, lock_holder GraphNode) NodeState {
  if lock_holder != nil {
    panic("Attempted to delegate a lock to a resource")
  }

  return state
}

func NewResourceState(name string) ResourceState {
  return ResourceState{
    name: name,
    owner: nil,
    children: []Resource{},
    parents: []Resource{},
  }
}

// Resource represents a Node which can be locked by another node,
// and needs to own all it's childrens locks before being locked.
// Resource connections form a directed acyclic graph
// Resources do not allow any other nodes to take locks from them
type Resource interface {
  GraphNode

  // Called when locking the node to allow for custom lock behaviour
  Lock(node GraphNode, state NodeState) (NodeState, error)
  // Called when unlocking the node to allow for custom lock behaviour
  Unlock(node GraphNode, state NodeState) (NodeState, error)
}

// Resources propagate update up to multiple parents, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (resource * BaseResource) PropagateUpdate(ctx * GraphContext, signal GraphSignal) {
  UseStates(ctx, []GraphNode{resource}, func(states []NodeState) (interface{}, error){
    resource_state := states[0].(ResourceState)
    if signal.Direction() == Up {
      // Child->Parent, resource updates parent resources
      for _, parent := range resource_state.parents {
        SendUpdate(ctx, parent, signal)
      }
    } else if signal.Direction() == Down {
      // Parent->Child, resource updates lock holder
      if resource_state.owner != nil {
        SendUpdate(ctx, resource_state.owner, signal)
      }

      for _, child := range(resource_state.children) {
        SendUpdate(ctx, child, signal)
      }
    } else if signal.Direction() == Direct {
    } else {
      panic(fmt.Sprintf("Invalid signal direction: %d", signal.Direction()))
    }
    return nil, nil
  })
}

func checkIfChild(ctx * GraphContext, r ResourceState, r_id NodeID, cur ResourceState, cur_id NodeID) bool {
  if r_id == cur_id {
    return true
  }

  for _, c := range(cur.children) {
    val, _ := UseStates(ctx, []GraphNode{c}, func(states []NodeState) (interface{}, error) {
      child_state := states[0].(ResourceState)
      return checkIfChild(ctx, cur, cur_id, child_state, c.ID()), nil
    })

    is_child := val.(bool)
    if is_child {
      return true
    }
  }

  return false
}

func UnlockResource(ctx * GraphContext, resource Resource, node GraphNode, node_state NodeState) (NodeState, error) {
  if node == nil || resource == nil{
    panic("Cannot unlock without a specified node and resource")
  }
  _, err := UpdateStates(ctx, []GraphNode{resource}, func(states []NodeState) ([]NodeState, interface{}, error) {
    if resource.ID() == node.ID() {
      if node_state != nil {
        panic("node_state must be nil if unlocking resource from itself")
      }
      node_state = states[0]
    }
    resource_state := states[0].(ResourceState)

    if resource_state.owner == nil {
      return nil, nil, fmt.Errorf("Resource already unlocked")
    }

    if resource_state.owner.ID() != node.ID() {
      return nil, nil, fmt.Errorf("Resource %s not locked by %s", resource.ID(), node.ID())
    }

    var lock_err error = nil
    for _, child := range(resource_state.children) {
      var err error = nil
      node_state, err = UnlockResource(ctx, child, node, node_state)
      if err != nil {
        lock_err = err
        break
      }
    }

    if lock_err != nil {
      return nil, nil, fmt.Errorf("Resource %s failed to unlock: %e", resource.ID(), lock_err)
    }

    resource_state.owner = node_state.OriginalLockHolder(resource.ID())
    unlock_state, err := resource.Unlock(node, resource_state)
    resource_state = unlock_state.(ResourceState)
    if err != nil {
      return nil, nil, fmt.Errorf("Resource %s failed custom Unlock: %e", resource.ID(), err)
    }

    if resource_state.owner == nil {
      ctx.Log.Logf("resource", "RESOURCE_UNLOCK: %s unlocked %s", node.ID(), resource.ID())
    } else {
      ctx.Log.Logf("resource", "RESOURCE_UNLOCK: %s passed lock of %s back to %s", node.ID(), resource.ID(), resource_state.owner.ID())
    }

    return []NodeState{resource_state}, nil, nil
  })

  if err != nil {
    return nil, err
  }

  return node_state, nil
}

// TODO: State
func LockResource(ctx * GraphContext, resource Resource, node GraphNode, node_state NodeState) (NodeState, error) {
  if node == nil || resource == nil {
    panic("Cannot lock without a specified node and resource")
  }

  _, err := UpdateStates(ctx, []GraphNode{resource}, func(states []NodeState) ([]NodeState, interface{}, error) {
    if resource.ID() == node.ID() {
      if node_state != nil {
        panic("node_state must be nil if locking resource from itself")
      }
      node_state = states[0]
    }
    resource_state := states[0].(ResourceState)
    if resource_state.owner != nil {
      var lock_pass_allowed bool = false

      if resource_state.owner.ID() == resource.ID() {
        lock_pass_allowed = resource_state.AllowedToTakeLock(node.ID())
      } else {
        tmp, _ := UseStates(ctx, []GraphNode{resource_state.owner}, func(states []NodeState)(interface{}, error){
          return states[0].AllowedToTakeLock(node.ID()), nil
        })
        lock_pass_allowed = tmp.(bool)
      }


      if lock_pass_allowed == false {
        return nil, nil, fmt.Errorf("%s is not allowed to take a lock from %s", node.ID(), resource_state.owner.ID())
      }
    }

    lock_state, err := resource.Lock(node, resource_state)
    if err != nil {
      return nil, nil, fmt.Errorf("Failed to lock resource: %e", err)
    }

    resource_state = lock_state.(ResourceState)

    var lock_err error = nil
    locked_resources := []Resource{}
    for _, child := range(resource_state.children) {
      node_state, err = LockResource(ctx, child, node, node_state)
      if err != nil {
        lock_err = err
        break
      }
      locked_resources = append(locked_resources, child)
    }

    if lock_err != nil {
      for _, locked_resource := range(locked_resources) {
        node_state, err = UnlockResource(ctx, locked_resource, node, node_state)
        if err != nil {
          panic(err)
        }
      }
      return nil, nil, fmt.Errorf("Resource failed to lock: %e", lock_err)
    }

    old_owner := resource_state.owner
    resource_state.owner = node
    node_state = node_state.RecordLockHolder(node.ID(), old_owner)

    if old_owner == nil {
      ctx.Log.Logf("resource", "RESOURCE_LOCK: %s locked %s", node.ID(), resource.ID())
    } else {
      ctx.Log.Logf("resource", "RESOURCE_LOCK: %s took lock of %s from %s", node.ID(), resource.ID(), old_owner.ID())
    }

    return []NodeState{resource_state}, nil, nil
  })
  if err != nil {
    return nil, err
  }

  return node_state, nil
}

// BaseResources represent simple resources in the DAG that can be used to create a hierarchy of locks that store names
type BaseResource struct {
  BaseNode
}

//BaseResources don't check anything special when locking/unlocking
func (resource * BaseResource) Lock(node GraphNode, state NodeState) (NodeState, error) {
  return state, nil
}

func (resource * BaseResource) Unlock(node GraphNode, state NodeState) (NodeState, error) {
  return state, nil
}

/*func FindResource(root Event, id string) Resource {
  if root == nil || id == ""{
    panic("invalid input")
  }

  for _, resource := range(root.Resources()) {
    if resource.ID() == id {
      return resource
    }
  }
  for _, child := range(root.Children()) {
    resource := FindResource(child, id)
    if resource != nil {
      return resource
    }
  }
  return nil
}*/

func NewResource(ctx * GraphContext, name string, children []Resource) (* BaseResource, error) {
  resource := &BaseResource{
    BaseNode: NewNode(ctx, RandID(), NewResourceState(name)),
  }

  err := LinkResources(ctx, resource, children)
  if err != nil {
    return nil, err
  }

  return resource, nil
}
