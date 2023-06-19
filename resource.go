package graphvent

import (
  "fmt"
  "sync"
  "errors"
)

// Resources propagate update up to multiple parents, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (resource * BaseResource) update(signal GraphSignal) {

  if signal.Downwards() == false {
    // Child->Parent, resource updates parent resources
    resource.parents_lock.Lock()
    defer resource.parents_lock.Unlock()
    for _, parent := range resource.Parents() {
      SendUpdate(parent, signal)
    }
  } else {
    // Parent->Child, resource updates lock holder
    resource.lock_holder_lock.Lock()
    defer resource.lock_holder_lock.Unlock()
    if resource.lock_holder != nil {
      SendUpdate(resource.lock_holder, signal)
    }

    resource.children_lock.Lock()
    defer resource.children_lock.Unlock()
    for _, child := range(resource.children) {
      SendUpdate(child, signal)
    }
  }
}

// Resource is the interface that DAG nodes are made from
// A resource needs to be able to represent logical entities and connections to physical entities.
// A resource lock could be aborted at any time if this connection is broken, if that happens the event locking it must be aborted
// The device connection should be maintained as much as possible(requiring some reconnection behaviour in the background)
type Resource interface {
  GraphNode
  Owner() GraphNode
  Children() []Resource
  Parents() []Resource

  AddParent(parent Resource) error
  LockParents()
  UnlockParents()

  SetOwner(owner GraphNode)
  LockState()
  UnlockState()

  Init(abort chan error) bool
  lock(node GraphNode) error
  unlock(node GraphNode) error
}

func AddParent(resource Resource, parent Resource) error {
  if parent.ID() == resource.ID() {
    error_str := fmt.Sprintf("Will not add %s as parent of itself", parent.Name())
    return errors.New(error_str)
  }

  resource.LockParents()
  for _, p := range resource.Parents() {
    if p.ID() == parent.ID() {
      error_str := fmt.Sprintf("%s is already a parent of %s, will not double-bond", p.Name(), resource.Name())
      return errors.New(error_str)
    }
  }

  err := resource.AddParent(parent)
  resource.UnlockParents()

  return err
}

func UnlockResource(resource Resource, event Event) error {
  var err error = nil
  resource.LockState()
  defer resource.UnlockState()
  if resource.Owner() == nil {
    return errors.New("Resource already unlocked")
  }

  if resource.Owner().ID() != event.ID() {
    return errors.New("Resource not locked by parent, unlock failed")
  }

  var lock_err error = nil
  for _, child := range resource.Children() {
    err := UnlockResource(child, event)
    if err != nil {
      lock_err = err
      break
    }
  }

  if lock_err != nil {
    return fmt.Errorf("Resource failed to unlock: %s", lock_err)
  }

  resource.SetOwner(nil)

  err = resource.unlock(event)
  if err != nil {
    return errors.New("Failed to unlock resource")
  }

  return nil
}

func LockResource(resource Resource, node GraphNode) error {
  resource.LockState()
  defer resource.UnlockState()
  if resource.Owner() != nil {
    return fmt.Errorf("Resource already locked: %s", resource.Name())
  }

  err := resource.lock(node)
  if err != nil {
    return fmt.Errorf("Failed to lock resource: %s", err)
  }

  var lock_err error = nil
  locked_resources := []Resource{}
  for _, child := range resource.Children() {
    err := LockResource(child, node)
    if err != nil{
      lock_err = err
      break
    }
    locked_resources = append(locked_resources, child)
  }

  if lock_err != nil {
    return fmt.Errorf("Resource failed to lock: %s", lock_err)
  }

  log.Logf("resource", "Locked %s", resource.Name())
  resource.SetOwner(node)

  return nil
}

// BaseResource is the most basic resource that can exist in the DAG
// It holds a single state variable, which contains a pointer to the event that is locking it
type BaseResource struct {
  BaseNode
  parents []Resource
  parents_lock sync.Mutex
  children []Resource
  children_lock sync.Mutex
  lock_holder GraphNode
  lock_holder_lock sync.Mutex
  state_lock sync.Mutex
}

func (resource * BaseResource) SetOwner(owner GraphNode) {
  resource.lock_holder_lock.Lock()
  resource.lock_holder = owner
  resource.lock_holder_lock.Unlock()
}

func (resource * BaseResource) LockState() {
  resource.state_lock.Lock()
}

func (resource * BaseResource) UnlockState() {
  resource.state_lock.Unlock()
}

func (resource * BaseResource) Init(abort chan error) bool {
  return false
}

func (resource * BaseResource) Owner() GraphNode {
  return resource.lock_holder
}

//BaseResources don't check anything special when locking/unlocking
func (resource * BaseResource) lock(node GraphNode) error {
  return nil
}

func (resource * BaseResource) unlock(node GraphNode) error {
  return nil
}

func (resource * BaseResource) Children() []Resource {
  return resource.children
}

func (resource * BaseResource) Parents() []Resource {
  return resource.parents
}

func (resource * BaseResource) LockParents() {
  resource.parents_lock.Lock()
}

func (resource * BaseResource) UnlockParents() {
  resource.parents_lock.Unlock()
}

func (resource * BaseResource) AddParent(parent Resource) error {
  resource.parents = append(resource.parents, parent)
  return nil
}

func NewBaseResource(name string, description string, children []Resource) BaseResource {
  resource := BaseResource{
    BaseNode: NewBaseNode(name, description, randid()),
    parents: []Resource{},
    children: children,
  }

  return resource
}

func NewResource(name string, description string, children []Resource) * BaseResource {
  resource := NewBaseResource(name, description, children)
  return &resource
}
