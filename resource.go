package main

import (
  "fmt"
  "errors"
  "sync"
)

// Resources propagate update up to multiple parents, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (resource * BaseResource) update(signal GraphSignal) {
  new_signal := signal.Trace(resource.ID())
  if signal.Type() == "lock_changed" {
    for _, child := range resource.Children() {
      SendUpdate(child, new_signal)
    }
  } else {
    for _, parent := range resource.Parents() {
      SendUpdate(parent, new_signal)
    }
    if resource.lock_holder != nil {
      if resource.lock_holder.ID() != signal.Last() {
        SendUpdate(resource.lock_holder, new_signal)
      }
    }
  }
}

// Resource is the interface that DAG nodes are made from
// A resource needs to be able to represent logical entities and connections to physical entities.
// A resource lock could be aborted at any time if this connection is broken, if that happens the event locking it must be aborted
// The device connection should be maintained as much as possible(requiring some reconnection behaviour in the background)
type Resource interface {
  GraphNode
  Owner() Event
  Children() []Resource
  Parents() []Resource

  AddParent(parent Resource) error
  LockParents()
  UnlockParents()

  SetOwner(owner Event)
  LockState()
  UnlockState()

  lock(event Event) error
  unlock(event Event) error
  Connect(abort chan error) bool
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
  if resource.Owner() == nil {
    resource.UnlockState()
    return errors.New("Resource already unlocked")
  }

  if resource.Owner().ID() != event.ID() {
    resource.UnlockState()
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
    resource.UnlockState()
    err_str := fmt.Sprintf("Resource failed to unlock: %s", lock_err)
    return errors.New(err_str)
  }

  resource.SetOwner(nil)

  err = resource.unlock(event)
  if err != nil {
    resource.UnlockState()
    return errors.New("Failed to unlock resource")
  }

  resource.UnlockState()
  return nil
}

func LockResource(resource Resource, event Event) error {
  resource.LockState()
  if resource.Owner() != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Resource already locked: %s", resource.Name())
    return errors.New(err_str)
  }

  err := resource.lock(event)
  if err != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Failed to lock resource: %s", err)
    return errors.New(err_str)
  }

  var lock_err error = nil
  locked_resources := []Resource{}
  for _, child := range resource.Children() {
    err := LockResource(child, event)
    if err != nil{
      lock_err = err
      break
    }
    locked_resources = append(locked_resources, child)
  }

  if lock_err != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Resource failed to lock: %s", lock_err)
    return errors.New(err_str)
  }

  resource.SetOwner(event)


  resource.UnlockState()
  return nil
}

func NotifyResourceLocked(resource Resource) {
  signal := NewSignal(resource, "lock_changed")
  signal.description = "lock"

  go SendUpdate(resource, signal)
}

func NotifyResourceUnlocked(resource Resource) {
  signal := NewSignal(resource, "lock_changed")
  signal.description = "unlock"

  go SendUpdate(resource, signal)
}

// BaseResource is the most basic resource that can exist in the DAG
// It holds a single state variable, which contains a pointer to the event that is locking it
type BaseResource struct {
  BaseNode
  parents []Resource
  parents_lock sync.Mutex
  children []Resource
  children_lock sync.Mutex
  lock_holder Event
  state_lock sync.Mutex
}

func (resource * BaseResource) SetOwner(owner Event) {
  resource.lock_holder = owner
}

func (resource * BaseResource) LockState() {
  resource.state_lock.Lock()
}

func (resource * BaseResource) UnlockState() {
  resource.state_lock.Unlock()
}

func (resource * BaseResource) Connect(abort chan error) bool {
  return false
}

func (resource * BaseResource) Owner() Event {
  return resource.lock_holder
}

//BaseResources don't check anything special when locking/unlocking
func (resource * BaseResource) lock(event Event) error {
  return nil
}

func (resource * BaseResource) unlock(event Event) error {
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
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: randid(),
      listeners: map[chan GraphSignal]chan GraphSignal{},
      signal: make(chan GraphSignal, 100),
    },
    parents: []Resource{},
    children: children,
  }

  return resource
}

func NewResource(name string, description string, children []Resource) * BaseResource {
  resource := NewBaseResource(name, description, children)
  return &resource
}
