package main

import (
  "fmt"
  "errors"
  "sync"
  "log"
)

// Resources propagate update up to multiple parents, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (resource * BaseResource) Update(reason string) error {
  log.Printf("UPDATE BaseResource %s: %s", resource.Name(), reason)
  err := resource.UpdateListeners(reason)
  if err != nil {
    return err
  }

  for _, parent := range resource.Parents() {
    err := parent.Update("update parents")
    if err != nil {
      return err
    }
  }

  return nil
}

// Resource is the interface that DAG nodes are made from
// A resource needs to be able to represent logical entities and connections to physical entities.
// A resource lock could be aborted at any time if this connection is broken, if that happens the event locking it must be aborted
// The device connection should be maintained as much as possible(requiring some reconnection behaviour in the background)
type Resource interface {
  GraphNode
  AddParent(parent Resource) error
  Children() []Resource
  Parents() []Resource
  Lock(event Event) error
  NotifyLocked() error
  Unlock(event Event) error
  Owner() Event
  Connect(abort chan error) bool
}

// BaseResource is the most basic resource that can exist in the DAG
// It holds a single state variable, which contains a pointer to the event that is locking it
type BaseResource struct {
  BaseNode
  parents []Resource
  children []Resource
  lock_holder Event
  state_lock sync.Mutex
}

func (resource * BaseResource) Connect(abort chan error) bool {
  return false
}

func (resource * BaseResource) Owner() Event {
  return resource.lock_holder
}

func (resource * BaseResource) NotifyLocked() error {
  err := resource.Update("finalize_lock")
  if err != nil {
    return err
  }

  for _, child := range(resource.children) {
    err = child.NotifyLocked()
    if err != nil {
      return err
    }
  }

  return nil
}

// Grab the state mutex and check the state, if unlocked continue to hold the mutex while doing the same for children
// When the bottom of a tree is reached(no more children) go back up and set the lock state
func (resource * BaseResource) Lock(event Event) error {
  return resource.lock(event)
}

func (resource * BaseResource) lock(event Event) error {
  var err error = nil

  resource.state_lock.Lock()
  if resource.lock_holder != nil {
    err_str := fmt.Sprintf("Resource already locked: %s", resource.Name())
    err = errors.New(err_str)
  } else {
    all_children_locked := true
    for _, child := range resource.Children() {
      err = child.Lock(event)
      if err != nil {
        all_children_locked = false
        break
      }
    }
    if all_children_locked == true {
      resource.lock_holder = event
    }
  }
  resource.state_lock.Unlock()

  return err
}

// Recurse through children, unlocking until no more children
// If the child isn't locked by the unlocker
func (resource * BaseResource) Unlock(event Event) error {
  var err error = nil
  //unlocked := false

  resource.state_lock.Lock()
  if resource.lock_holder == nil {
    err = errors.New("Resource already unlocked")
  } else if resource.lock_holder != event {
    err = errors.New("Resource not locked by parent, can't unlock")
  } else {
    all_children_unlocked := true
    for _, child := range resource.Children() {
      err = child.Unlock(event)
      if err != nil {
        all_children_unlocked = false
        break
      }
    }
    if all_children_unlocked == true{
      resource.lock_holder = nil
      //unlocked = true
    }
  }
  resource.state_lock.Unlock()

  /*if unlocked == true {
    resource.Update("unlocking resource")
  }*/

  return err
}

func (resource * BaseResource) Children() []Resource {
  return resource.children
}

func (resource * BaseResource) Parents() []Resource {
  return resource.parents
}

// Add a parent to a DAG node
func (resource * BaseResource) AddParent(parent Resource) error {
  // Don't add self as parent
  if parent.ID() == resource.ID() {
    error_str := fmt.Sprintf("Will not add %s as parent of itself", parent.ID())
    return errors.New(error_str)
  }

  // Don't add parent if it's already a parent
  for _, p := range resource.parents {
    if p.ID() == parent.ID() {
      error_str := fmt.Sprintf("%s is already a parent of %s, will not double-bond", p.ID(), resource.ID())
      return errors.New(error_str)
    }
  }

  // Add the parent
  resource.parents = append(resource.parents, parent)
  return nil
}

func NewResource(name string, description string, children []Resource) * BaseResource {
  resource := &BaseResource{
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: randid(),
      listeners: []chan string{},
    },
    parents: []Resource{},
    children: children,
  }

  return resource
}
