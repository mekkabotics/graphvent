package main

import (
  "fmt"
  "errors"
)

type EventManager struct {
  dag_nodes map[string]Resource
  root_event Event
  aborts []chan error
}

// root_event's requirements must be in dag_nodes, and dag_nodes must be ordered by dependency(children first)
func NewEventManager(root_event Event, dag_nodes []Resource) * EventManager {

  manager := &EventManager{
    dag_nodes: map[string]Resource{},
    root_event: nil,
    aborts: []chan error{},
  }

  // Construct the DAG
  for _, resource := range dag_nodes {
    err := manager.AddResource(resource)
    if err != nil {
      log.Logf("manager", "Failed to add %s to EventManager: %s", resource.Name(), err)
      return nil
    }
  }

  err := manager.AddEvent(nil, root_event, nil)
  if err != nil {
    log.Logf("manager", "Failed to add %s to EventManager as root_event: %s", root_event.Name(), err)
  }

  return manager;
}

// Connect to all resources(in a thread to handle reconnections), and start the first event
func (manager * EventManager) Run() error {
  log.Logf("manager", "MANAGER_START")

  abort := make(chan error, 1)
  go func(abort chan error, manager * EventManager) {
    <- abort
    for _, c := range(manager.aborts) {
      c <- nil
    }
  }(abort, manager)

  err := LockResources(manager.root_event)
  if err != nil {
    log.Logf("manager", "MANAGER_LOCK_ERR: %s", err)
    abort <- nil
    return err
  }

  err = RunEvent(manager.root_event)
  abort <- nil
  if err != nil {
    log.Logf("manager", "MANAGER_RUN_ERR: %s", err)
    return err
  }

  err = FinishEvent(manager.root_event)
  if err != nil {
    log.Logf("manager", "MANAGER_FINISH_ERR: %s", err)
    return err
  }
  log.Logf("manager", "MANAGER_DONE")

  return nil
}

func (manager * EventManager) FindResource(id string) Resource {
  resource, exists := manager.dag_nodes[id]
  if exists == false {
    return nil
  }

  return resource
}

func (manager * EventManager) FindEvent(id string) Event {
  event := FindChild(manager.root_event, id)

  return event
}

func (manager * EventManager) AddResource(resource Resource) error {
  log.Logf("manager", "Adding resource %s", resource.Name())
  _, exists := manager.dag_nodes[resource.ID()]
  if exists == true {
    error_str := fmt.Sprintf("%s is already in the resource DAG, cannot add again", resource.Name())
    return errors.New(error_str)
  }

  for _, child := range resource.Children() {
    _, exists := manager.dag_nodes[child.ID()]
    if exists == false {
      error_str := fmt.Sprintf("%s is not in the resource DAG, cannot add %s to DAG", child.Name(), resource.Name())
      return errors.New(error_str)
    }
  }
  manager.dag_nodes[resource.ID()] = resource
  abort := make(chan error, 1)
  abort_used := resource.Connect(abort)
  if abort_used == true {
    manager.aborts = append(manager.aborts, abort)
  }
  for _, child := range resource.Children() {
    AddParent(child, resource)
  }
  return nil
}

// Check that the node doesn't already exist in the tree
// Check the the selected parent exists in the tree
// Check that required resources exist in the DAG
// Check that created resources don't exist in the DAG
// Add resources created by the event to the DAG
// Add child to parent
func (manager * EventManager) AddEvent(parent Event, child Event, info EventInfo) error {
  if child == nil {
    return errors.New("Cannot add nil Event to EventManager")
  } else if len(child.Children()) != 0 {
    return errors.New("Adding events recursively not implemented")
  }

  for _, resource := range child.RequiredResources() {
    _, exists := manager.dag_nodes[resource.ID()]
    if exists == false {
      error_str := fmt.Sprintf("Required resource %s not in DAG, cannot add event %s", resource.ID(), child.ID())
      return errors.New(error_str)
    }
  }

  resource := child.DoneResource()
  _, exists := manager.dag_nodes[resource.ID()]
  if exists == true {
    error_str := fmt.Sprintf("Created resource %s already exists in DAG, cannot add event %s", resource.ID(), child.ID())
    return errors.New(error_str)
  }
  manager.AddResource(resource)

  if manager.root_event == nil && parent != nil {
    error_str := fmt.Sprintf("EventManager has no root, so can't add event to parent")
    return errors.New(error_str)
  } else if manager.root_event != nil && parent == nil {
    // TODO
    return errors.New("Replacing root event not implemented")
  } else if manager.root_event == nil && parent == nil {
    manager.root_event = child
    return nil;
  } else {
    if FindChild(manager.root_event, parent.ID()) == nil {
      error_str := fmt.Sprintf("Event %s is not present in the event tree, cannot add %s as child", parent.ID(), child.ID())
      return errors.New(error_str)
    }

    if FindChild(manager.root_event, child.ID()) != nil {
      error_str := fmt.Sprintf("Event %s already exists in the event tree, can not add again", child.ID())
      return errors.New(error_str)
    }
    return AddChild(parent, child, info)
  }
}


