package main

import (
  "fmt"
  "log"
  "errors"
  graphql "github.com/graph-gophers/graphql-go"
)

type EventManager struct {
  dag_nodes map[graphql.ID]Resource
  root_event Event
}

// root_event's requirements must be in dag_nodes, and dag_nodes must be ordered by dependency(children first)
func NewEventManager(root_event Event, dag_nodes []Resource) * EventManager {

  manager := &EventManager{
    dag_nodes: map[graphql.ID]Resource{},
    root_event: nil,
  }

  // Construct the DAG
  for _, resource := range dag_nodes {
    err := manager.AddResource(resource)
    if err != nil {
      log.Printf("Failed to add %s to EventManager: %s", resource.Name(), err)
      return nil
    }
  }

  err := manager.AddEvent(nil, root_event, nil)
  if err != nil {
    log.Printf("Failed to add %s to EventManager as root_event: %s", root_event.Name(), err)
  }

  return manager;
}

func (manager * EventManager) Run() error {
  return manager.root_event.Run()
}

func (manager * EventManager) FindResource(id graphql.ID) Resource {
  resource, exists := manager.dag_nodes[id]
  if exists == false {
    return nil
  }

  return resource
}

func (manager * EventManager) FindEvent(id graphql.ID) Event {
  event := manager.root_event.FindChild(id)

  return event
}

func (manager * EventManager) AddResource(resource Resource) error {
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
  for _, child := range resource.Children() {
    child.AddParent(resource)
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
    if manager.root_event.FindChild(parent.ID()) == nil {
      error_str := fmt.Sprintf("Event %s is not present in the event tree, cannot add %s as child", parent.ID(), child.ID())
      return errors.New(error_str)
    }

    if manager.root_event.FindChild(child.ID()) != nil {
      error_str := fmt.Sprintf("Event %s already exists in the event tree, can not add again", child.ID())
      return errors.New(error_str)
    }
    return parent.AddChild(child, info)
  }
}


