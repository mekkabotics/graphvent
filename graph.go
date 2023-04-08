package main

import (
  "fmt"
  "errors"
  "sync"
  graphql "github.com/graph-gophers/graphql-go"
  "github.com/google/uuid"
)

func gql_randid() graphql.ID{
  uuid_str := uuid.New().String()
  return graphql.ID(uuid_str)
}

type GraphNode interface {
  Name() string
  Description() string
  ID() graphql.ID
  UpdateListeners() error
  UpdateChannel() chan error
  Update() error
}

type BaseNode struct {
  name string
  description string
  id graphql.ID
  listeners []chan error
  listeners_lock sync.Mutex
}

func (node * BaseNode) Name() string {
  return node.name
}

func (node * BaseNode) Description() string {
  return node.description
}

func (node * BaseNode) ID() graphql.ID {
  return node.id
}

// Create a new listener channel for the node, add it to the nodes listener list, and return the new channel
func (node * BaseNode) UpdateChannel() chan error {
  new_listener := make(chan error, 1)
  node.listeners_lock.Lock()
  node.listeners = append(node.listeners, new_listener)
  node.listeners_lock.Unlock()
  return new_listener
}

// Send the update to listener channels
func (node * BaseNode) UpdateListeners() error {
  closed_listeners := []int{}
  listeners_closed := false

  // Send each listener nil to signal it to check for new content
  // if the first attempt to send it fails close the listener
  node.listeners_lock.Lock()
  for i, listener := range node.listeners {
    select {
      case listener <- nil:
      default:
        close(listener)
        closed_listeners = append(closed_listeners, i)
        listeners_closed = true
    }
  }

  // If any listeners have been closed, loop over the listeners
  // Add listeners to the "remaining" list if i insn't in closed_listeners
  if listeners_closed == true {
    remaining_listeners := []chan error{}
    for i, listener := range node.listeners {
      listener_closed := false
      for _, index := range closed_listeners {
        if index == i {
          listener_closed = true
          break
        }
      }
      if listener_closed == false {
        remaining_listeners = append(remaining_listeners, listener)
      }
    }

    node.listeners = remaining_listeners
  }
  node.listeners_lock.Unlock()

  return nil
}

func (node * BaseNode) Update() error {
  return node.UpdateListeners()
}

// Resources propagate update up to multiple parents, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (resource * BaseResource) Update() error {
  err := resource.UpdateListeners()
  if err != nil {
    return err
  }

  for _, parent := range resource.Parents() {
    err := parent.Update()
    if err != nil {
      return err
    }
  }

  return nil
}

// Update the events listeners, and notify the parent to do the same
func (event * BaseEvent) Update() error {
  err := event.UpdateListeners()
  if err != nil {
    return err
  }

  if event.parent != nil{
    return event.parent.Update()
  }
  return nil
}

type Resource interface {
  GraphNode
  AddParent(parent Resource) error
  Children() []Resource
  Parents() []Resource
}

type BaseResource struct {
  BaseNode
  parents []Resource
  children []Resource
}

func (resource * BaseResource) Children() []Resource {
  return resource.children
}

func (resource * BaseResource) Parents() []Resource {
  return resource.parents
}

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

type Event interface {
  GraphNode
  Children() []Event
  Parent() Event
  RegisterParent(parent Event) error
  RequiredResources() []Resource
  CreatedResources() []Resource
  AddChild(child Event) error
  FindChild(id graphql.ID) Event
}

type BaseEvent struct {
  BaseNode
  locked_resources []Resource
  created_resources []Resource
  required_resources []Resource
  children []Event
  parent Event
}

func NewResource(name string, description string, children []Resource) * BaseResource {
  resource := &BaseResource{
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: gql_randid(),
      listeners: []chan error{},
    },
    parents: []Resource{},
    children: children,
  }

  return resource
}

func NewEvent(name string, description string, required_resources []Resource) * BaseEvent {
  event := &BaseEvent{
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: gql_randid(),
      listeners: []chan error{},
    },
    parent: nil,
    children: []Event{},
    locked_resources: []Resource{},
    created_resources: []Resource{},
    required_resources: required_resources,
  }

  return event
}

// Store the nodes parent for upwards propagation of changes
func (event * BaseEvent) RegisterParent(parent Event) error{
  if event.parent != nil {
    return errors.New("Parent already registered")
  }

  event.parent = parent
  return nil
}

func (event * BaseEvent) Parent() Event {
  return event.parent
}

func (event * BaseEvent) RequiredResources() []Resource {
  return event.required_resources
}

func (event * BaseEvent) CreatedResources() []Resource {
  return event.created_resources
}

func (event * BaseEvent) Children() []Event {
  return event.children
}

func (event * BaseEvent) FindChild(id graphql.ID) Event {
  if id == event.ID() {
    return event
  }

  for _, child := range event.Children() {
    result := child.FindChild(id)
    if result != nil {
      return result
    }
  }

  return nil
}

func (event * BaseEvent) AddChild(child Event) error {
  err := child.RegisterParent(event)
  if err != nil {
    error_str := fmt.Sprintf("Failed to register %s as a parent of %s, cancelling AddChild", event.ID(), child.ID())
    return errors.New(error_str)
  }

  event.children = append(event.children, child)
  return nil
}

type EventManager struct {
  dag_nodes map[graphql.ID]Resource
  root_event Event
}

func NewEventManager() * EventManager {
  state := &EventManager{
    dag_nodes: map[graphql.ID]Resource{},
    root_event: nil,
  }
  return state;
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
    error_str := fmt.Sprintf("%s is already in the resource DAG, cannot add again", resource.ID())
    return errors.New(error_str)
  }

  for _, child := range resource.Children() {
    _, exists := manager.dag_nodes[child.ID()]
    if exists == false {
      error_str := fmt.Sprintf("%s is not in the resource DAG, cannot add %s to DAG", child.ID(), resource.ID())
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
func (manager * EventManager) AddEvent(parent Event, child Event) error {
  if manager.root_event.FindChild(child.ID()) != nil {
    error_str := fmt.Sprintf("Event %s already exists in the event tree, can not add again", child.ID())
    return errors.New(error_str)
  }

  if manager.root_event.FindChild(parent.ID()) == nil {
    error_str := fmt.Sprintf("Event %s is not present in the event tree, cannot add %s as child", parent.ID(), child.ID())
    return errors.New(error_str)
  }

  for _, resource := range child.RequiredResources() {
    _, exists := manager.dag_nodes[resource.ID()]
    if exists == false {
      error_str := fmt.Sprintf("Required resource %s not in DAG, cannot add event %s", resource.ID(), child.ID())
      return errors.New(error_str)
    }
  }

  for _, resource := range child.CreatedResources() {
    _, exists := manager.dag_nodes[resource.ID()]
    if exists == true {
      error_str := fmt.Sprintf("Created resource %s already exists in DAG, cannot add event %s", resource.ID(), child.ID())
      return errors.New(error_str)
    }
  }

  parent.AddChild(child)
  return nil
}


