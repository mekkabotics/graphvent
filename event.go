package main

import (
  "fmt"
  "errors"
  graphql "github.com/graph-gophers/graphql-go"
  "reflect"
)

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

type EventInfo interface {
}

type BaseEventInfo interface {
  EventInfo
}

type EventQueueInfo struct {
  EventInfo
  priority int
  state string
}

func NewEventQueueInfo(priority int) * EventQueueInfo {
  info := &EventQueueInfo{
    priority: priority,
    state: "queued",
  }

  return info
}

// Event is the interface that event tree nodes must implement
type Event interface {
  GraphNode
  Children() []Event
  ChildInfo(event Event) EventInfo
  Parent() Event
  RegisterParent(parent Event) error
  RequiredResources() []Resource
  CreatedResources() []Resource
  AddChild(child Event, info EventInfo) error
  FindChild(id graphql.ID) Event
}

// BaseEvent is the most basic event that can exist in the event tree.
// On start it automatically transitions to completion.
// It can optionally require events, which will all need to be locked to start it
// It can optionally create resources, which will be locked by default and unlocked on completion
// This node by itself doesn't implement any special behaviours for children, so they will be ignored.
// When starter, this event automatically transitions to completion and unlocks all it's resources(including created)
type BaseEvent struct {
  BaseNode
  created_resources []Resource
  required_resources []Resource
  children []Event
  child_info map[Event]EventInfo
  parent Event
}

// EventQueue is a basic event that can have children.
// On start, it attempts to start it's children from the highest 'priority'
type EventQueue struct {
  BaseEvent
}

func NewEvent(name string, description string, required_resources []Resource) (* BaseEvent, Resource) {
  done_resource := NewResource("event_done", "signal that event is done", []Resource{})
  event := &BaseEvent{
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: gql_randid(),
      listeners: []chan error{},
    },
    parent: nil,
    children: []Event{},
    child_info: map[Event]EventInfo{},
    created_resources: []Resource{done_resource},
    required_resources: required_resources,
  }

  // Lock the done_resource by default
  done_resource.Lock(event)

  return event, done_resource
}

func NewEventQueue(name string, description string, required_resources []Resource) (* EventQueue, Resource) {
  done_resource := NewResource("event_done", "signal that event is done", []Resource{})
  queue := &EventQueue{
    BaseEvent: BaseEvent{
      BaseNode: BaseNode{
        name: name,
        description: description,
        id: gql_randid(),
        listeners: []chan error{},
      },
      parent: nil,
      children: []Event{},
      child_info: map[Event]EventInfo{},
      created_resources: []Resource{done_resource},
      required_resources: required_resources,
    },
  }

  done_resource.Lock(queue)

  return queue, done_resource
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

func (event * BaseEvent) ChildInfo(idx Event) EventInfo {
  val, ok := event.child_info[idx]
  if ok == false {
    return nil
  }
  return val
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

// Checks that the type of info is equal to EventQueueInfo
func (event * EventQueue) AddChild(child Event, info EventInfo) error {
  if checkType(info, (*EventQueueInfo)(nil)) == false {
    return errors.New("EventQueue.AddChild passed invalid type for info")
  }

  return event.addChild(child, info)
}

func (event * BaseEvent) addChild(child Event, info EventInfo) error {
  err := child.RegisterParent(event)
  if err != nil {
    error_str := fmt.Sprintf("Failed to register %s as a parent of %s, cancelling AddChild", event.ID(), child.ID())
    return errors.New(error_str)
  }

  event.children = append(event.children, child)
  event.child_info[child] = info
  event.Update()
  return nil
}

// Overloaded function AddChild checks the info passed and calls the BaseEvent.addChild
func (event * BaseEvent) AddChild(child Event, info EventInfo) error {
  if info != nil {
    return errors.New("info must be nil for BaseEvent children")
  }

  return event.addChild(child, info)
}

func checkType(first interface{}, second interface{}) bool {
  if first == nil || second == nil {
    if first == nil && second == nil {
      return true
    } else {
      return false
    }
  }

  first_type := reflect.TypeOf(first)
  second_type := reflect.TypeOf(second)

  return first_type == second_type
}
