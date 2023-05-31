package main

import (
  "fmt"
  "log"
  "errors"
  graphql "github.com/graph-gophers/graphql-go"
  "reflect"
  "sort"
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
  DoneResource() Resource
  AddChild(child Event, info EventInfo) error
  FindChild(id graphql.ID) Event
  Run() error
  Abort() error
  Signal(action string) error
  LockResources() error
  Finish() error
}

// BaseEvent is the most basic event that can exist in the event tree.
// On start it automatically transitions to completion.
// It can optionally require events, which will all need to be locked to start it
// It can optionally create resources, which will be locked by default and unlocked on completion
// This node by itself doesn't implement any special behaviours for children, so they will be ignored.
// When starter, this event automatically transitions to completion and unlocks all it's resources(including created)
type BaseEvent struct {
  BaseNode
  done_resource Resource
  required_resources []Resource
  children []Event
  child_info map[Event]EventInfo
  actions map[string]func() (string, error)
  parent Event
  signal chan string
  abort chan string
}

func (event * BaseEvent) Abort() error {
  for _, event := range(event.Children()) {
    event.Abort()
  }
  event.signal <- "abort"
  return nil
}

func (event * BaseEvent) Signal(action string) error {
  event.signal <- action
  return nil
}

func (event * BaseEvent) LockResources() error {
  locked_resources := []Resource{}
  lock_err := false
  for _, resource := range(event.RequiredResources()) {
    err := resource.Lock(event)
    if err != nil {
      lock_err = true
    }
    locked_resources = append(locked_resources, resource)
  }

  if lock_err == true {
    for _, resource := range(locked_resources) {
      resource.Unlock(event)
    }
    return errors.New("failed to lock required resources")
  }
  return nil
}

func (event * BaseEvent) Finish() error {
  for _, resource := range(event.RequiredResources()) {
    err := resource.Unlock(event)
    if err != nil {
      panic(err)
    }
  }
  return event.DoneResource().Unlock(event)
}

func (event * BaseEvent) LockDone() {
  event.DoneResource().Lock(event)
}

func (event * BaseEvent) Run() error {
  next_action := "start"
  var err error = nil
  for next_action != "" {
    // Check if the edge exists
    action, exists := event.actions[next_action]
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    // Run the edge function
    next_action, err = action()
    if err != nil {
      return err
    } else if next_action == "wait" {
      // Wait for an external signal to set the next_action
      signal := <- event.signal
      if signal == "abort" {
        return errors.New("State Machine aborted by signal")
      } else {
        next_action = signal
      }
    } else {
      // next_action is already set correctly
    }

    // Update the event after running the edge
    event.Update()
  }

  err = event.DoneResource().Unlock(event)
  if err != nil {
    return err
  }
  return nil
}

// EventQueue is a basic event that can have children.
// On start, it attempts to start it's children from the highest 'priority'
type EventQueue struct {
  BaseEvent
}

func NewBaseEvent(name string, description string, required_resources []Resource) (BaseEvent) {
  done_resource := NewResource("event_done", "signal that event is done", []Resource{})
  event := BaseEvent{
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: gql_randid(),
      listeners: []chan error{},
    },
    parent: nil,
    children: []Event{},
    child_info: map[Event]EventInfo{},
    done_resource: done_resource,
    required_resources: required_resources,
    actions: map[string]func()(string, error){},
    signal: make(chan string, 10),
    abort: make(chan string, 1),
  }

  return event
}

func NewEvent(name string, description string, required_resources []Resource) (* BaseEvent) {
  event := NewBaseEvent(name, description, required_resources)
  event_ptr := &event

  // Lock the done_resource by default
  event.LockDone()

  event_ptr.actions["start"] = func() (string, error) {
    return "", nil
  }

  return event_ptr
}

func NewEventQueue(name string, description string, required_resources []Resource) (* EventQueue) {
  queue := &EventQueue{
    BaseEvent: NewBaseEvent(name, description, []Resource{}),
  }

  // Need to lock it with th BaseEvent since Unlock is implemented on the BaseEvent
  queue.LockDone()

  queue.actions["start"] = func() (string, error) {
    log.Printf("Starting Event Queue")
    return "queue_event", nil
  }

  queue.actions["queue_event"] = func() (string, error) {
    log.Printf("Queueing events")
    // Copy the events to sort the list
    copied_events := make([]Event, len(queue.Children()))
    copy(copied_events, queue.Children())
    less := func(i int, j int) bool {
      info_i := queue.ChildInfo(copied_events[i]).(*EventQueueInfo)
      info_j := queue.ChildInfo(copied_events[j]).(*EventQueueInfo)
      return info_i.priority < info_j.priority
    }
    sort.SliceStable(copied_events, less)

    wait := false
    for _, event := range(copied_events) {
      info := queue.ChildInfo(event).(*EventQueueInfo)
      if info.state == "queued" {
        wait = true
        // Try to lock it
        err := event.LockResources()
        // start in new goroutine
        if err != nil {

        } else {
          info.state = "running"
          go func(event Event, info * EventQueueInfo, queue Event) {
            event.Run()
            info.state = "done"
            event.Finish()
            queue.Signal("event_done")
          }(event, info, queue)
        }
      } else if info.state == "running" {
        wait = true
      }
    }

    if wait == true {
      return "wait", nil
    } else {
      return "", nil
    }
  }

  queue.actions["event_done"] = func() (string, error) {
    log.Printf("event_done")
    return "queue_event", nil
  }

  queue.actions["resource_available"] = func() (string, error) {
    log.Printf("resources_available")
    return "queue_event", nil
  }

  queue.actions["event_added"] = func() (string, error) {
    log.Printf("event_added")
    return "queue_event", nil
  }

  return queue
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

func (event * BaseEvent) DoneResource() Resource {
  return event.done_resource
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
