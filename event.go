package main

import (
  "fmt"
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
  Lock() error
  Unlock() error
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
  event.signal <- "abort"
  return nil
}

func (queue * EventQueue) Abort() error {
  for _, event := range(queue.Children()) {
    event.Abort()
  }
  queue.signal <- "abort"
  return nil
}

func (event * BaseEvent) Lock() error {
  locked_resources := []Resource{}
  lock_err := false
  for _, resource := range(event.RequiredResources()) {
    err := resource.Lock(event)
    if err != nil {
      lock_err = true
    }
  }

  if lock_err == true {
    for _, resource := range(locked_resources) {
      resource.Unlock(event)
    }
    return errors.New("failed to lock required resources")
  }
  return nil
}

func (event * BaseEvent) Unlock() error {
  return event.DoneResource().Unlock(event)
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
    }

    // Check signals
    select {
    case reason := <-event.abort:
      error_str := fmt.Sprintf("State Machine aborted: %s", reason)
      return errors.New(error_str)
    default:
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

func NewEvent(name string, description string, required_resources []Resource) (* BaseEvent) {
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
    done_resource: done_resource,
    required_resources: required_resources,
    actions: map[string]func()(string, error){},
    signal: make(chan string, 10),
  }

  // Lock the done_resource by default
  done_resource.Lock(event)

  event.actions["start"] = func() (string, error) {
    return "", nil
  }

  return event
}

func NewEventQueue(name string, description string, required_resources []Resource) (* EventQueue) {
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
      done_resource: done_resource,
      required_resources: required_resources,
      actions: map[string]func()(string, error){},
      signal: make(chan string, 10),
      abort: make(chan string, 1),
    },
  }

  // Need to lock it with th BaseEvent since Unlock is implemented on the BaseEvent
  done_resource.Lock(&queue.BaseEvent)

  queue.actions["start"] = func() (string, error) {
    return "queue_event", nil
  }

  queue.actions["queue_event"] = func() (string, error) {
    // Sort the list of events by priority
    // Keep trying to lock the highest priority event until the end of the list is reached, or an event is locked
    // If an event is locked, transition it to "started" and start event in a new goroutine
    // If the end of the queue is reached and there are no uncompleted events, transition to "done"
    // If the end of the queue is reached and there are uncompleted events, transition to "wait"
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
        err := event.Lock()
        // start in new goroutine
        if err != nil {

        } else {
          info.state = "running"
          go func(event Event, info * EventQueueInfo, queue * EventQueue) {
            event.Run()
            info.state = "done"
            queue.signal <- "event_done"
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

  queue.actions["wait"] = func() (string, error) {
    // Wait until signaled by a thread
    /*
      What signals to take action for:
       - abort : sent by any other thread : abort any child events and set the next event to none
       - resource_available : sent by the aggregator goroutine when the lock on a resource changes : see if any events can be locked
       - event_done : sent by child event threads : see if all events are completed
    */
    signal := <- queue.signal
    if signal == "abort" {
      queue.abort <- "aborted by signal"
      return "", nil
    }
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
