package main

import (
  "fmt"
  "log"
  "errors"
  "reflect"
  "sort"
  "sync"
)

// Update the events listeners, and notify the parent to do the same
func (event * BaseEvent) update(signal GraphSignal) {
  event.signal <- signal

  if event.parent != nil && signal.Type() != "abort"{
    event.parent.update(signal)
  } else if signal.Type() == "abort" {
    for _, child := range(event.Children()) {
      child.update(signal)
    }
  }
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
  LockChildren()
  UnlockChildren()
  InfoType() reflect.Type
  ChildInfo(event Event) EventInfo
  Parent() Event
  LockParent()
  UnlockParent()
  Action(action string) (func()(string, error), bool)
  Handler(signal_type string) (func() (string, error), bool)
  RequiredResources() []Resource
  DoneResource() Resource

  finish() error

  addChild(child Event, info EventInfo)
  setParent(parent Event)
}

func (event * BaseEvent) Handler(signal_type string) (func()(string, error), bool) {
  handler, exists := event.handlers[signal_type]
  return handler, exists
}

func FindChild(event Event, id string) Event {
  if id == event.ID() {
    return event
  }

  for _, child := range event.Children() {
    result := FindChild(child, id)
    if result != nil {
      return result
    }
  }

  return nil
}

func CheckInfoType(event Event, info EventInfo) bool {
  if event.InfoType() == nil || info == nil {
    if event.InfoType() == nil && info == nil {
      return true
    } else {
      return false
    }
  }

  return event.InfoType() == reflect.TypeOf(info)
}

func AddChild(event Event, child Event, info EventInfo) error {
  if CheckInfoType(event, info) == false {
    return errors.New("AddChild got wrong type")
  }

  event.LockParent()
  if event.Parent() != nil {
    event.UnlockParent()
    return errors.New("Parent already registered")
  }

  event.LockChildren()

  for _, c := range(event.Children()) {
    if c.ID() == child.ID() {
      event.UnlockChildren()
      event.UnlockParent()
      return errors.New("Child already in event")
    }
  }

  // After all the checks are done, update the state of child + parent, then unlock and update
  child.setParent(event)
  event.addChild(child, info)

  event.UnlockChildren()
  event.UnlockParent()

  SendUpdate(event, NewSignal(event, "child_added"))
  return nil
}

func RunEvent(event Event) error {
  log.Printf("EVENT_RUN: %s", event.Name())
  next_action := "start"
  var err error = nil
  for next_action != "" {
    action, exists := event.Action(next_action)
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    log.Printf("EVENT_ACTION: %s - %s", event.Name(), next_action)
    next_action, err = action()
    if err != nil {
      return err
    }
  }

  log.Printf("EVENT_RUN_DONE: %s", event.Name())

  return nil
}

func AbortEvent(event Event) error {
  signal := NewSignal(event, "abort")
  SendUpdate(event, signal)
  return nil
}

func LockResources(event Event) error {
  locked_resources := []Resource{}
  var lock_err error = nil
  for _, resource := range(event.RequiredResources()) {
    err := LockResource(resource, event)
    if err != nil {
      lock_err = err
      break
    }
    locked_resources = append(locked_resources, resource)
  }

  if lock_err != nil {
    for _, resource := range(locked_resources) {
      UnlockResource(resource, event)
    }
    return lock_err
  }

  for _, resource := range(locked_resources) {
    NotifyResourceLocked(resource)
  }

  return nil
}

func FinishEvent(event Event) error {
  // TODO make more 'safe' like LockResources, or make UnlockResource not return errors
  log.Printf("EVENT_FINISH: %s", event.Name())
  for _, resource := range(event.RequiredResources()) {
    err := UnlockResource(resource, event)
    if err != nil {
      panic(err)
    }
    NotifyResourceUnlocked(resource)
  }

  err := UnlockResource(event.DoneResource(), event)
  if err != nil {
    return err
  }

  NotifyResourceUnlocked(event.DoneResource())

  err = event.finish()
  if err != nil {
    return err
  }

  SendUpdate(event, NewSignal(event, "event_done"))
  return nil
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
  child_info map[string]EventInfo
  child_lock sync.Mutex
  actions map[string]func() (string, error)
  handlers map[string]func() (string, error)
  parent Event
  parent_lock sync.Mutex
  abort chan string
}

func (event * BaseEvent) Action(action string) (func() (string, error), bool) {
  action_fn, exists := event.actions[action]
  return action_fn, exists
}

func NewBaseEvent(name string, description string, required_resources []Resource) (BaseEvent) {
  done_resource := NewResource("event_done", "signal that event is done", []Resource{})
  event := BaseEvent{
    BaseNode: BaseNode{
      name: name,
      description: description,
      id: randid(),
      signal: make(chan GraphSignal, 100),
      listeners: map[chan GraphSignal] chan GraphSignal{},
    },
    parent: nil,
    children: []Event{},
    child_info: map[string]EventInfo{},
    done_resource: done_resource,
    required_resources: required_resources,
    actions: map[string]func()(string, error){},
    handlers: map[string]func()(string, error){},
    abort: make(chan string, 1),
  }

  LockResource(event.done_resource, &event)

  event.actions["wait"] = func() (string, error) {
    signal := <- event.signal
    if signal.Type() == "abort" {
      return "", errors.New("State machine aborted by signal")
    } else if signal.Type() == "do_action" {
      return signal.Description(), nil
    } else {
      signal_fn, exists := event.Handler(signal.Type())
      if exists == true {
        return signal_fn()
      }
    }
    // ignore signals other than "abort" and "do_action"
    return "wait", nil
  }

  return event
}

func NewEvent(name string, description string, required_resources []Resource) (* BaseEvent) {
  event := NewBaseEvent(name, description, required_resources)
  event_ptr := &event

  event_ptr.actions["start"] = func() (string, error) {
    return "", nil
  }

  return event_ptr
}

func (event * BaseEvent) finish() error {
  return nil
}

func (event * BaseEvent) InfoType() reflect.Type {
  return nil
}

// EventQueue is a basic event that can have children.
// On start, it attempts to start it's children from the highest 'priority'
type EventQueue struct {
  BaseEvent
  listened_resources map[string]Resource
  queue_lock sync.Mutex
}

func (queue * EventQueue) finish() error {
  for _, resource := range(queue.listened_resources) {
    resource.UnregisterChannel(queue.signal)
  }
  return nil
}

func (queue * EventQueue) InfoType() reflect.Type {
  return reflect.TypeOf((*EventQueueInfo)(nil))
}

func NewEventQueue(name string, description string, required_resources []Resource) (* EventQueue) {
  queue := &EventQueue{
    BaseEvent: NewBaseEvent(name, description, []Resource{}),
    listened_resources: map[string]Resource{},
  }

  queue.actions["start"] = func() (string, error) {
    return "queue_event", nil
  }

  queue.actions["queue_event"] = func() (string, error) {
    // Copy the events to sort the list
    queue.LockChildren()
    copied_events := make([]Event, len(queue.Children()))
    copy(copied_events, queue.Children())
    less := func(i int, j int) bool {
      info_i := queue.ChildInfo(copied_events[i]).(*EventQueueInfo)
      info_j := queue.ChildInfo(copied_events[j]).(*EventQueueInfo)
      return info_i.priority < info_j.priority
    }
    sort.SliceStable(copied_events, less)

    wait := false
    needed_resources := map[string]Resource{}
    for _, event := range(copied_events) {
      // make sure all the required resources are registered to update the event
      for _, resource := range(event.RequiredResources()) {
        needed_resources[resource.ID()] = resource
      }

      info := queue.ChildInfo(event).(*EventQueueInfo)
      if info.state == "queued" {
        wait = true
        // Try to lock it
        err := LockResources(event)
        // start in new goroutine
        if err != nil {
          //log.Printf("Failed to lock %s: %s", event.Name(), err)
        } else {
          info.state = "running"
          log.Printf("EVENT_START: %s", event.Name())
          go func(event Event, info * EventQueueInfo, queue Event) {
            log.Printf("EVENT_GOROUTINE: %s", event.Name())
            err := RunEvent(event)
            if err != nil {
              log.Printf("EVENT_ERROR: %s", err)
            }
            info.state = "done"
            FinishEvent(event)
          }(event, info, queue)
        }
      } else if info.state == "running" {
        wait = true
      }
    }


    for _, resource := range(needed_resources) {
      queue.listened_resources[resource.ID()] = resource
      resource.RegisterChannel(queue.signal)
    }

    queue.UnlockChildren()

    if wait == true {
      return "wait", nil
    } else {
      return "", nil
    }
  }

  queue.actions["event_done"] = func() (string, error) {
    return "queue_event", nil
  }

  queue.actions["resource_update"] = func() (string, error) {
    return "queue_event", nil
  }

  queue.handlers["child_added"] = func() (string, error) {
    return "queue_event", nil
  }

  queue.handlers["lock_change"] = func() (string, error) {
    return "queue_event", nil
  }

  queue.handlers["event_done"] = func() (string, error) {
    return "queue_event", nil
  }

  return queue
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
  val, ok := event.child_info[idx.ID()]
  if ok == false {
    return nil
  }
  return val
}

func (event * BaseEvent) LockChildren() {
  log.Printf("LOCKING CHILDREN OF %s", event.Name())
  event.child_lock.Lock()
}

func (event * BaseEvent) UnlockChildren() {
  event.child_lock.Unlock()
}

func (event * BaseEvent) LockParent() {
  event.parent_lock.Lock()
}

func (event * BaseEvent) UnlockParent() {
  event.parent_lock.Unlock()
}

func (event * BaseEvent) setParent(parent Event) {
  event.parent = parent
}

func (event * BaseEvent) addChild(child Event, info EventInfo) {
  event.children = append(event.children, child)
  event.child_info[child.ID()] = info
}
