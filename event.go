package graphvent

import (
  "fmt"
  "time"
  "errors"
  "reflect"
  "sort"
  "sync"
)

// Update the events listeners, and notify the parent to do the same
func (event * BaseEvent) update(signal GraphSignal) {
  if signal.Downwards() == false {
    // Child->Parent
    event.parent_lock.Lock()
    defer event.parent_lock.Unlock()
    if event.parent != nil {
      SendUpdate(event.parent, signal)
    }

    event.rr_lock.Lock()
    defer event.rr_lock.Unlock()
    for _, resource := range(event.required_resources) {
      SendUpdate(resource, signal)
    }
  } else {
    // Parent->Child
    event.ChildrenSliceLock.Lock()
    defer event.ChildrenSliceLock.Unlock()
    for _, child := range(event.ChildrenSlice) {
      SendUpdate(child, signal)
    }
  }
  event.signal <- signal
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
  Handler(signal_type string) (func(GraphSignal) (string, error), bool)
  RequiredResources() []Resource
  DoneResource() Resource
  SetTimeout(end_time time.Time, action string)
  Timeout() <-chan time.Time
  TimeoutAction() string
  Signal() chan GraphSignal

  finish() error

  addChild(child Event, info EventInfo)
  setParent(parent Event)
}

func (event * BaseEvent) Signal() chan GraphSignal {
  return event.signal
}

func (event * BaseEvent) TimeoutAction() string {
  return event.timeout_action
}

func (event * BaseEvent) Timeout() <-chan time.Time {
  return event.timeout
}

func (event * BaseEvent) SetTimeout(end_time time.Time, action string) {
  event.timeout_action = action
  event.timeout = time.After(time.Until(end_time))
}

func (event * BaseEvent) Handler(signal_type string) (func(GraphSignal)(string, error), bool) {
  handler, exists := event.handlers[signal_type]
  return handler, exists
}

func FindResources(event Event, resource_type reflect.Type) []Resource {
  resources := event.RequiredResources()
  found := []Resource{}
  for _, resource := range(resources) {
    if reflect.TypeOf(resource) == resource_type {
      found = append(found, resource)
    }
  }

  for _, child := range(event.Children()) {
    found = append(found, FindResources(child, resource_type)...)
  }

  m := map[string]Resource{}
  for _, resource := range(found) {
    m[resource.ID()] = resource
  }
  ret := []Resource{}
  for _, resource := range(m) {
    ret = append(ret, resource)
  }
  return ret
}

func FindRequiredResource(event Event, id string) Resource {
  for _, resource := range(event.RequiredResources()) {
    if resource.ID() == id {
      return resource
    }
  }

  for _, child := range(event.Children()) {
    result := FindRequiredResource(child, id)
    if result != nil {
      return result
    }
  }

  return nil
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
  child.LockParent()
  if child.Parent() != nil {
    child.UnlockParent()
    event.UnlockParent()
    return errors.New(fmt.Sprintf("Parent already registered: %s->%s already %s", child.Name(), event.Name(), child.Parent().Name()))
  }

  event.LockChildren()

  for _, c := range(event.Children()) {
    if c.ID() == child.ID() {
      event.UnlockChildren()
      child.UnlockParent()
      event.UnlockParent()
      return errors.New("Child already in event")
    }
  }

  // After all the checks are done, update the state of child + parent, then unlock and update
  child.setParent(event)
  event.addChild(child, info)

  event.UnlockChildren()
  child.UnlockParent()
  event.UnlockParent()

  SendUpdate(event, NewSignal(event, "child_added"))
  return nil
}

func RunEvent(event Event) error {
  log.Logf("event", "EVENT_RUN: %s", event.Name())
  SendUpdate(event, NewSignal(event, "event_start"))
  next_action := "start"
  var err error = nil
  for next_action != "" {
    action, exists := event.Action(next_action)
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    log.Logf("event", "EVENT_ACTION: %s - %s", event.Name(), next_action)
    next_action, err = action()
    if err != nil {
      return err
    }
  }

  log.Logf("event", "EVENT_RUN_DONE: %s", event.Name())

  return nil
}

func EventAbort(event Event) func(signal GraphSignal) (string, error) {
  return func(signal GraphSignal) (string, error) {
    return "", errors.New(fmt.Sprintf("%s aborted by signal", event.ID()))
  }
}

func EventCancel(event Event) func(signal GraphSignal) (string, error) {
  return func(signal GraphSignal) (string, error) {
    return "", nil
  }
}

func LockResources(event Event) error {
  log.Logf("event", "RESOURCE_LOCKING for %s - %+v", event.Name(), event.RequiredResources())
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

  signal := NewDownSignal(event, "locked")
  SendUpdate(event, signal)

  return nil
}

func FinishEvent(event Event) error {
  log.Logf("event", "EVENT_FINISH: %s", event.Name())
  for _, resource := range(event.RequiredResources()) {
    err := UnlockResource(resource, event)
    if err != nil {
      panic(err)
    }
  }

  err := UnlockResource(event.DoneResource(), event)
  if err != nil {
    return err
  }

  SendUpdate(event, NewDownSignal(event, "unlocked"))
  SendUpdate(event.DoneResource(), NewDownSignal(event, "unlocked"))

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
  rr_lock sync.Mutex
  required_resources []Resource
  ChildrenSlice []Event
  ChildInfoMap map[string]EventInfo
  ChildrenSliceLock sync.Mutex
  actions map[string]func() (string, error)
  handlers map[string]func(GraphSignal) (string, error)
  parent Event
  parent_lock sync.Mutex
  abort chan string
  timeout <-chan time.Time
  timeout_action string
}

func (event * BaseEvent) Action(action string) (func() (string, error), bool) {
  action_fn, exists := event.actions[action]
  return action_fn, exists
}

func EventWait(event Event) (func() (string, error)) {
  return func() (string, error) {
    log.Logf("event", "EVENT_WAIT: %s TIMEOUT: %+v", event.Name(), event.Timeout())
    select {
    case signal := <- event.Signal():
      log.Logf("event", "EVENT_SIGNAL: %s %+v", event.Name(), signal)
      signal_fn, exists := event.Handler(signal.Type())
      if exists == true {
        log.Logf("event", "EVENT_HANDLER: %s - %s", event.Name(), signal.Type())
        return signal_fn(signal)
      }
      return "wait", nil
    case <- event.Timeout():
      log.Logf("event", "EVENT_TIMEOUT %s - NEXT_STATE: %s", event.Name(), event.TimeoutAction())
      return event.TimeoutAction(), nil
    }
  }
}

func NewBaseEvent(name string, description string, required_resources []Resource) (BaseEvent) {
  done_resource := NewResource("event_done", "signal that event is done", []Resource{})
  event := BaseEvent{
    BaseNode: NewBaseNode(name, description, randid()),
    parent: nil,
    ChildrenSlice: []Event{},
    ChildInfoMap: map[string]EventInfo{},
    done_resource: done_resource,
    required_resources: required_resources,
    actions: map[string]func()(string, error){},
    handlers: map[string]func(GraphSignal)(string, error){},
    abort: make(chan string, 1),
    timeout: nil,
    timeout_action: "",
  }

  LockResource(event.done_resource, &event)

  return event
}

func NewEvent(name string, description string, required_resources []Resource) (* BaseEvent) {
  event := NewBaseEvent(name, description, required_resources)
  event_ptr := &event

  event_ptr.actions["wait"] = EventWait(event_ptr)
  event_ptr.handlers["abort"] = EventAbort(event_ptr)
  event_ptr.handlers["cancel"] = EventCancel(event_ptr)

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
    BaseEvent: NewBaseEvent(name, description, required_resources),
    listened_resources: map[string]Resource{},
  }

  queue.actions["wait"] = EventWait(queue)
  queue.handlers["abort"] = EventAbort(queue)
  queue.handlers["cancel"] = EventCancel(queue)

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

    needed_resources := map[string]Resource{}
    for _, event := range(copied_events) {
      // make sure all the required resources are registered to update the event
      for _, resource := range(event.RequiredResources()) {
        needed_resources[resource.ID()] = resource
      }

      info := queue.ChildInfo(event).(*EventQueueInfo)
      if info.state == "queued" {
        // Try to lock it
        err := LockResources(event)
        // start in new goroutine
        if err != nil {
          //log.Logf("event", "Failed to lock %s: %s", event.Name(), err)
        } else {
          info.state = "running"
          log.Logf("event", "EVENT_START: %s", event.Name())
          go func(event Event, info * EventQueueInfo, queue Event) {
            log.Logf("event", "EVENT_GOROUTINE: %s", event.Name())
            err := RunEvent(event)
            if err != nil {
              log.Logf("event", "EVENT_ERROR: %s", err)
            }
            info.state = "done"
            FinishEvent(event)
          }(event, info, queue)
        }
      }
    }


    for _, resource := range(needed_resources) {
      _, exists := queue.listened_resources[resource.ID()]
      if exists == false {
        log.Logf("event", "REGISTER_RESOURCE: %s - %s", queue.Name(), resource.Name())
        queue.listened_resources[resource.ID()] = resource
        resource.RegisterChannel(queue.signal)
      }
    }

    queue.UnlockChildren()

    return "wait", nil
  }

  queue.handlers["resource_connected"] = func(signal GraphSignal) (string, error) {
    return "queue_event", nil
  }

  queue.handlers["child_added"] = func(signal GraphSignal) (string, error) {
    return "queue_event", nil
  }

  queue.handlers["lock_changed"] = func(signal GraphSignal) (string, error) {
    return "queue_event", nil
  }

  queue.handlers["event_done"] = func(signal GraphSignal) (string, error) {
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
  return event.ChildrenSlice
}

func (event * BaseEvent) ChildInfo(idx Event) EventInfo {
  val, ok := event.ChildInfoMap[idx.ID()]
  if ok == false {
    return nil
  }
  return val
}

func (event * BaseEvent) LockChildren() {
  event.ChildrenSliceLock.Lock()
}

func (event * BaseEvent) UnlockChildren() {
  event.ChildrenSliceLock.Unlock()
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
  event.ChildrenSlice = append(event.ChildrenSlice, child)
  event.ChildInfoMap[child.ID()] = info
}

type GQLEvent struct {
  BaseEvent
  abort chan error
}
