package graphvent

import (
  "fmt"
  "time"
  "errors"
  "reflect"
  "sync"
  "encoding/json"
)

// Update the threads listeners, and notify the parent to do the same
func (thread * BaseThread) PropagateUpdate(ctx * GraphContext, signal GraphSignal) {
  UseStates(ctx, []GraphNode{thread}, func(states []NodeState) (interface{}, error) {
    thread_state := states[0].(ThreadState)
    if signal.Direction() == Up {
      // Child->Parent, thread updates parent and connected resources
      if thread_state.Parent() != nil {
        SendUpdate(ctx, thread_state.Parent(), signal)
      }

      for _, resource := range(thread_state.Requirements()) {
        SendUpdate(ctx, resource, signal)
      }
    } else if signal.Direction() == Down {
      // Parent->Child, thread updated children
      for _, child := range(thread_state.Children()) {
        SendUpdate(ctx, child, signal)
      }

      for _, dep := range(thread_state.Dependencies()) {
        SendUpdate(ctx, dep, signal)
      }
    } else if signal.Direction() == Direct {

    } else {
      panic(fmt.Sprintf("Invalid signal direction: %d", signal.Direction()))
    }

    return nil, nil
  })
  thread.signal <- signal
}

/*func FindLockable(root Thread, id string) Lockable {
  if root == nil || id == ""{
    panic("invalid input")
  }

  for _, resource := range(root.Lockables()) {
    if resource.ID() == id {
      return resource
    }
  }
  for _, child := range(root.Children()) {
    resource := FindLockable(child, id)
    if resource != nil {
      return resource
    }
  }
  return nil
}*/

type ThreadInfo interface {
}

// An Thread is a lockable that has an additional parent->child relationship with other Threads
// This relationship allows the thread tree to be modified independent of the lockable state
type ThreadState interface {
  LockHolderState
  LockableState

  Parent() Thread
  SetParent(parent Thread)
  Children() []Thread
  ChildInfo(child NodeID) ThreadInfo
  AddChild(child Thread, info ThreadInfo) error
}

type BaseThreadState struct {
  BaseLockableState
  parent Thread
  children []Thread
  child_info map[NodeID] ThreadInfo
  resources map[NodeID]Lockable
  delegation_map map[NodeID]GraphNode
  info_type reflect.Type
}

func (state * BaseThreadState) MarshalJSON() ([]byte, error) {
  children := map[NodeID]interface{}{}
  for _, child := range(state.children) {
    children[child.ID()] = state.child_info[child.ID()]
  }

  var parent_id *NodeID = nil
  if state.parent != nil {
    new_str := state.parent.ID()
    parent_id = &new_str
  }

  resources := map[NodeID]*NodeID{}
  for _, resource := range(state.resources) {
    original_owner := state.delegation_map[resource.ID()]
    if original_owner != nil {
      owner := original_owner.ID()
      resources[resource.ID()] = &owner
    } else {
      resources[resource.ID()] = nil
    }
  }

  return json.Marshal(&struct{
    Name string `json:"name"`
    Parent *NodeID `json:"parent"`
    Children map[NodeID]interface{} `json:"children"`
    Lockables map[NodeID]*NodeID `json:"resources"`
  }{
    Name: state.Name(),
    Parent: parent_id,
    Children: children,
    Lockables: resources,
  })
}

func (state * BaseThreadState) Parent() Thread {
  return state.parent
}

func (state * BaseThreadState) SetParent(parent Thread) {
  state.parent = parent
}

func (state * BaseThreadState) Children() []Thread {
  return state.children
}

func (state * BaseThreadState) ChildInfo(child NodeID) ThreadInfo {
  return state.child_info[child]
}

func (state * BaseThreadState) AddChild(child Thread, info ThreadInfo) error {
  if child == nil {
    return fmt.Errorf("Will not connect nil to the thread tree")
  }

  _, exists := state.child_info[child.ID()]
  if exists == true {
    return fmt.Errorf("Will not connect the same child twice")
  }

  if info == nil && state.info_type != nil {
    return fmt.Errorf("nil info passed when expecting info")
  } else if info != nil {
    if reflect.TypeOf(info) != state.info_type {
      return fmt.Errorf("info type mismatch, expecting %+v", state.info_type)
    }
  }

  state.children = append(state.children, child)
  state.child_info[child.ID()] = info

  return nil
}

func checkIfChild(ctx * GraphContext, thread_state ThreadState, thread_id NodeID, cur_state ThreadState, cur_id NodeID) bool {
  for _, child := range(cur_state.Children()) {
    if child.ID() == thread_id {
      return true
    }
    val, _ := UseStates(ctx, []GraphNode{child}, func(states []NodeState) (interface{}, error) {
      child_state := states[0].(ThreadState)
      return checkIfRequirement(ctx, cur_state, cur_id, child_state, child.ID()), nil
    })

    is_child := val.(bool)
    if is_child {
      return true
    }
  }

  return false
}

func LinkThreads(ctx * GraphContext, thread Thread, child Thread, info ThreadInfo) error {
  if ctx == nil || thread == nil || child == nil {
    return fmt.Errorf("invalid input")
  }

  if thread.ID() == child.ID() {
    return fmt.Errorf("Will not link %s as a child of itself", thread.ID())
  }


  _, err := UpdateStates(ctx, []GraphNode{thread, child}, func(states []NodeState) ([]NodeState, interface{}, error) {
    thread_state := states[0].(ThreadState)
    child_state := states[1].(ThreadState)

    if child_state.Parent() != nil {
      return nil, nil, fmt.Errorf("EVENT_LINK_ERR: %s already has a parent, cannot link as child", child.ID())
    }

    if checkIfChild(ctx, thread_state, thread.ID(), child_state, child.ID()) == true {
      return nil, nil, fmt.Errorf("EVENT_LINK_ERR: %s is a child of %s so cannot add as parent", thread.ID(), child.ID())
    }

    if checkIfChild(ctx, child_state, child.ID(), thread_state, thread.ID()) == true {
      return nil, nil, fmt.Errorf("EVENT_LINK_ERR: %s is already a parent of %s so will not add again", thread.ID(), child.ID())
    }

    err := thread_state.AddChild(child, info)
    if err != nil {
      return nil, nil, fmt.Errorf("EVENT_LINK_ERR: error adding %s as child to %s: %e", child.ID(), thread.ID(), err)
    }
    child_state.SetParent(thread)

    return states, nil, nil
  })

  if err != nil {
    return err
  }

  return nil
}

// Threads allow locks to pass to their requirements, but they won't allow cycles
func (state * BaseThreadState) OriginalLockHolder(id NodeID) GraphNode {
  node, exists := state.delegation_map[id]
  if exists == false {
    panic("Attempted to take a get the original lock holder of a resource we don't own")
  }
  delete(state.delegation_map, id)
  return node
}

func (state * BaseThreadState) AllowedToTakeLock(id NodeID) bool {
  return false
}

func (state * BaseThreadState) RecordLockHolder(id NodeID, lock_holder GraphNode) {
  _, exists := state.delegation_map[id]
  if exists == true {
    panic("Attempted to lock a resource we're already holding(lock cycle)")
  }

  state.delegation_map[id] = lock_holder
}

// Thread is the interface that thread tree nodes must implement
type Thread interface {
  GraphNode

  Action(action string) (ThreadAction, bool)
  Handler(signal_type string) (ThreadHandler, bool)

  SetTimeout(end_time time.Time, action string)
  ClearTimeout()
  Timeout() <-chan time.Time
  TimeoutAction() string
}

func FindChild(ctx * GraphContext, thread Thread, thread_state ThreadState, id NodeID) Thread {
  if thread == nil {
    panic("cannot recurse through nil")
  }
  if id ==  thread.ID() {
    return thread
  }


  for _, child := range thread_state.Children() {
    res, _ := UseStates(ctx, []GraphNode{child}, func(states []NodeState) (interface{}, error) {
      child_state := states[0].(ThreadState)
      result := FindChild(ctx, child, child_state, id)
      return result, nil
    })
    result := res.(Thread)
    if result != nil {
      return result
    }
  }

  return nil
}

func RunThread(ctx * GraphContext, thread Thread) error {
  ctx.Log.Logf("thread", "EVENT_RUN: %s", thread.ID())

  _, err := UseStates(ctx, []GraphNode{thread}, func(states []NodeState) (interface{}, error) {
    thread_state := states[0].(ThreadState)
    if thread_state.Owner() == nil {
      return nil, fmt.Errorf("EVENT_RUN_NOT_LOCKED: %s", thread_state.Name())
    } else if thread_state.Owner().ID() != thread.ID() {
      return nil, fmt.Errorf("EVENT_RUN_RESOURCE_ALREADY_LOCKED: %s, %s", thread_state.Name(), thread_state.Owner().ID())
    }
    return nil, nil
  })

  SendUpdate(ctx, thread, NewSignal(thread, "thread_start"))

  next_action := "start"
  for next_action != "" {
    action, exists := thread.Action(next_action)
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    ctx.Log.Logf("thread", "EVENT_ACTION: %s - %s", thread.ID(), next_action)
    next_action, err = action(ctx, thread)
    if err != nil {
      return err
    }
  }

  SendUpdate(ctx, thread, NewSignal(thread, "thread_done"))

  ctx.Log.Logf("thread", "EVENT_RUN_DONE: %s", thread.ID())

  return nil
}

type ThreadAction func(* GraphContext, Thread)(string, error)
type ThreadActions map[string]ThreadAction
type ThreadHandler func(* GraphContext, Thread, GraphSignal)(string, error)
type ThreadHandlers map[string]ThreadHandler

// Thread is the most basic thread that can exist in the thread tree.
// On start it automatically transitions to completion.
// This node by itself doesn't implement any special behaviours for children, so they will be ignored.
// When started, this thread automatically transitions to completion
type BaseThread struct {
  BaseNode

  resources_lock sync.Mutex
  children_lock sync.Mutex
  info_lock sync.Mutex
  parent_lock sync.Mutex

  Actions ThreadActions
  Handlers ThreadHandlers

  timeout <-chan time.Time
  timeout_action string
}

func (thread * BaseThread) Lock(node GraphNode, state LockableState) error {
  return nil
}

func (thread * BaseThread) Unlock(node GraphNode, state LockableState) error {
  return nil
}

func (thread * BaseThread) Action(action string) (ThreadAction, bool) {
  action_fn, exists := thread.Actions[action]
  return action_fn, exists
}

func (thread * BaseThread) Handler(signal_type string) (ThreadHandler, bool) {
  handler, exists := thread.Handlers[signal_type]
  return handler, exists
}

func (thread * BaseThread) TimeoutAction() string {
  return thread.timeout_action
}

func (thread * BaseThread) Timeout() <-chan time.Time {
  return thread.timeout
}

func (thread * BaseThread) ClearTimeout() {
  thread.timeout_action = ""
  thread.timeout = nil
}

func (thread * BaseThread) SetTimeout(end_time time.Time, action string) {
  thread.timeout_action = action
  thread.timeout = time.After(time.Until(end_time))
}

var ThreadDefaultStart = func(ctx * GraphContext, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_DEFAUL_START: %s", thread.ID())
  return "wait", nil
}

var ThreadWait = func(ctx * GraphContext, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_WAIT: %s TIMEOUT: %+v", thread.ID(), thread.Timeout())
  select {
    case signal := <- thread.SignalChannel():
      ctx.Log.Logf("thread", "THREAD_SIGNAL: %s %+v", thread.ID(), signal)
      signal_fn, exists := thread.Handler(signal.Type())
      if exists == true {
        ctx.Log.Logf("thread", "THREAD_HANDLER: %s - %s", thread.ID(), signal.Type())
        return signal_fn(ctx, thread, signal)
      }
    case <- thread.Timeout():
      ctx.Log.Logf("thread", "THREAD_TIMEOUT %s - NEXT_STATE: %s", thread.ID(), thread.TimeoutAction())
      return thread.TimeoutAction(), nil
  }
  return "wait", nil
}

var ThreadAbort = func(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
  return "", fmt.Errorf("%s aborted by signal from %s", thread.ID(), signal.Source())
}

var ThreadCancel = func(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
  return "", nil
}

func NewBaseThreadState(name string) BaseThreadState {
  return BaseThreadState{
    BaseLockableState: NewLockableState(name),
    delegation_map: map[NodeID]GraphNode{},
    children: []Thread{},
    child_info: map[NodeID]ThreadInfo{},
    resources: map[NodeID]Lockable{},
    parent: nil,
  }
}

func NewBaseThread(ctx * GraphContext, name string) (BaseThread, error) {
  state := NewBaseThreadState(name)
  thread := BaseThread{
    BaseNode: NewNode(ctx, RandID(), &state),
    Actions: ThreadActions{
      "wait": ThreadWait,
      "start": ThreadDefaultStart,
    },
    Handlers: ThreadHandlers{
      "abort": ThreadAbort,
      "cancel": ThreadCancel,
    },
    timeout: nil,
    timeout_action: "",
  }

  return thread, nil
}

func NewThread(ctx * GraphContext, name string, requirements []Lockable, actions ThreadActions, handlers ThreadHandlers) (* BaseThread, error) {
  thread, err := NewBaseThread(ctx, name)
  if err != nil {
    return nil, err
  }

  thread_ptr := &thread

  for _, requirement := range(requirements) {
    err := LinkLockables(ctx, thread_ptr, requirement)
    if err != nil {
      return nil, err
    }
  }

  for key, fn := range(actions) {
    thread.Actions[key] = fn
  }

  for key, fn := range(handlers) {
    thread.Handlers[key] = fn
  }

  return thread_ptr, nil
}

/*
// ThreadQueue is a basic thread that can have children.
// On start, it attempts to start it's children from the highest 'priority'
type ThreadQueueInfo struct {
  priority int
  state string
}

func (info * BaseThreadQueueInfo) MarshalJSON() ([]byte, error) {
  return json.Marshal(&struct{
    Priority int `json:"priority"`
    State string `json:"state"`
  }{
    Priority: info.priority,
    State: info.state,
  })
}

func NewThreadQueueInfo(priority int) * BaseThreadQueueInfo {
  info := &ThreadQueueInfo{
    priority: priority,
    state: "queued",
  }

  return info
}

type ThreadQueue struct {
  Thread
  listened_resources map[string]Lockable
  queue_lock sync.Mutex
}

func (queue * BaseThreadQueue) Unlock() error {
  for _, resource := range(queue.listened_resources) {
    resource.UnregisterChannel(queue.signal)
  }
  return nil
}

func (queue * BaseThreadQueue) InfoType() reflect.Type {
  return reflect.TypeOf((*ThreadQueueInfo)(nil))
}

func NewThreadQueue(name string, description string, resources []Lockable) (* BaseThreadQueue, error) {
  queue := &ThreadQueue{
    Thread: NewThread(name, description),
    listened_resources: map[string]Lockable{},
  }

  queue.state = NewBaseThreadState(name, description)

  AddLockables(queue, resources)

  queue.Actions["wait"] = ThreadWait(queue)
  queue.Handlers["abort"] = ThreadAbort(queue)
  queue.Handlers["cancel"] = ThreadCancel(queue)

  queue.Actions["start"] = func() (string, error) {
    return "queue_thread", nil
  }

  queue.Actions["queue_thread"] = func() (string, error) {
    // Copy the threads to sort the list
    queue.LockChildren()
    copied_threads := make([]Thread, len(queue.Children()))
    copy(copied_threads, queue.Children())
    less := func(i int, j int) bool {
      info_i := queue.ChildInfo(copied_threads[i]).(*ThreadQueueInfo)
      info_j := queue.ChildInfo(copied_threads[j]).(*ThreadQueueInfo)
      return info_i.priority < info_j.priority
    }
    sort.SliceStable(copied_threads, less)

    needed_resources := map[string]Lockable{}
    for _, thread := range(copied_threads) {
      // make sure all the required resources are registered to update the thread
      for _, resource := range(thread.Lockables()) {
        needed_resources[resource.ID()] = resource
      }

      info := queue.ChildInfo(thread).(*ThreadQueueInfo)
      thread.LockInfo()
      defer thread.UnlockInfo()
      if info.state == "queued" {
        err := LockLockable(thread)
        // start in new goroutine
        if err != nil {
        } else {
          info.state = "running"
          Log.Logf("thread", "EVENT_START: %s", thread.Name())
          go func(thread Thread, info * BaseThreadQueueInfo, queue Thread) {
            Log.Logf("thread", "EVENT_GOROUTINE: %s", thread.Name())
            err := RunThread(thread)
            if err != nil {
              Log.Logf("thread", "EVENT_ERROR: %s", err)
            }
            thread.LockInfo()
            defer thread.UnlockInfo()
            info.state = "done"
          }(thread, info, queue)
        }
      }
    }


    for _, resource := range(needed_resources) {
      _, exists := queue.listened_resources[resource.ID()]
      if exists == false {
        Log.Logf("thread", "REGISTER_RESOURCE: %s - %s", queue.Name(), resource.Name())
        queue.listened_resources[resource.ID()] = resource
        resource.RegisterChannel(queue.signal)
      }
    }

    queue.UnlockChildren()

    return "wait", nil
  }

  queue.Handlers["resource_connected"] = func(signal GraphSignal) (string, error) {
    return "queue_thread", nil
  }

  queue.Handlers["child_added"] = func(signal GraphSignal) (string, error) {
    return "queue_thread", nil
  }

  queue.Handlers["lock_changed"] = func(signal GraphSignal) (string, error) {
    return "queue_thread", nil
  }

  queue.Handlers["thread_done"] = func(signal GraphSignal) (string, error) {
    return "queue_thread", nil
  }

  return queue, nil
}
*/
