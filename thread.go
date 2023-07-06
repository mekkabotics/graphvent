package graphvent

import (
  "fmt"
  "time"
  "sync"
  "errors"
  "reflect"
  "encoding/json"
)

// Update the threads listeners, and notify the parent to do the same
func (thread * BaseThread) PropagateUpdate(ctx * GraphContext, signal GraphSignal, states NodeStateMap) {
  thread_state := states[thread.ID()].(ThreadState)
  if signal.Direction() == Up {
    // Child->Parent, thread updates parent and connected requirement
    if thread_state.Parent() != nil {
      UseMoreStates(ctx, []GraphNode{thread_state.Parent()}, states, func(states NodeStateMap) (error) {
        SendUpdate(ctx, thread_state.Parent(), signal, states)
        return nil
      })
    }

    UseMoreStates(ctx, NodeList(thread_state.Dependencies()), states, func(states NodeStateMap) (error) {
      for _, dep := range(thread_state.Dependencies()) {
        SendUpdate(ctx, dep, signal, states)
      }
      return nil
    })
  } else if signal.Direction() == Down {
    // Parent->Child, updates children and dependencies
    UseMoreStates(ctx, NodeList(thread_state.Children()), states, func(states NodeStateMap) (error) {
      for _, child := range(thread_state.Children()) {
        SendUpdate(ctx, child, signal, states)
      }
      return nil
    })

    UseMoreStates(ctx, NodeList(thread_state.Requirements()), states, func(states NodeStateMap) (error) {
      for _, requirement := range(thread_state.Requirements()) {
        SendUpdate(ctx, requirement, signal, states)
      }
      return nil
    })
  } else if signal.Direction() == Direct {

  } else {
    panic(fmt.Sprintf("Invalid signal direction: %d", signal.Direction()))
  }

  thread.signal <- signal
}

type ThreadInfo interface {
}

// An Thread is a lockable that has an additional parent->child relationship with other Threads
// This relationship allows the thread tree to be modified independent of the lockable state
type ThreadState interface {
  LockableState

  Parent() Thread
  SetParent(parent Thread)
  Children() []Thread
  Child(id NodeID) Thread
  ChildInfo(child NodeID) ThreadInfo
  AddChild(child Thread, info ThreadInfo) error
  RemoveChild(child Thread)
  Start() error
  Stop() error
  State() string

  TimeoutAction() string
  SetTimeout(end_time time.Time, action string)
}

type BaseThreadState struct {
  BaseLockableState
  state_name string
  parent Thread
  children []Thread
  child_info map[NodeID] ThreadInfo
  InfoType reflect.Type
  timeout time.Time
  timeout_action string
}

type BaseThreadStateJSON struct {
  Parent *NodeID `json:"parent"`
  Children map[NodeID]interface{} `json:"children"`
  Timeout time.Time `json:"timeout"`
  TimeoutAction string `json:"timeout_action"`
  StateName string `json:"state_name"`
  BaseLockableStateJSON
}

func SaveBaseThreadState(state * BaseThreadState) BaseThreadStateJSON {
  children := map[NodeID]interface{}{}
  for _, child := range(state.children) {
    children[child.ID()] = state.child_info[child.ID()]
  }

  var parent_id *NodeID = nil
  if state.parent != nil {
    new_str := state.parent.ID()
    parent_id = &new_str
  }

  lockable_state := SaveBaseLockableState(&state.BaseLockableState)

  ret := BaseThreadStateJSON{
    Parent: parent_id,
    Children: children,
    Timeout: state.timeout,
    TimeoutAction: state.timeout_action,
    StateName: state.state_name,
    BaseLockableStateJSON: lockable_state,
  }

  return ret
}

func RestoreBaseThread(ctx * GraphContext, id NodeID, actions ThreadActions, handlers ThreadHandlers) BaseThread {
  base_lockable := RestoreBaseLockable(ctx, id)
  thread := BaseThread{
    BaseLockable: base_lockable,
    Actions: actions,
    Handlers: handlers,
    child_waits: &sync.WaitGroup{},
    active: false,
    active_lock: &sync.Mutex{},
  }

  return thread
}

func LoadSimpleThread(ctx * GraphContext, id NodeID) (GraphNode, error) {
  thread := RestoreBaseThread(ctx, id, BaseThreadActions, BaseThreadHandlers)
  return &thread, nil
}

func RestoreBaseThreadState(ctx * GraphContext, j BaseThreadStateJSON, loaded_nodes NodeMap) (*BaseThreadState, error) {
  lockable_state, err := RestoreBaseLockableState(ctx, j.BaseLockableStateJSON, loaded_nodes)
  if err != nil {
    return nil, err
  }

  state := BaseThreadState{
    BaseLockableState: *lockable_state,
    parent: nil,
    children: make([]Thread, len(j.Children)),
    child_info: map[NodeID]ThreadInfo{},
    InfoType: nil,
    state_name: j.StateName,
    timeout: j.Timeout,
    timeout_action: j.TimeoutAction,
  }

  if j.Parent != nil {
    p, err := LoadNodeRecurse(ctx, *j.Parent, loaded_nodes)
    if err != nil {
      return nil, err
    }
    p_t, ok := p.(Thread)
    if ok == false {
      return nil, err
    }
    state.parent = p_t
  }

  // TODO: Call different loading functions(to return different ThreadInfo types, based on the j.Type,
  // Will probably have to add another set of callbacks to the context for this, and since there's now 3 sets that need to be matching it could be useful to move them to a struct so it's easier to keep in sync
  i := 0
  for id, info_raw := range(j.Children) {
    child_node, err := LoadNodeRecurse(ctx, id, loaded_nodes)
    if err != nil {
      return nil, err
    }
    child_t, ok := child_node.(Thread)
    if ok == false {
      return nil, fmt.Errorf("%+v is not a Thread as expected", child_node)
    }
    state.children[i] = child_t

    info_map, ok := info_raw.(map[string]interface{})
    if ok == false && info_raw != nil {
      return nil, fmt.Errorf("Parsed map wrong type: %+v", info_raw)
    }
    info_fn, exists := ctx.InfoLoadFuncs[j.Type]
    var parsed_info ThreadInfo
    if exists == false {
      parsed_info = nil
    } else {
      parsed_info, err = info_fn(ctx, info_map)
      if err != nil {
        return nil, err
      }
    }

    state.child_info[id] = parsed_info
    i++
  }

  return &state, nil
}

func LoadSimpleThreadState(ctx * GraphContext, data []byte, loaded_nodes NodeMap)(NodeState, error){
  var j BaseThreadStateJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  state, err := RestoreBaseThreadState(ctx, j, loaded_nodes)
  if err != nil {
    return nil, err
  }

  return state, nil
}

func (state * BaseThreadState) SetTimeout(timeout time.Time, action string) {
  state.timeout = timeout
  state.timeout_action = action
}

func (state * BaseThreadState) TimeoutAction() string {
  return state.timeout_action
}

func (state * BaseThreadState) MarshalJSON() ([]byte, error) {
  thread_state := SaveBaseThreadState(state)
  return json.Marshal(&thread_state)
}

func (state * BaseThreadState) State() string {
  return state.state_name
}

func (state * BaseThreadState) SetState(new_state string) error {
  if new_state == "init" {
    return fmt.Errorf("Cannot set a thread to 'init' with SetState")
  } else if new_state == "finished" {
    return fmt.Errorf("Cannot set a thread to 'finished' with SetState")
  } else if new_state == "started" {
    return fmt.Errorf("Cannot set a thread to 'started' with SetState")
  }

  state.state_name = new_state
  return nil
}

func (state * BaseThreadState) Start() error {
  if state.state_name != "init" {
    return fmt.Errorf("Cannot start a thread that's already started")
  }
  state.state_name = "started"
  return nil
}

func (state * BaseThreadState) Stop() error {
  if state.state_name == "finished" {
    return fmt.Errorf("Cannot stop a finished thread")
  } else if state.state_name == "init" {
    return fmt.Errorf("Cannot stop a thread that hasn't been started")
  }
  state.state_name = "finished"
  return nil
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

func (state * BaseThreadState) Child(id NodeID) Thread {
  for _, child := range(state.children) {
    if child.ID() == id {
      return child
    }
  }
  return nil
}



func (state * BaseThreadState) ChildInfo(child NodeID) ThreadInfo {
  return state.child_info[child]
}

// Requires thread and childs state to be locked for write
func UnlinkThreads(ctx * GraphContext, thread Thread, child Thread) error {
  state := thread.State().(ThreadState)
  var found GraphNode = nil
  for _, c := range(state.Children()) {
    if child.ID() == c.ID() {
      found = c
      break
    }
  }

  if found == nil {
    return fmt.Errorf("UNLINK_THREADS_ERR: %s is not a child of %s", child.ID(), thread.ID())
  }

  child_state := child.State().(ThreadState)
  child_state.SetParent(nil)
  state.RemoveChild(child)

  return nil
}

func (state * BaseThreadState) RemoveChild(child Thread) {
  idx := -1
  for i, c := range(state.children) {
    if c.ID() == child.ID() {
      idx = i
      break
    }
  }

  if idx == -1 {
    panic(fmt.Sprintf("%s is not a child of %s", child.ID(), state.Name()))
  }

  child_len := len(state.children)
  state.children[idx] = state.children[child_len-1]
  state.children = state.children[0:child_len-1]
}

func (state * BaseThreadState) AddChild(child Thread, info ThreadInfo) error {
  if child == nil {
    return fmt.Errorf("Will not connect nil to the thread tree")
  }

  _, exists := state.child_info[child.ID()]
  if exists == true {
    return fmt.Errorf("Will not connect the same child twice")
  }

  if info == nil && state.InfoType != nil {
    return fmt.Errorf("nil info passed when expecting info")
  } else if info != nil {
    if reflect.TypeOf(info) != state.InfoType {
      return fmt.Errorf("info type mismatch, expecting %+v", state.InfoType)
    }
  }

  state.children = append(state.children, child)
  state.child_info[child.ID()] = info

  return nil
}

func checkIfChild(ctx * GraphContext, thread_id NodeID, cur_state ThreadState, cur_id NodeID) bool {
  for _, child := range(cur_state.Children()) {
    if child.ID() == thread_id {
      return true
    }
    is_child := false
    UseStates(ctx, []GraphNode{child}, func(states NodeStateMap) (error) {
      child_state := states[child.ID()].(ThreadState)
      is_child = checkIfChild(ctx, cur_id, child_state, child.ID())
      return nil
    })
    if is_child {
      return true
    }
  }

  return false
}

// Requires thread and childs state to be locked for write
func LinkThreads(ctx * GraphContext, thread Thread, child Thread, info ThreadInfo) error {
  if ctx == nil || thread == nil || child == nil {
    return fmt.Errorf("invalid input")
  }

  if thread.ID() == child.ID() {
    return fmt.Errorf("Will not link %s as a child of itself", thread.ID())
  }

  thread_state := thread.State().(ThreadState)
  child_state := child.State().(ThreadState)

  if child_state.Parent() != nil {
    return fmt.Errorf("EVENT_LINK_ERR: %s already has a parent, cannot link as child", child.ID())
  }

  if checkIfChild(ctx, thread.ID(), child_state, child.ID()) == true {
    return fmt.Errorf("EVENT_LINK_ERR: %s is a child of %s so cannot add as parent", thread.ID(), child.ID())
  }

  if checkIfChild(ctx, child.ID(), thread_state, thread.ID()) == true {
    return fmt.Errorf("EVENT_LINK_ERR: %s is already a parent of %s so will not add again", thread.ID(), child.ID())
  }

  err := thread_state.AddChild(child, info)
  if err != nil {
    return fmt.Errorf("EVENT_LINK_ERR: error adding %s as child to %s: %e", child.ID(), thread.ID(), err)
  }
  child_state.SetParent(thread)

  if err != nil {
    return err
  }

  return nil
}

// Thread is the interface that thread tree nodes must implement
type Thread interface {
  Lockable

  Action(action string) (ThreadAction, bool)
  Handler(signal_type string) (ThreadHandler, bool)

  SetTimeout(end time.Time)
  Timeout() <-chan time.Time
  ClearTimeout()

  ChildWaits() *sync.WaitGroup
  Start() error
  Stop() error
}

// Requires that thread is already locked for read in UseStates
func FindChild(ctx * GraphContext, thread Thread, id NodeID, states NodeStateMap) Thread {
  if thread == nil {
    panic("cannot recurse through nil")
  }
  if id ==  thread.ID() {
    return thread
  }

  thread_state := thread.State().(ThreadState)
  for _, child := range thread_state.Children() {
    var result Thread = nil
    UseMoreStates(ctx, []GraphNode{child}, states, func(states NodeStateMap) (error) {
      result = FindChild(ctx, child, id, states)
      return nil
    })
    if result != nil {
      return result
    }
  }

  return nil
}

func ChildGo(ctx * GraphContext, thread Thread, child Thread, first_action string) {
  thread.ChildWaits().Add(1)
  go func(child Thread) {
    ctx.Log.Logf("thread", "THREAD_START_CHILD: %s from %s", thread.ID(), child.ID())
    defer thread.ChildWaits().Done()
    err := ThreadLoop(ctx, child, first_action)
    if err != nil {
      ctx.Log.Logf("thread", "THREAD_CHILD_RUN_ERR: %s %e", child.ID(), err)
    } else {
      ctx.Log.Logf("thread", "THREAD_CHILD_RUN_DONE: %s", child.ID())
    }
  }(child)
}

// Main Loop for Threads
func ThreadLoop(ctx * GraphContext, thread Thread, first_action string) error {
  // Start the thread, error if double-started
  ctx.Log.Logf("thread", "THREAD_LOOP_START: %s - %s", thread.ID(), first_action)
  err := thread.Start()
  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_START_ERR: %e", err)
    return err
  }
  next_action := first_action
  for next_action != "" {
    action, exists := thread.Action(next_action)
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    ctx.Log.Logf("thread", "THREAD_ACTION: %s - %s", thread.ID(), next_action)
    next_action, err = action(ctx, thread)
    if err != nil {
      return err
    }
  }

  err = thread.Stop()
  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_STOP_ERR: %e", err)
    return err
  }

  err = UpdateStates(ctx, []GraphNode{thread}, func(nodes NodeMap) (error) {
    thread_state := thread.State().(ThreadState)
    err := thread_state.Stop()
    if err != nil {
      return err
    }
    return UnlockLockables(ctx, []Lockable{thread}, thread, nodes)
  })

  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_UNLOCK_ERR: %e", err)
    return err
  }

  ctx.Log.Logf("thread", "THREAD_LOOP_DONE: %s", thread.ID())

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
  BaseLockable

  Actions ThreadActions
  Handlers ThreadHandlers

  timeout_chan <-chan time.Time
  child_waits *sync.WaitGroup
  active bool
  active_lock *sync.Mutex
}

func (thread * BaseThread) ChildWaits() *sync.WaitGroup {
  return thread.child_waits
}

func (thread * BaseThread) Start() error {
  thread.active_lock.Lock()
  defer thread.active_lock.Unlock()
  if thread.active == true {
    return fmt.Errorf("%s is active, cannot start", thread.ID())
  }
  thread.active = true
  return nil
}

func (thread * BaseThread) Stop() error {
  thread.active_lock.Lock()
  defer thread.active_lock.Unlock()
  if thread.active == false {
    return fmt.Errorf("%s is not active, cannot stop", thread.ID())
  }
  thread.active = false
  return nil
}

func (thread * BaseThread) CanLock(node GraphNode, state LockableState) error {
  return nil
}

func (thread * BaseThread) CanUnlock(node GraphNode, state LockableState) error {
  return nil
}

func (thread * BaseThread) Lock(node GraphNode, state LockableState) {
  return
}

func (thread * BaseThread) Unlock(node GraphNode, state LockableState) {
  return
}

func (thread * BaseThread) Action(action string) (ThreadAction, bool) {
  action_fn, exists := thread.Actions[action]
  return action_fn, exists
}

func (thread * BaseThread) Handler(signal_type string) (ThreadHandler, bool) {
  handler, exists := thread.Handlers[signal_type]
  return handler, exists
}

func (thread * BaseThread) Timeout() <-chan time.Time {
  return thread.timeout_chan
}

func (thread * BaseThread) ClearTimeout() {
  thread.timeout_chan = nil
}

func (thread * BaseThread) SetTimeout(end time.Time) {
  thread.timeout_chan = time.After(time.Until(end))
}

var ThreadStart = func(ctx * GraphContext, thread Thread) error {
  err := UpdateStates(ctx, []GraphNode{thread}, func(nodes NodeMap) (error) {
    thread_state := thread.State().(ThreadState)
    owner_id := NodeID("")
    if thread_state.Owner() != nil {
      owner_id = thread_state.Owner().ID()
    }
    if owner_id != thread.ID() {
      err := LockLockables(ctx, []Lockable{thread}, thread, nodes)
      if err != nil {
        return err
      }
    }
    return thread_state.Start()
  })

  if err != nil {
    return err
  }

  return nil
}

var ThreadDefaultStart = func(ctx * GraphContext, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_DEFAULT_START: %s", thread.ID())
  err := ThreadStart(ctx, thread)
  if err != nil {
    return "", err
  }
  return "wait", nil
}

var ThreadDefaultRestore = func(ctx * GraphContext, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_DEFAULT_RESTORE: %s", thread.ID())
  return "wait", nil
}

var ThreadWait = func(ctx * GraphContext, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_WAIT: %s TIMEOUT: %+v", thread.ID(), thread.Timeout())
  for {
    select {
      case signal := <- thread.SignalChannel():
        if signal.Source() == thread.ID() {
          ctx.Log.Logf("thread", "THREAD_SIGNAL_INTERNAL")
        } else {
          ctx.Log.Logf("thread", "THREAD_SIGNAL: %s %+v", thread.ID(), signal)
        }
        signal_fn, exists := thread.Handler(signal.Type())
        if exists == true {
          ctx.Log.Logf("thread", "THREAD_HANDLER: %s - %s", thread.ID(), signal.Type())
          return signal_fn(ctx, thread, signal)
        } else {
          ctx.Log.Logf("thread", "THREAD_NOHANDLER: %s - %s", thread.ID(), signal.Type())
        }
      case <- thread.Timeout():
        timeout_action := ""
        err := UpdateStates(ctx, []GraphNode{thread}, func(nodes NodeMap) error {
          thread_state := thread.State().(ThreadState)
          timeout_action = thread_state.TimeoutAction()
          thread.ClearTimeout()
          return nil
        })
        if err != nil {
          ctx.Log.Logf("thread", "THREAD_TIMEOUT_ERR: %s - %e", thread.ID(), err)
        }
        ctx.Log.Logf("thread", "THREAD_TIMEOUT %s - NEXT_STATE: %s", thread.ID(), timeout_action)
        return timeout_action, nil
    }
  }
}

type ThreadAbortedError NodeID

func (e ThreadAbortedError) Is(target error) bool {
  error_type := reflect.TypeOf(ThreadAbortedError(""))
  target_type := reflect.TypeOf(target)
  return error_type == target_type
}
func (e ThreadAbortedError) Error() string {
  return fmt.Sprintf("Aborted by %s", string(e))
}
func NewThreadAbortedError(aborter NodeID) ThreadAbortedError {
  return ThreadAbortedError(aborter)
}

// Default thread abort is to return a ThreadAbortedError
func ThreadAbort(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
  UseStates(ctx, []GraphNode{thread}, func(states NodeStateMap) error {
    SendUpdate(ctx, thread, NewSignal(thread, "thread_aborted"), states)
    return nil
  })
  return "", NewThreadAbortedError(signal.Source())
}

// Default thread cancel is to finish the thread
func ThreadCancel(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
  UseStates(ctx, []GraphNode{thread}, func(states NodeStateMap) error {
    SendUpdate(ctx, thread, NewSignal(thread, "thread_cancelled"), states)
    return nil
  })
  return "", nil
}

func NewBaseThreadState(name string, _type string) BaseThreadState {
  return BaseThreadState{
    BaseLockableState: NewBaseLockableState(name, _type),
    children: []Thread{},
    child_info: map[NodeID]ThreadInfo{},
    parent: nil,
    timeout: time.Time{},
    timeout_action: "wait",
    state_name: "init",
  }
}

func NewThreadActions() ThreadActions{
  actions := ThreadActions{}
  for k, v := range(BaseThreadActions) {
    actions[k] = v
  }

  return actions
}

func NewThreadHandlers() ThreadHandlers{
  handlers := ThreadHandlers{}
  for k, v := range(BaseThreadHandlers) {
    handlers[k] = v
  }

  return handlers
}

var BaseThreadActions = ThreadActions{
  "wait": ThreadWait,
  "start": ThreadDefaultStart,
  "restore": ThreadDefaultRestore,
}

var BaseThreadHandlers = ThreadHandlers{
  "abort": ThreadAbort,
  "cancel": ThreadCancel,
}

func NewBaseThread(ctx * GraphContext, actions ThreadActions, handlers ThreadHandlers, state ThreadState) (BaseThread, error) {
  lockable, err := NewBaseLockable(ctx, state)
  if err != nil {
    return BaseThread{}, err
  }

  thread := BaseThread{
    BaseLockable: lockable,
    Actions: actions,
    Handlers: handlers,
    child_waits: &sync.WaitGroup{},
    active: false,
    active_lock: &sync.Mutex{},
  }

  return thread, nil
}

func NewSimpleThread(ctx * GraphContext, name string, requirements []Lockable, actions ThreadActions, handlers ThreadHandlers) (* BaseThread, error) {
  state := NewBaseThreadState(name, "simple_thread")

  thread, err := NewBaseThread(ctx, actions, handlers, &state)
  if err != nil {
    return nil, err
  }

  thread_ptr := &thread


  if len(requirements) > 0 {
    req_nodes := make([]GraphNode, len(requirements))
    for i, req := range(requirements) {
      req_nodes[i] = req
    }
    err = UpdateStates(ctx, req_nodes, func(nodes NodeMap) error {
      return LinkLockables(ctx, thread_ptr, requirements, nodes)
    })
    if err != nil {
      return nil, err
    }
  }

  return thread_ptr, nil
}
