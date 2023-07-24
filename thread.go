package graphvent

import (
  "fmt"
  "time"
  "sync"
  "errors"
  "reflect"
  "encoding/json"
)

// Assumed that thread is already locked for signal
func (thread *SimpleThread) Signal(context *StateContext, princ Node, signal GraphSignal) error {
  err := thread.SimpleLockable.Signal(context, princ, signal)
  if err != nil {
    return err
  }

  switch signal.Direction() {
  case Up:
    err = UseStates(context, thread, NewLockInfo(thread, []string{"parent"}), func(context *StateContext) error {
      if thread.parent != nil {
        return thread.parent.Signal(context, thread, signal)
      } else {
        return nil
      }
    })
  case Down:
    err = UseStates(context, thread, NewLockInfo(thread, []string{"children"}), func(context *StateContext) error {
      for _, child := range(thread.children) {
        err := child.Signal(context, thread, signal)
        if err != nil {
          return err
        }
      }
      return nil
    })
  case Direct:
    err = nil
  default:
    return fmt.Errorf("Invalid signal direction %d", signal.Direction())
  }
  if err != nil {
    return err
  }

  thread.signal <- signal
  return nil
}

// Interface to represent any type of thread information
type ThreadInfo interface {
}

func (thread * SimpleThread) SetTimeout(timeout time.Time, action string) {
  thread.timeout = timeout
  thread.timeout_action = action
  thread.timeout_chan = time.After(time.Until(timeout))
}

func (thread * SimpleThread) TimeoutAction() string {
  return thread.timeout_action
}

func (thread * SimpleThread) State() string {
  return thread.state_name
}

func (thread * SimpleThread) SetState(new_state string) error {
  if new_state == "" {
    return fmt.Errorf("Cannot set state to '' with SetState")
  }

  thread.state_name = new_state
  return nil
}

func (thread * SimpleThread) Parent() Thread {
  return thread.parent
}

func (thread * SimpleThread) SetParent(parent Thread) {
  thread.parent = parent
}

func (thread * SimpleThread) Children() []Thread {
  return thread.children
}

func (thread * SimpleThread) Child(id NodeID) Thread {
  for _, child := range(thread.children) {
    if child.ID() == id {
      return child
    }
  }
  return nil
}



func (thread * SimpleThread) ChildInfo(child NodeID) ThreadInfo {
  return thread.child_info[child]
}

// Requires thread and childs thread to be locked for write
func UnlinkThreads(ctx * Context, thread Thread, child Thread) error {
  var found Node = nil
  for _, c := range(thread.Children()) {
    if child.ID() == c.ID() {
      found = c
      break
    }
  }

  if found == nil {
    return fmt.Errorf("UNLINK_THREADS_ERR: %s is not a child of %s", child.ID(), thread.ID())
  }

  child.SetParent(nil)
  thread.RemoveChild(child)

  return nil
}

func (thread * SimpleThread) RemoveChild(child Thread) {
  idx := -1
  for i, c := range(thread.children) {
    if c.ID() == child.ID() {
      idx = i
      break
    }
  }

  if idx == -1 {
    panic(fmt.Sprintf("%s is not a child of %s", child.ID(), thread.Name()))
  }

  child_len := len(thread.children)
  thread.children[idx] = thread.children[child_len-1]
  thread.children = thread.children[0:child_len-1]
}

func (thread * SimpleThread) AddChild(child Thread, info ThreadInfo) error {
  if child == nil {
    return fmt.Errorf("Will not connect nil to the thread tree")
  }

  _, exists := thread.child_info[child.ID()]
  if exists == true {
    return fmt.Errorf("Will not connect the same child twice")
  }

  if info == nil && thread.InfoType != nil {
    return fmt.Errorf("nil info passed when expecting info")
  } else if info != nil {
    if reflect.TypeOf(info) != thread.InfoType {
      return fmt.Errorf("info type mismatch, expecting %+v - %+v", thread.InfoType, reflect.TypeOf(info))
    }
  }

  thread.children = append(thread.children, child)
  thread.child_info[child.ID()] = info

  return nil
}

func checkIfChild(context *StateContext, target Thread, cur Thread) bool {
  for _, child := range(cur.Children()) {
    if child.ID() == target.ID() {
      return true
    }
    is_child := false
    UpdateStates(context, cur, NewLockMap(
      NewLockInfo(child, []string{"children"}),
    ), func(context *StateContext) error {
      is_child = checkIfChild(context, target, child)
      return nil
    })
    if is_child {
      return true
    }
  }

  return false
}

// Links child to parent with info as the associated info
// Continues the write context with princ, getting children for thread and parent for child
func LinkThreads(context *StateContext, princ Node, thread Thread, child Thread, info ThreadInfo) error {
  if context == nil || thread == nil || child == nil {
    return fmt.Errorf("invalid input")
  }

  if thread.ID() == child.ID() {
    return fmt.Errorf("Will not link %s as a child of itself", thread.ID())
  }

  return UpdateStates(context, princ, LockMap{
    child.ID(): LockInfo{Node: child, Resources: []string{"parent"}},
    thread.ID(): LockInfo{Node: thread, Resources: []string{"children"}},
  }, func(context *StateContext) error {
    if child.Parent() != nil {
      return fmt.Errorf("EVENT_LINK_ERR: %s already has a parent, cannot link as child", child.ID())
    }

    if checkIfChild(context, thread, child) == true {
      return fmt.Errorf("EVENT_LINK_ERR: %s is a child of %s so cannot add as parent", thread.ID(), child.ID())
    }

    if checkIfChild(context, child, thread) == true {
      return fmt.Errorf("EVENT_LINK_ERR: %s is already a parent of %s so will not add again", thread.ID(), child.ID())
    }

    err := thread.AddChild(child, info)
    if err != nil {
      return fmt.Errorf("EVENT_LINK_ERR: error adding %s as child to %s: %e", child.ID(), thread.ID(), err)
    }
    child.SetParent(thread)

    if err != nil {
      return err
    }

    return nil
  })
}

type ThreadAction func(* Context, Thread)(string, error)
type ThreadActions map[string]ThreadAction
type ThreadHandler func(* Context, Thread, GraphSignal)(string, error)
type ThreadHandlers map[string]ThreadHandler

type Thread interface {
  // All Threads are Lockables
  Lockable
  /// State Modification Functions
  SetParent(parent Thread)
  AddChild(child Thread, info ThreadInfo) error
  RemoveChild(child Thread)
  SetState(new_thread string) error
  SetTimeout(end_time time.Time, action string)
  /// State Reading Functions
  Parent() Thread
  Children() []Thread
  Child(id NodeID) Thread
  ChildInfo(child NodeID) ThreadInfo
  State() string
  TimeoutAction() string

  /// Functions that dont read/write thread
  // Deserialize the attribute map from json.Unmarshal
  DeserializeInfo(ctx *Context, data []byte) (ThreadInfo, error)
  SetActive(active bool) error
  Action(action string) (ThreadAction, bool)
  Handler(signal_type string) (ThreadHandler, bool)

  // Internal timeout channel for thread
  Timeout() <-chan time.Time
  // Internal signal channel for thread
  SignalChannel() <-chan GraphSignal
  ClearTimeout()

  ChildWaits() *sync.WaitGroup
}

type ParentInfo interface {
  Parent() *ParentThreadInfo
}

// Data required by a parent thread to restore it's children
type ParentThreadInfo struct {
  Start bool `json:"start"`
  StartAction string `json:"start_action"`
  RestoreAction string `json:"restore_action"`
}

func (info * ParentThreadInfo) Parent() *ParentThreadInfo{
  return info
}

func NewParentThreadInfo(start bool, start_action string, restore_action string) ParentThreadInfo {
  return ParentThreadInfo{
    Start: start,
    StartAction: start_action,
    RestoreAction: restore_action,
  }
}

type SimpleThread struct {
  SimpleLockable

  actions ThreadActions
  handlers ThreadHandlers

  timeout_chan <-chan time.Time
  signal chan GraphSignal
  child_waits *sync.WaitGroup
  active bool
  active_lock *sync.Mutex

  state_name string
  parent Thread
  children []Thread
  child_info map[NodeID] ThreadInfo
  InfoType reflect.Type
  timeout time.Time
  timeout_action string
}

func (thread * SimpleThread) Type() NodeType {
  return NodeType("simple_thread")
}

func (thread * SimpleThread) Serialize() ([]byte, error) {
  thread_json := NewSimpleThreadJSON(thread)
  return json.MarshalIndent(&thread_json, "", "  ")
}

func (thread * SimpleThread) SignalChannel() <-chan GraphSignal {
  return thread.signal
}

type SimpleThreadJSON struct {
  Parent string `json:"parent"`
  Children map[string]interface{} `json:"children"`
  Timeout time.Time `json:"timeout"`
  TimeoutAction string `json:"timeout_action"`
  StateName string `json:"state_name"`
  SimpleLockableJSON
}

func NewSimpleThreadJSON(thread *SimpleThread) SimpleThreadJSON {
  children := map[string]interface{}{}
  for _, child := range(thread.children) {
    children[child.ID().String()] = thread.child_info[child.ID()]
  }

  parent_id := ""
  if thread.parent != nil {
    parent_id = thread.parent.ID().String()
  }

  lockable_json := NewSimpleLockableJSON(&thread.SimpleLockable)

  return SimpleThreadJSON{
    Parent: parent_id,
    Children: children,
    Timeout: thread.timeout,
    TimeoutAction: thread.timeout_action,
    StateName: thread.state_name,
    SimpleLockableJSON: lockable_json,
  }
}

func LoadSimpleThread(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j SimpleThreadJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  thread := NewSimpleThread(id, j.Name, j.StateName, nil, BaseThreadActions, BaseThreadHandlers)
  nodes[id] = &thread

  err = RestoreSimpleThread(ctx, &thread, j, nodes)
  if err != nil {
    return nil, err
  }

  return &thread, nil
}

// SimpleThread has no associated info with children
func (thread * SimpleThread) DeserializeInfo(ctx *Context, data []byte) (ThreadInfo, error) {
  if len(data) > 0 {
    return nil, fmt.Errorf("SimpleThread expected to deserialize no info but got %d length data: %s", len(data), string(data))
  }
  return nil, nil
}

func RestoreSimpleThread(ctx *Context, thread Thread, j SimpleThreadJSON, nodes NodeMap) error {
  if j.TimeoutAction != "" {
    thread.SetTimeout(j.Timeout, j.TimeoutAction)
  }

  if j.Parent != "" {
    parent_id, err := ParseID(j.Parent)
    if err != nil {
      return err
    }
    p, err := LoadNodeRecurse(ctx, parent_id, nodes)
    if err != nil {
      return err
    }
    p_t, ok := p.(Thread)
    if ok == false {
      return err
    }
    thread.SetParent(p_t)
  }

  for id_str, info_raw := range(j.Children) {
    id, err := ParseID(id_str)
    if err != nil {
      return err
    }
    child_node, err := LoadNodeRecurse(ctx, id, nodes)
    if err != nil {
      return err
    }
    child_t, ok := child_node.(Thread)
    if ok == false {
      return fmt.Errorf("%+v is not a Thread as expected", child_node)
    }

    var info_ser []byte
    if info_raw != nil {
      info_ser, err = json.Marshal(info_raw)
      if err != nil {
        return err
      }
    }

    parsed_info, err := thread.DeserializeInfo(ctx, info_ser)
    if err != nil {
      return err
    }

    thread.AddChild(child_t, parsed_info)
  }

  return RestoreSimpleLockable(ctx, thread, j.SimpleLockableJSON, nodes)
}

const THREAD_SIGNAL_BUFFER_SIZE = 128
func NewSimpleThread(id NodeID, name string, state_name string, info_type reflect.Type, actions ThreadActions, handlers ThreadHandlers) SimpleThread {
  return SimpleThread{
    SimpleLockable: NewSimpleLockable(id, name),
    InfoType: info_type,
    state_name: state_name,
    signal: make(chan GraphSignal, THREAD_SIGNAL_BUFFER_SIZE),
    children: []Thread{},
    child_info: map[NodeID]ThreadInfo{},
    actions: actions,
    handlers: handlers,
    child_waits: &sync.WaitGroup{},
    active_lock: &sync.Mutex{},
  }
}

// Requires the read permission of threads children
func FindChild(context *StateContext, princ Node, thread Thread, id NodeID) Thread {
  if thread == nil {
    panic("cannot recurse through nil")
  }
  if id ==  thread.ID() {
    return thread
  }

  for _, child := range thread.Children() {
    var result Thread = nil
    UseStates(context, princ, NewLockInfo(child, []string{"children"}), func(context *StateContext) error {
      result = FindChild(context, princ, child, id)
      return nil
    })
    if result != nil {
      return result
    }
  }

  return nil
}

func ChildGo(ctx * Context, thread Thread, child Thread, first_action string) {
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

// Main Loop for Threads, starts a write context, so cannot be called from a write or read context
func ThreadLoop(ctx * Context, thread Thread, first_action string) error {
  // Start the thread, error if double-started
  ctx.Log.Logf("thread", "THREAD_LOOP_START: %s - %s", thread.ID(), first_action)
  err := thread.SetActive(true)
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

  err = thread.SetActive(false)
  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_STOP_ERR: %e", err)
    return err
  }




  ctx.Log.Logf("thread", "THREAD_LOOP_DONE: %s", thread.ID())

  return nil
}


func (thread * SimpleThread) ChildWaits() *sync.WaitGroup {
  return thread.child_waits
}

func (thread * SimpleThread) SetActive(active bool) error {
  thread.active_lock.Lock()
  defer thread.active_lock.Unlock()
  if thread.active == true && active == true {
    return fmt.Errorf("%s is active, cannot set active", thread.ID())
  } else if thread.active == false && active == false {
    return fmt.Errorf("%s is already inactive, canot set inactive", thread.ID())
  }
  thread.active = active
  return nil
}

func (thread * SimpleThread) Action(action string) (ThreadAction, bool) {
  action_fn, exists := thread.actions[action]
  return action_fn, exists
}

func (thread * SimpleThread) Handler(signal_type string) (ThreadHandler, bool) {
  handler, exists := thread.handlers[signal_type]
  return handler, exists
}

func (thread * SimpleThread) Timeout() <-chan time.Time {
  return thread.timeout_chan
}

func (thread * SimpleThread) ClearTimeout() {
  thread.timeout_chan = nil
  thread.timeout_action = ""
}

func (thread * SimpleThread) AllowedToTakeLock(new_owner Lockable, lockable Lockable) bool {
  for _, child := range(thread.children) {
    if new_owner.ID() == child.ID() {
      return true
    }
  }
  return false
}

// Helper function to start a child from a thread during a signal handler
// Starts a write context, so cannot be called from either a write or read context
func ThreadStartChild(ctx *Context, thread Thread, signal StartChildSignal) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewLockInfo(thread, []string{"children"}), func(context *StateContext) error {
    child := thread.Child(signal.ID)
    if child == nil {
      return fmt.Errorf("%s is not a child of %s", signal.ID, thread.ID())
    }
    return UpdateStates(context, thread, NewLockInfo(child, []string{"start"}), func(context *StateContext) error {

      info := thread.ChildInfo(signal.ID).(*ParentThreadInfo)
      info.Start = true
      ChildGo(ctx, thread, child, signal.Action)

      return nil
    })
  })
}

// Helper function to restore threads that should be running from a parents restore action
// Starts a write context, so cannot be called from either a write or read context
func ThreadRestore(ctx * Context, thread Thread) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewLockInfo(thread, []string{"children"}), func(context *StateContext) error {
    return UpdateStates(context, thread, LockList(thread.Children(), []string{"start"}), func(context *StateContext) error {
      for _, child := range(thread.Children()) {
        should_run := (thread.ChildInfo(child.ID())).(ParentInfo).Parent()
        ctx.Log.Logf("thread", "THREAD_RESTORE: %s -> %s: %+v", thread.ID(), child.ID(), should_run)
        if should_run.Start == true && child.State() != "finished" {
          ctx.Log.Logf("thread", "THREAD_RESTORED: %s -> %s", thread.ID(), child.ID())
          ChildGo(ctx, thread, child, should_run.RestoreAction)
        }
      }
      return nil
    })
  })
}

// Helper function to be called during a threads start action, sets the thread state to started
// Starts a write context, so cannot be called from either a write or read context
func ThreadStart(ctx * Context, thread Thread) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewLockInfo(thread, []string{"state"}), func(context *StateContext) error {
    err := LockLockables(context, []Lockable{thread}, thread)
    if err != nil {
      return err
    }
    return thread.SetState("started")
  })
}

func ThreadDefaultStart(ctx * Context, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_DEFAULT_START: %s", thread.ID())
  err := ThreadStart(ctx, thread)
  if err != nil {
    return "", err
  }
  return "wait", nil
}

func ThreadDefaultRestore(ctx * Context, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_DEFAULT_RESTORE: %s", thread.ID())
  return "wait", nil
}

func ThreadWait(ctx * Context, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_WAIT: %s TIMEOUT: %+v", thread.ID(), thread.Timeout())
  for {
    select {
      case signal := <- thread.SignalChannel():
        ctx.Log.Logf("thread", "THREAD_SIGNAL: %s %+v", thread.ID(), signal)
        signal_fn, exists := thread.Handler(signal.Type())
        if exists == true {
          ctx.Log.Logf("thread", "THREAD_HANDLER: %s - %s", thread.ID(), signal.Type())
          return signal_fn(ctx, thread, signal)
        } else {
          ctx.Log.Logf("thread", "THREAD_NOHANDLER: %s - %s", thread.ID(), signal.Type())
        }
      case <- thread.Timeout():
        timeout_action := ""
        context := NewWriteContext(ctx)
        err := UpdateStates(context, thread, NewLockMap(NewLockInfo(thread, []string{"timeout"})), func(context *StateContext) error {
          timeout_action = thread.TimeoutAction()
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

func ThreadDefaultFinish(ctx *Context, thread Thread) (string, error) {
  ctx.Log.Logf("thread", "THREAD_DEFAULT_FINISH: %s", thread.ID().String())
  return "", ThreadFinish(ctx, thread)
}

func ThreadFinish(ctx *Context, thread Thread) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewLockInfo(thread, []string{"state"}), func(context *StateContext) error {
    err := thread.SetState("finished")
    if err != nil {
      return err
    }
    return UnlockLockables(context, []Lockable{thread}, thread)
  })
}

var ThreadAbortedError = errors.New("Thread aborted by signal")

// Default thread action function for "abort", sends a signal and returns a ThreadAbortedError
func ThreadAbort(ctx * Context, thread Thread, signal GraphSignal) (string, error) {
  context := NewReadContext(ctx)
  err := thread.Signal(context, thread, NewStatusSignal("aborted", thread.ID()))
  if err != nil {
    return "", err
  }
  return "", ThreadAbortedError
}

// Default thread action for "stop", sends a signal and returns no error
func ThreadStop(ctx * Context, thread Thread, signal GraphSignal) (string, error) {
  context := NewReadContext(ctx)
  err := thread.Signal(context, thread, NewStatusSignal("stopped", thread.ID()))
  return "finish", err
}

// Copy the default thread actions to a new ThreadActions map
func NewThreadActions() ThreadActions{
  actions := ThreadActions{}
  for k, v := range(BaseThreadActions) {
    actions[k] = v
  }

  return actions
}

// Copy the defult thread handlers to a new ThreadAction map
func NewThreadHandlers() ThreadHandlers{
  handlers := ThreadHandlers{}
  for k, v := range(BaseThreadHandlers) {
    handlers[k] = v
  }

  return handlers
}

// Default thread actions
var BaseThreadActions = ThreadActions{
  "wait": ThreadWait,
  "start": ThreadDefaultStart,
  "finish": ThreadDefaultFinish,
  "restore": ThreadDefaultRestore,
}

// Default thread signal handlers
var BaseThreadHandlers = ThreadHandlers{
  "abort": ThreadAbort,
  "stop": ThreadStop,
}
