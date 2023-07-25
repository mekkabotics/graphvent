package graphvent

import (
  "fmt"
  "time"
  "sync"
  "errors"
  "encoding/json"
)

// Assumed that thread is already locked for signal
func (thread *Thread) Process(context *StateContext, signal GraphSignal) error {
  context.Graph.Log.Logf("signal", "THREAD_PROCESS: %s", thread.ID())

  var err error
  switch signal.Direction() {
  case Up:
    err = UseStates(context, thread, NewLockInfo(thread, []string{"parent"}), func(context *StateContext) error {
      if thread.Parent != nil {
        return Signal(context, thread.Parent, thread, signal)
      } else {
        return nil
      }
    })
  case Down:
    err = UseStates(context, thread, NewLockInfo(thread, []string{"children"}), func(context *StateContext) error {
      for _, info := range(thread.Children) {
        err := Signal(context, info.Child, thread, signal)
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

  thread.Chan <- signal
  return thread.Lockable.Process(context, signal)
}

// Requires thread and childs thread to be locked for write
func UnlinkThreads(ctx * Context, node ThreadNode, child_node ThreadNode) error {
  thread := node.ThreadHandle()
  child := child_node.ThreadHandle()
  _, is_child := thread.Children[child_node.ID()]
  if is_child == false {
    return fmt.Errorf("UNLINK_THREADS_ERR: %s is not a child of %s", child.ID(), thread.ID())
  }

  child.Parent = nil
  delete(thread.Children, child.ID())

  return nil
}

func checkIfChild(context *StateContext, target ThreadNode, cur ThreadNode) bool {
  for _, info := range(cur.ThreadHandle().Children) {
    if info.Child.ID() == target.ID() {
      return true
    }
    is_child := false
    UpdateStates(context, cur, NewLockMap(
      NewLockInfo(info.Child, []string{"children"}),
    ), func(context *StateContext) error {
      is_child = checkIfChild(context, target, info.Child)
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
func LinkThreads(context *StateContext, princ Node, thread_node ThreadNode, info ChildInfo) error {
  if context == nil || thread_node == nil || info.Child == nil {
    return fmt.Errorf("invalid input")
  }
  thread := thread_node.ThreadHandle()
  child := info.Child.ThreadHandle()

  if thread.ID() == child.ID() {
    return fmt.Errorf("Will not link %s as a child of itself", thread.ID())
  }

  return UpdateStates(context, princ, LockMap{
    child.ID(): LockInfo{Node: child, Resources: []string{"parent"}},
    thread.ID(): LockInfo{Node: thread, Resources: []string{"children"}},
  }, func(context *StateContext) error {
    if child.Parent != nil {
      return fmt.Errorf("EVENT_LINK_ERR: %s already has a parent, cannot link as child", child.ID())
    }

    if checkIfChild(context, thread, child) == true {
      return fmt.Errorf("EVENT_LINK_ERR: %s is a child of %s so cannot add as parent", thread.ID(), child.ID())
    }

    if checkIfChild(context, child, thread) == true {
      return fmt.Errorf("EVENT_LINK_ERR: %s is already a parent of %s so will not add again", thread.ID(), child.ID())
    }

    // TODO check for info types

    thread.Children[child.ID()] = info
    child.Parent = thread_node

    return nil
  })
}

type ThreadAction func(*Context, ThreadNode)(string, error)
type ThreadActions map[string]ThreadAction
type ThreadHandler func(*Context, ThreadNode, GraphSignal)(string, error)
type ThreadHandlers map[string]ThreadHandler

type InfoType string
func (t InfoType) String() string {
  return string(t)
}

// Data required by a parent thread to restore it's children
type ParentThreadInfo struct {
  Start bool `json:"start"`
  StartAction string `json:"start_action"`
  RestoreAction string `json:"restore_action"`
}

func NewParentThreadInfo(start bool, start_action string, restore_action string) ParentThreadInfo {
  return ParentThreadInfo{
    Start: start,
    StartAction: start_action,
    RestoreAction: restore_action,
  }
}

type ChildInfo struct {
  Child ThreadNode
  Infos map[InfoType]interface{}
}

func NewChildInfo(child ThreadNode, infos map[InfoType]interface{}) ChildInfo {
  if infos == nil {
    infos = map[InfoType]interface{}{}
  }

  return ChildInfo{
    Child: child,
    Infos: infos,
  }
}

type QueuedAction struct {
  Timeout time.Time `json:"time"`
  Action string `json:"action"`
}

type ThreadNode interface {
  LockableNode
  ThreadHandle() *Thread
}

type Thread struct {
  Lockable

  Actions ThreadActions
  Handlers ThreadHandlers

  TimeoutChan <-chan time.Time
  Chan chan GraphSignal
  ChildWaits sync.WaitGroup
  Active bool
  ActiveLock sync.Mutex

  StateName string
  Parent ThreadNode
  Children map[NodeID]ChildInfo
  InfoTypes []InfoType
  ActionQueue []QueuedAction
  NextAction *QueuedAction
}

func (thread *Thread) QueueAction(end time.Time, action string) {
  thread.ActionQueue = append(thread.ActionQueue, QueuedAction{end, action})
  thread.NextAction, thread.TimeoutChan = thread.SoonestAction()
}

func (thread *Thread) ClearActionQueue() {
  thread.ActionQueue = []QueuedAction{}
  thread.NextAction = nil
  thread.TimeoutChan = nil
}

func (thread *Thread) ThreadHandle() *Thread {
  return thread
}

func (thread *Thread) Type() NodeType {
  return NodeType("thread")
}

func (thread *Thread) Serialize() ([]byte, error) {
  thread_json := NewThreadJSON(thread)
  return json.MarshalIndent(&thread_json, "", "  ")
}

func (thread *Thread) ChildList() []ThreadNode {
  ret := make([]ThreadNode, len(thread.Children))
  i := 0
  for _, info := range(thread.Children) {
    ret[i] = info.Child
    i += 1
  }
  return ret
}

type ThreadJSON struct {
  Parent string `json:"parent"`
  Children map[string]map[string]interface{} `json:"children"`
  ActionQueue []QueuedAction `json:"action_queue"`
  StateName string `json:"state_name"`
  LockableJSON
}

func NewThreadJSON(thread *Thread) ThreadJSON {
  children := map[string]map[string]interface{}{}
  for id, info := range(thread.Children) {
    tmp := map[string]interface{}{}
    for name, i := range(info.Infos) {
      tmp[name.String()] = i
    }
    children[id.String()] = tmp
  }

  parent_id := ""
  if thread.Parent != nil {
    parent_id = thread.Parent.ID().String()
  }

  lockable_json := NewLockableJSON(&thread.Lockable)

  return ThreadJSON{
    Parent: parent_id,
    Children: children,
    ActionQueue: thread.ActionQueue,
    StateName: thread.StateName,
    LockableJSON: lockable_json,
  }
}

func LoadThread(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j ThreadJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  thread := NewThread(id, j.Name, j.StateName, nil, BaseThreadActions, BaseThreadHandlers)
  nodes[id] = &thread

  err = RestoreThread(ctx, &thread, j, nodes)
  if err != nil {
    return nil, err
  }

  return &thread, nil
}

func (thread *Thread) SoonestAction() (*QueuedAction, <-chan time.Time) {
  var soonest_action *QueuedAction
  var soonest_time time.Time
  for _, action := range(thread.ActionQueue) {
    if action.Timeout.Compare(soonest_time) == -1 || soonest_action == nil {
      soonest_action = &action
      soonest_time = action.Timeout
    }
  }
  if soonest_action != nil {
    return soonest_action, time.After(time.Until(soonest_action.Timeout))
  } else {
    return nil, nil
  }
}

func RestoreThread(ctx *Context, thread *Thread, j ThreadJSON, nodes NodeMap) error {
  thread.ActionQueue = j.ActionQueue
  thread.NextAction, thread.TimeoutChan = thread.SoonestAction()

  if j.Parent != "" {
    parent_id, err := ParseID(j.Parent)
    if err != nil {
      return err
    }
    p, err := LoadNodeRecurse(ctx, parent_id, nodes)
    if err != nil {
      return err
    }
    p_t, ok := p.(ThreadNode)
    if ok == false {
      return err
    }
    thread.Parent = p_t
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

    child_t, ok := child_node.(ThreadNode)
    if ok == false {
      return fmt.Errorf("%+v is not a Thread as expected", child_node)
    }

    parsed_info, err := DeserializeChildInfo(ctx, info_raw)
    if err != nil {
      return err
    }

    thread.Children[id] = ChildInfo{child_t, parsed_info}
  }

  return RestoreLockable(ctx, &thread.Lockable, j.LockableJSON, nodes)
}

var deserializers = map[InfoType]func(interface{})(interface{}, error) {
  "parent": func(raw interface{})(interface{}, error) {
    m, ok := raw.(map[string]interface{})
    if ok == false {
      return nil, fmt.Errorf("Failed to cast parent info to map")
    }
    start, ok := m["start"].(bool)
    if ok == false {
      return nil, fmt.Errorf("Failed to get start from parent info")
    }
    start_action, ok := m["start_action"].(string)
    if ok == false {
      return nil, fmt.Errorf("Failed to get start_action from parent info")
    }
    restore_action, ok := m["restore_action"].(string)
    if ok == false {
      return nil, fmt.Errorf("Failed to get restore_action from parent info")
    }

    return &ParentThreadInfo{
      Start: start,
      StartAction: start_action,
      RestoreAction: restore_action,
    }, nil
  },
}

func DeserializeChildInfo(ctx *Context, infos_raw map[string]interface{}) (map[InfoType]interface{}, error) {
  ret := map[InfoType]interface{}{}
  for type_str, info_raw := range(infos_raw) {
    info_type := InfoType(type_str)
    deserializer, exists := deserializers[info_type]
    if exists == false {
      return nil, fmt.Errorf("No deserializer for %s", info_type)
    }
    var err error
    ret[info_type], err = deserializer(info_raw)
    if err != nil {
      return nil, err
    }
  }

  return ret, nil
}

const THREAD_SIGNAL_BUFFER_SIZE = 128
func NewThread(id NodeID, name string, state_name string, info_types []InfoType, actions ThreadActions, handlers ThreadHandlers) Thread {
  return Thread{
    Lockable: NewLockable(id, name),
    InfoTypes: info_types,
    StateName: state_name,
    Chan: make(chan GraphSignal, THREAD_SIGNAL_BUFFER_SIZE),
    Children: map[NodeID]ChildInfo{},
    Actions: actions,
    Handlers: handlers,
  }
}

func (thread *Thread) SetActive(active bool) error {
  thread.ActiveLock.Lock()
  defer thread.ActiveLock.Unlock()
  if thread.Active == true && active == true {
    return fmt.Errorf("%s is active, cannot set active", thread.ID())
  } else if thread.Active == false && active == false {
    return fmt.Errorf("%s is already inactive, canot set inactive", thread.ID())
  }
  thread.Active = active
  return nil
}

func (thread *Thread) SetState(state string) error {
  thread.StateName = state
  return nil
}

// Requires the read permission of threads children
func FindChild(context *StateContext, princ Node, node ThreadNode, id NodeID) ThreadNode {
  if node == nil {
    panic("cannot recurse through nil")
  }
  thread := node.ThreadHandle()
  if id ==  thread.ID() {
    return thread
  }

  for _, info := range thread.Children {
    var result ThreadNode
    UseStates(context, princ, NewLockInfo(info.Child, []string{"children"}), func(context *StateContext) error {
      result = FindChild(context, princ, info.Child, id)
      return nil
    })
    if result != nil {
      return result
    }
  }

  return nil
}

func ChildGo(ctx * Context, thread *Thread, child ThreadNode, first_action string) {
  thread.ChildWaits.Add(1)
  go func(child ThreadNode) {
    ctx.Log.Logf("thread", "THREAD_START_CHILD: %s from %s", thread.ID(), child.ID())
    defer thread.ChildWaits.Done()
    err := ThreadLoop(ctx, child, first_action)
    if err != nil {
      ctx.Log.Logf("thread", "THREAD_CHILD_RUN_ERR: %s %s", child.ID(), err)
    } else {
      ctx.Log.Logf("thread", "THREAD_CHILD_RUN_DONE: %s", child.ID())
    }
  }(child)
}

// Main Loop for Threads, starts a write context, so cannot be called from a write or read context
func ThreadLoop(ctx * Context, node ThreadNode, first_action string) error {
  // Start the thread, error if double-started
  thread := node.ThreadHandle()
  ctx.Log.Logf("thread", "THREAD_LOOP_START: %s - %s", thread.ID(), first_action)
  err := thread.SetActive(true)
  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_START_ERR: %e", err)
    return err
  }
  next_action := first_action
  for next_action != "" {
    action, exists := thread.Actions[next_action]
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    ctx.Log.Logf("thread", "THREAD_ACTION: %s - %s", thread.ID(), next_action)
    next_action, err = action(ctx, node)
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

func ThreadChildLinked(ctx *Context, node ThreadNode, signal GraphSignal) (string, error) {
  thread := node.ThreadHandle()
  ctx.Log.Logf("thread", "THREAD_CHILD_LINKED: %+v", signal)
  context := NewWriteContext(ctx)
  err := UpdateStates(context, thread, NewLockMap(
    NewLockInfo(thread, []string{"children"}),
  ), func(context *StateContext) error {
    sig, ok := signal.(IDSignal)
    if ok == false {
      ctx.Log.Logf("thread", "THREAD_NODE_LINKED_BAD_CAST")
      return nil
    }
    info, exists := thread.Children[sig.ID]
    if exists == false {
      ctx.Log.Logf("thread", "THREAD_NODE_LINKED: %s is not a child of %s", sig.ID)
      return nil
    }
    parent_info, exists := info.Infos["parent"].(*ParentThreadInfo)
    if exists == false {
      panic("ran ThreadChildLinked from a thread that doesn't require 'parent' child info. library party foul")
    }

    if parent_info.Start == true {
      ChildGo(ctx, thread, info.Child, parent_info.StartAction)
    }
    return nil
  })

  if err != nil {

  } else {

  }
  return "wait", nil
}

// Helper function to start a child from a thread during a signal handler
// Starts a write context, so cannot be called from either a write or read context
func ThreadStartChild(ctx *Context, node ThreadNode, signal GraphSignal) (string, error) {
  sig, ok := signal.(StartChildSignal)
  if ok == false {
    return "wait", nil
  }

  thread := node.ThreadHandle()

  context := NewWriteContext(ctx)
  return "wait", UpdateStates(context, thread, NewLockInfo(thread, []string{"children"}), func(context *StateContext) error {
    info, exists:= thread.Children[sig.ID]
    if exists == false {
      return fmt.Errorf("%s is not a child of %s", sig.ID, thread.ID())
    }
    return UpdateStates(context, thread, NewLockInfo(info.Child, []string{"start"}), func(context *StateContext) error {

      parent_info, exists := info.Infos["parent"].(*ParentThreadInfo)
      if exists == false {
        return fmt.Errorf("Called ThreadStartChild from a thread that doesn't require parent child info")
      }
      parent_info.Start = true
      ChildGo(ctx, thread, info.Child, sig.Action)

      return nil
    })
  })
}

// Helper function to restore threads that should be running from a parents restore action
// Starts a write context, so cannot be called from either a write or read context
func ThreadRestore(ctx * Context, node ThreadNode, start bool) error {
  thread := node.ThreadHandle()
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewLockInfo(thread, []string{"children"}), func(context *StateContext) error {
    return UpdateStates(context, thread, LockList(thread.ChildList(), []string{"start"}), func(context *StateContext) error {
      for _, info := range(thread.Children) {
        parent_info := info.Infos["parent"].(*ParentThreadInfo)
        if parent_info.Start == true && info.Child.ThreadHandle().StateName != "finished" {
          ctx.Log.Logf("thread", "THREAD_RESTORED: %s -> %s", thread.ID(), info.Child.ID())
          if start == true {
            ChildGo(ctx, thread, info.Child, parent_info.StartAction)
          } else {
            ChildGo(ctx, thread, info.Child, parent_info.RestoreAction)
          }
        }
      }
      return nil
    })
  })
}

// Helper function to be called during a threads start action, sets the thread state to started
// Starts a write context, so cannot be called from either a write or read context
// Returns "wait", nil on success, so the first return value can be ignored safely
func ThreadStart(ctx * Context, node ThreadNode) (string, error) {
  thread := node.ThreadHandle()
  context := NewWriteContext(ctx)
  err := UpdateStates(context, thread, NewLockInfo(thread, []string{"state"}), func(context *StateContext) error {
    err := LockLockables(context, map[NodeID]LockableNode{thread.ID(): thread}, thread)
    if err != nil {
      return err
    }
    return thread.SetState("started")
  })
  if err != nil {
    return "", err
  }

  context = NewReadContext(ctx)
  return "wait", Signal(context, thread, thread, NewStatusSignal("started", thread.ID()))
}

func ThreadWait(ctx * Context, node ThreadNode) (string, error) {
  thread := node.ThreadHandle()
  ctx.Log.Logf("thread", "THREAD_WAIT: %s - %+v", thread.ID(), thread.ActionQueue)
  for {
    select {
      case signal := <- thread.Chan:
        ctx.Log.Logf("thread", "THREAD_SIGNAL: %s %+v", thread.ID(), signal)
        signal_fn, exists := thread.Handlers[signal.Type()]
        if exists == true {
          ctx.Log.Logf("thread", "THREAD_HANDLER: %s - %s", thread.ID(), signal.Type())
          return signal_fn(ctx, node, signal)
        } else {
          ctx.Log.Logf("thread", "THREAD_NOHANDLER: %s - %s", thread.ID(), signal.Type())
        }
      case <- thread.TimeoutChan:
        timeout_action := ""
        context := NewWriteContext(ctx)
        err := UpdateStates(context, thread, NewLockMap(NewLockInfo(thread, []string{"timeout"})), func(context *StateContext) error {
          timeout_action = thread.NextAction.Action
          thread.NextAction, thread.TimeoutChan = thread.SoonestAction()
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

func ThreadFinish(ctx *Context, node ThreadNode) (string, error) {
  thread := node.ThreadHandle()
  context := NewWriteContext(ctx)
  return "", UpdateStates(context, thread, NewLockInfo(thread, []string{"state"}), func(context *StateContext) error {
    err := thread.SetState("finished")
    if err != nil {
      return err
    }
    return UnlockLockables(context, map[NodeID]LockableNode{thread.ID(): thread}, thread)
  })
}

var ThreadAbortedError = errors.New("Thread aborted by signal")

// Default thread action function for "abort", sends a signal and returns a ThreadAbortedError
func ThreadAbort(ctx * Context, node ThreadNode, signal GraphSignal) (string, error) {
  thread := node.ThreadHandle()
  context := NewReadContext(ctx)
  err := Signal(context, thread, thread, NewStatusSignal("aborted", thread.ID()))
  if err != nil {
    return "", err
  }
  return "", ThreadAbortedError
}

// Default thread action for "stop", sends a signal and returns no error
func ThreadStop(ctx * Context, node ThreadNode, signal GraphSignal) (string, error) {
  thread := node.ThreadHandle()
  context := NewReadContext(ctx)
  err := Signal(context, thread, thread, NewStatusSignal("stopped", thread.ID()))
  return "finish", err
}

// Default thread actions
var BaseThreadActions = ThreadActions{
  "wait": ThreadWait,
  "start": ThreadStart,
  "finish": ThreadFinish,
}

// Default thread signal handlers
var BaseThreadHandlers = ThreadHandlers{
  "abort": ThreadAbort,
  "stop": ThreadStop,
}
