package graphvent

import (
  "fmt"
  "time"
  "sync"
  "errors"
  "encoding/json"
)

type ThreadExt struct {
  Actions ThreadActions
  Handlers ThreadHandlers

  SignalChan chan GraphSignal
  TimeoutChan <-chan time.Time

  ChildWaits sync.WaitGroup

  ActiveLock sync.Mutex
  Active bool
  StateName string

  Parent *Node
  Children map[NodeID]ChildInfo

  ActionQueue []QueuedAction
  NextAction *QueuedAction
}

func (ext *ThreadExt) Serialize() ([]byte, error) {
  return nil, fmt.Errorf("NOT_IMPLEMENTED")
}

const ThreadExtType = ExtType("THREAD")
func (ext *ThreadExt) Type() ExtType {
  return ThreadExtType
}

func (ext *ThreadExt) ChildList() []*Node {
  ret := make([]*Node, len(ext.Children))
  i := 0
  for _, info := range(ext.Children) {
    ret[i] = info.Child
    i += 1
  }

  return ret
}

// Assumed that thread is already locked for signal
func (ext *ThreadExt) Process(context *StateContext, node *Node, signal GraphSignal) error {
  context.Graph.Log.Logf("signal", "THREAD_PROCESS: %s", node.ID)

  var err error
  switch signal.Direction() {
  case Up:
    err = UseStates(context, node, NewACLInfo(node, []string{"parent"}), func(context *StateContext) error {
      if ext.Parent != nil {
        return Signal(context, ext.Parent, node, signal)
      } else {
        return nil
      }
    })
  case Down:
    err = UseStates(context, node, NewACLInfo(node, []string{"children"}), func(context *StateContext) error {
      for _, info := range(ext.Children) {
        err := Signal(context, info.Child, node, signal)
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
  ext.SignalChan <- signal
  return err
}

func UnlinkThreads(context *StateContext, principal *Node, thread *Node, child *Node) error {
  thread_ext, err := GetExt[*ThreadExt](thread)
  if err != nil {
    return err
  }

  child_ext, err := GetExt[*ThreadExt](child)
  if err != nil {
    return err
  }

  return UpdateStates(context, principal, ACLMap{
    thread.ID: ACLInfo{thread, []string{"children"}},
    child.ID: ACLInfo{child, []string{"parent"}},
  }, func(context *StateContext) error {
    _, is_child := thread_ext.Children[child.ID]
    if is_child == false {
      return fmt.Errorf("UNLINK_THREADS_ERR: %s is not a child of %s", child.ID, thread.ID)
    }

    delete(thread_ext.Children, child.ID)
    child_ext.Parent = nil
    return nil
  })
}

func checkIfChild(context *StateContext, id NodeID, cur *ThreadExt) (bool, error) {
  for _, info := range(cur.Children) {
    child := info.Child
    if child.ID == id {
      return true, nil
    }

    child_ext, err := GetExt[*ThreadExt](child)
    if err != nil {
      return false, err
    }

    var is_child bool
    err = UpdateStates(context, child, NewACLInfo(child, []string{"children"}), func(context *StateContext) error {
      is_child, err = checkIfChild(context, id, child_ext)
      return err
    })
    if err != nil {
      return false, err
    }
    if is_child {
      return true, nil
    }
  }

  return false, nil
}

// Links child to parent with info as the associated info
// Continues the write context with princ, getting children for thread and parent for child
func LinkThreads(context *StateContext, principal *Node, thread *Node, info ChildInfo) error {
  if context == nil || principal == nil || thread == nil || info.Child == nil {
    return fmt.Errorf("invalid input")
  }

  child := info.Child
  if thread.ID == child.ID {
    return fmt.Errorf("Will not link %s as a child of itself", thread.ID)
  }

  thread_ext, err := GetExt[*ThreadExt](thread)
  if err != nil {
    return err
  }

  child_ext, err := GetExt[*ThreadExt](thread)
  if err != nil {
    return err
  }

  return UpdateStates(context, principal, ACLMap{
    child.ID: ACLInfo{Node: child, Resources: []string{"parent"}},
    thread.ID: ACLInfo{Node: thread, Resources: []string{"children"}},
  }, func(context *StateContext) error {
    if child_ext.Parent != nil {
      return fmt.Errorf("EVENT_LINK_ERR: %s already has a parent, cannot link as child", child.ID)
    }

    is_child, err := checkIfChild(context, thread.ID, child_ext)
    if err != nil {
      return err
    } else if is_child == true {
      return fmt.Errorf("EVENT_LINK_ERR: %s is a child of %s so cannot add as parent", thread.ID, child.ID)
    }

    is_child, err = checkIfChild(context, child.ID, thread_ext)
    if err != nil {

        return err
    } else if is_child == true {
      return fmt.Errorf("EVENT_LINK_ERR: %s is already a parent of %s so will not add again", thread.ID, child.ID)
    }

    // TODO check for info types

    thread_ext.Children[child.ID] = info
    child_ext.Parent = thread

    return nil
  })
}

type ThreadAction func(*Context, *Node, *ThreadExt)(string, error)
type ThreadActions map[string]ThreadAction
type ThreadHandler func(*Context, *Node, *ThreadExt, GraphSignal)(string, error)
type ThreadHandlers map[string]ThreadHandler

type InfoType string
func (t InfoType) String() string {
  return string(t)
}

type ThreadInfo interface {
  Serializable[InfoType]
}

// Data required by a parent thread to restore it's children
type ParentThreadInfo struct {
  Start bool `json:"start"`
  StartAction string `json:"start_action"`
  RestoreAction string `json:"restore_action"`
}

const ParentThreadInfoType = InfoType("PARENT")
func (info *ParentThreadInfo) Type() InfoType {
  return ParentThreadInfoType
}

func (info *ParentThreadInfo) Serialize() ([]byte, error) {
  return json.MarshalIndent(info, "", "  ")
}

type ChildInfo struct {
  Child *Node
  Infos map[InfoType]ThreadInfo
}

func NewChildInfo(child *Node, infos map[InfoType]ThreadInfo) ChildInfo {
  if infos == nil {
    infos = map[InfoType]ThreadInfo{}
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

func (ext *ThreadExt) QueueAction(end time.Time, action string) {
  ext.ActionQueue = append(ext.ActionQueue, QueuedAction{end, action})
  ext.NextAction, ext.TimeoutChan = ext.SoonestAction()
}

func (ext *ThreadExt) ClearActionQueue() {
  ext.ActionQueue = []QueuedAction{}
  ext.NextAction = nil
  ext.TimeoutChan = nil
}

func (ext *ThreadExt) SoonestAction() (*QueuedAction, <-chan time.Time) {
  var soonest_action *QueuedAction
  var soonest_time time.Time
  for _, action := range(ext.ActionQueue) {
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

func NewThreadExt(buffer int, name string, state_name string, actions ThreadActions, handlers ThreadHandlers) ThreadExt {
  return ThreadExt{
    Actions: actions,
    Handlers: handlers,
    SignalChan: make(chan GraphSignal, buffer),
    TimeoutChan: nil,
    Active: false,
    StateName: state_name,
    Parent: nil,
    Children: map[NodeID]ChildInfo{},
    ActionQueue: []QueuedAction{},
    NextAction: nil,
  }
}

func (ext *ThreadExt) SetActive(active bool) error {
  ext.ActiveLock.Lock()
  defer ext.ActiveLock.Unlock()
  if ext.Active == true && active == true {
    return fmt.Errorf("alreday active, cannot set active")
  } else if ext.Active == false && active == false {
    return fmt.Errorf("already inactive, canot set inactive")
  }
  ext.Active = active
  return nil
}

func (ext *ThreadExt) SetState(state string) error {
  ext.StateName = state
  return nil
}

// Requires the read permission of threads children
func FindChild(context *StateContext, principal *Node, thread *Node, id NodeID) (*Node, error) {
  if thread == nil {
    panic("cannot recurse through nil")
  }

  if id ==  thread.ID {
    return thread, nil
  }

  thread_ext, err := GetExt[*ThreadExt](thread)
  if err != nil {
    return nil, err
  }

  var found *Node = nil
  err = UseStates(context, principal, NewACLInfo(thread, []string{"children"}), func(context *StateContext) error {
    for _, info := range(thread_ext.Children) {
      found, err = FindChild(context, principal, info.Child, id)
      if err != nil {
        return err
      }
      if found != nil {
        return nil
      }
    }
    return nil
  })

  return found, err
}

func ChildGo(ctx * Context, thread_ext *ThreadExt, child *Node, first_action string) {
  thread_ext.ChildWaits.Add(1)
  go func(child *Node) {
    defer thread_ext.ChildWaits.Done()
    err := ThreadLoop(ctx, child, first_action)
    if err != nil {
      ctx.Log.Logf("thread", "THREAD_CHILD_RUN_ERR: %s %s", child.ID, err)
    } else {
      ctx.Log.Logf("thread", "THREAD_CHILD_RUN_DONE: %s", child.ID)
    }
  }(child)
}

// Main Loop for Threads, starts a write context, so cannot be called from a write or read context
func ThreadLoop(ctx * Context, thread *Node, first_action string) error {
  thread_ext, err := GetExt[*ThreadExt](thread)
  if err != nil {
    return err
  }

  ctx.Log.Logf("thread", "THREAD_LOOP_START: %s - %s", thread.ID, first_action)

  err = thread_ext.SetActive(true)
  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_START_ERR: %e", err)
    return err
  }
  next_action := first_action
  for next_action != "" {
    action, exists := thread_ext.Actions[next_action]
    if exists == false {
      error_str := fmt.Sprintf("%s is not a valid action", next_action)
      return errors.New(error_str)
    }

    ctx.Log.Logf("thread", "THREAD_ACTION: %s - %s", thread.ID, next_action)
    next_action, err = action(ctx, thread, thread_ext)
    if err != nil {
      return err
    }
  }

  err = thread_ext.SetActive(false)
  if err != nil {
    ctx.Log.Logf("thread", "THREAD_LOOP_STOP_ERR: %e", err)
    return err
  }

  ctx.Log.Logf("thread", "THREAD_LOOP_DONE: %s", thread.ID)

  return nil
}

func ThreadChildLinked(ctx *Context, thread *Node, thread_ext *ThreadExt, signal GraphSignal) (string, error) {
  ctx.Log.Logf("thread", "THREAD_CHILD_LINKED: %s - %+v", thread.ID, signal)
  context := NewWriteContext(ctx)
  err := UpdateStates(context, thread, NewACLInfo(thread, []string{"children"}), func(context *StateContext) error {
    sig, ok := signal.(IDSignal)
    if ok == false {
      ctx.Log.Logf("thread", "THREAD_NODE_LINKED_BAD_CAST")
      return nil
    }
    info, exists := thread_ext.Children[sig.ID]
    if exists == false {
      ctx.Log.Logf("thread", "THREAD_NODE_LINKED: %s is not a child of %s", sig.ID)
      return nil
    }
    parent_info, exists := info.Infos["parent"].(*ParentThreadInfo)
    if exists == false {
      panic("ran ThreadChildLinked from a thread that doesn't require 'parent' child info. library party foul")
    }

    if parent_info.Start == true {
      ChildGo(ctx, thread_ext, info.Child, parent_info.StartAction)
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
func ThreadStartChild(ctx *Context, thread *Node, thread_ext *ThreadExt, signal GraphSignal) (string, error) {
  sig, ok := signal.(StartChildSignal)
  if ok == false {
    return "wait", nil
  }

  context := NewWriteContext(ctx)
  return "wait", UpdateStates(context, thread, NewACLInfo(thread, []string{"children"}), func(context *StateContext) error {
    info, exists:= thread_ext.Children[sig.ID]
    if exists == false {
      return fmt.Errorf("%s is not a child of %s", sig.ID, thread.ID)
    }
    return UpdateStates(context, thread, NewACLInfo(info.Child, []string{"start"}), func(context *StateContext) error {

      parent_info, exists := info.Infos["parent"].(*ParentThreadInfo)
      if exists == false {
        return fmt.Errorf("Called ThreadStartChild from a thread that doesn't require parent child info")
      }
      parent_info.Start = true
      ChildGo(ctx, thread_ext, info.Child, sig.Action)

      return nil
    })
  })
}

// Helper function to restore threads that should be running from a parents restore action
// Starts a write context, so cannot be called from either a write or read context
func ThreadRestore(ctx * Context, thread *Node, thread_ext *ThreadExt, start bool) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewACLInfo(thread, []string{"children"}), func(context *StateContext) error {
    return UpdateStates(context, thread, ACLList(thread_ext.ChildList(), []string{"start", "state"}), func(context *StateContext) error {
      for _, info := range(thread_ext.Children) {
        child_ext, err := GetExt[*ThreadExt](info.Child)
        if err != nil {
          return err
        }

        parent_info := info.Infos["parent"].(*ParentThreadInfo)
        if parent_info.Start == true && child_ext.StateName != "finished" {
          ctx.Log.Logf("thread", "THREAD_RESTORED: %s -> %s", thread.ID, info.Child.ID)
          if start == true {
            ChildGo(ctx, thread_ext, info.Child, parent_info.StartAction)
          } else {
            ChildGo(ctx, thread_ext, info.Child, parent_info.RestoreAction)
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
func ThreadStart(ctx * Context, thread *Node, thread_ext *ThreadExt) (string, error) {
  context := NewWriteContext(ctx)
  err := UpdateStates(context, thread, NewACLInfo(thread, []string{"state"}), func(context *StateContext) error {
    err := LockLockables(context, map[NodeID]*Node{thread.ID: thread}, thread)
    if err != nil {
      return err
    }
    return thread_ext.SetState("started")
  })
  if err != nil {
    return "", err
  }

  context = NewReadContext(ctx)
  return "wait", Signal(context, thread, thread, NewStatusSignal("started", thread.ID))
}

func ThreadWait(ctx * Context, thread *Node, thread_ext *ThreadExt) (string, error) {
  ctx.Log.Logf("thread", "THREAD_WAIT: %s - %+v", thread.ID, thread_ext.ActionQueue)
  for {
    select {
      case signal := <- thread_ext.SignalChan:
        ctx.Log.Logf("thread", "THREAD_SIGNAL: %s %+v", thread.ID, signal)
        signal_fn, exists := thread_ext.Handlers[signal.Type()]
        if exists == true {
          ctx.Log.Logf("thread", "THREAD_HANDLER: %s - %s", thread.ID, signal.Type())
          return signal_fn(ctx, thread, thread_ext, signal)
        } else {
          ctx.Log.Logf("thread", "THREAD_NOHANDLER: %s - %s", thread.ID, signal.Type())
        }
      case <- thread_ext.TimeoutChan:
        timeout_action := ""
        context := NewWriteContext(ctx)
        err := UpdateStates(context, thread, NewACLMap(NewACLInfo(thread, []string{"timeout"})), func(context *StateContext) error {
          timeout_action = thread_ext.NextAction.Action
          thread_ext.NextAction, thread_ext.TimeoutChan = thread_ext.SoonestAction()
          return nil
        })
        if err != nil {
          ctx.Log.Logf("thread", "THREAD_TIMEOUT_ERR: %s - %e", thread.ID, err)
        }
        ctx.Log.Logf("thread", "THREAD_TIMEOUT %s - NEXT_STATE: %s", thread.ID, timeout_action)
        return timeout_action, nil
    }
  }
}

func ThreadFinish(ctx *Context, thread *Node, thread_ext *ThreadExt) (string, error) {
  context := NewWriteContext(ctx)
  return "", UpdateStates(context, thread, NewACLInfo(thread, []string{"state"}), func(context *StateContext) error {
    err := thread_ext.SetState("finished")
    if err != nil {
      return err
    }
    return UnlockLockables(context, map[NodeID]*Node{thread.ID: thread}, thread)
  })
}

var ThreadAbortedError = errors.New("Thread aborted by signal")

// Default thread action function for "abort", sends a signal and returns a ThreadAbortedError
func ThreadAbort(ctx * Context, thread *Node, thread_ext *ThreadExt, signal GraphSignal) (string, error) {
  context := NewReadContext(ctx)
  err := Signal(context, thread, thread, NewStatusSignal("aborted", thread.ID))
  if err != nil {
    return "", err
  }
  return "", ThreadAbortedError
}

// Default thread action for "stop", sends a signal and returns no error
func ThreadStop(ctx * Context, thread *Node, thread_ext *ThreadExt, signal GraphSignal) (string, error) {
  context := NewReadContext(ctx)
  err := Signal(context, thread, thread, NewStatusSignal("stopped", thread.ID))
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
