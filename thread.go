package graphvent

import (
  "fmt"
  "time"
  "sync"
  "errors"
  "encoding/json"
  "crypto/sha512"
  "encoding/binary"
)

type ThreadAction func(*Context, *Node, *ThreadExt)(string, error)
type ThreadActions map[string]ThreadAction
type ThreadHandler func(*Context, *Node, *ThreadExt, Signal)(string, error)
type ThreadHandlers map[SignalType]ThreadHandler

type InfoType string
func (t InfoType) String() string {
  return string(t)
}

type Info interface {
  Serializable[InfoType]
}

// Data required by a parent thread to restore it's children
type ParentInfo struct {
  Start bool `json:"start"`
  StartAction string `json:"start_action"`
  RestoreAction string `json:"restore_action"`
}

const ParentInfoType = InfoType("PARENT")
func (info *ParentInfo) Type() InfoType {
  return ParentInfoType
}

func (info *ParentInfo) Serialize() ([]byte, error) {
  return json.MarshalIndent(info, "", "  ")
}

type QueuedAction struct {
  Timeout time.Time `json:"time"`
  Action string `json:"action"`
}

type ThreadType string
func (thread ThreadType) Hash() uint64 {
  hash := sha512.Sum512([]byte(fmt.Sprintf("THREAD: %s", string(thread))))
  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}

type ThreadInfo struct {
  Actions ThreadActions
  Handlers ThreadHandlers
}

type InfoLoadFunc func([]byte)(Info, error)
type ThreadExtContext struct {
  Types map[ThreadType]ThreadInfo
  Loads map[InfoType]InfoLoadFunc
}

const BaseThreadType = ThreadType("BASE")
func NewThreadExtContext() *ThreadExtContext {
  return &ThreadExtContext{
    Types: map[ThreadType]ThreadInfo{
      BaseThreadType: ThreadInfo{
        Actions: BaseThreadActions,
        Handlers: BaseThreadHandlers,
      },
    },
    Loads: map[InfoType]InfoLoadFunc{
      ParentInfoType: func(data []byte) (Info, error) {
        var info ParentInfo
        err := json.Unmarshal(data, &info)
        if err != nil {
          return nil, err
        }

        return &info, nil
      },
    },
  }
}

func (ctx *ThreadExtContext) RegisterThreadType(thread_type ThreadType, actions ThreadActions, handlers ThreadHandlers) error {
  if actions == nil || handlers == nil {
    return fmt.Errorf("Cannot register ThreadType %s with nil actions or handlers", thread_type)
  }

  _, exists := ctx.Types[thread_type]
  if exists == true {
    return fmt.Errorf("ThreadType %s already registered in ThreadExtContext, cannot register again", thread_type)
  }
  ctx.Types[thread_type] = ThreadInfo{
    Actions: actions,
    Handlers: handlers,
  }

  return nil
}

func (ctx *ThreadExtContext) RegisterInfoType(info_type InfoType, load_fn InfoLoadFunc) error {
  if load_fn == nil {
    return fmt.Errorf("Cannot register %s with nil load_fn", info_type)
  }

  _, exists := ctx.Loads[info_type]
  if exists == true {
    return fmt.Errorf("InfoType %s is already registered in ThreadExtContext, cannot register again", info_type)
  }

  ctx.Loads[info_type] = load_fn
  return nil
}

type ThreadExt struct {
  Actions ThreadActions
  Handlers ThreadHandlers

  ThreadType ThreadType

  SignalChan chan Signal
  TimeoutChan <-chan time.Time

  ChildWaits sync.WaitGroup

  ActiveLock sync.Mutex
  Active bool
  State string

  Parent *Node
  Children map[NodeID]ChildInfo

  ActionQueue []QueuedAction
  NextAction *QueuedAction
}

type ThreadExtJSON struct {
  State string `json:"state"`
  Type string `json:"type"`
  Parent string `json:"parent"`
  Children map[string]map[string][]byte `json:"children"`
  ActionQueue []QueuedAction
}

func (ext *ThreadExt) Serialize() ([]byte, error) {
  children := map[string]map[string][]byte{}
  for id, child := range(ext.Children) {
    id_str := id.String()
    children[id_str] = map[string][]byte{}
    for info_type, info := range(child.Infos) {
      var err error
      children[id_str][string(info_type)], err = info.Serialize()
      if err != nil {
        return nil, err
      }
    }
  }

  return json.MarshalIndent(&ThreadExtJSON{
    State: ext.State,
    Type: string(ext.ThreadType),
    Parent: SaveNode(ext.Parent),
    Children: children,
    ActionQueue: ext.ActionQueue,
  }, "", "  ")
}

func NewThreadExt(ctx*Context, thread_type ThreadType, parent *Node, children map[NodeID]ChildInfo, state string, action_queue []QueuedAction) (*ThreadExt, error) {
  if children == nil {
    children = map[NodeID]ChildInfo{}
  }

  if action_queue == nil {
    action_queue = []QueuedAction{}
  }

  thread_ctx, err := GetCtx[*ThreadExt, *ThreadExtContext](ctx)
  if err != nil {
    return nil, err
  }
  type_info, exists := thread_ctx.Types[thread_type]
  if exists == false {
    return nil, fmt.Errorf("Tried to load thread type %s which is not in context", thread_type)
  }
  next_action, timeout_chan := SoonestAction(action_queue)

  return &ThreadExt{
    ThreadType: thread_type,
    Actions: type_info.Actions,
    Handlers: type_info.Handlers,
    SignalChan: make(chan Signal, THREAD_BUFFER_SIZE),
    TimeoutChan: timeout_chan,
    Active: false,
    State: state,
    Parent: parent,
    Children: children,
    ActionQueue: action_queue,
    NextAction: next_action,
  }, nil
}

const THREAD_BUFFER_SIZE int = 1024
func LoadThreadExt(ctx *Context, data []byte) (Extension, error) {
  var j ThreadExtJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }
  ctx.Log.Logf("db", "DB_LOAD_THREAD_EXT_JSON: %+v", j)

  parent, err := RestoreNode(ctx, j.Parent)
  if err != nil {
    return nil, err
  }


  thread_ctx, err := GetCtx[*ThreadExt, *ThreadExtContext](ctx)
  if err != nil {
    return nil, err
  }

  children := map[NodeID]ChildInfo{}
  for id_str, infos := range(j.Children) {
    child_node, err := RestoreNode(ctx, id_str)
    if err != nil {
      return nil, err
    }
    child_infos := map[InfoType]Info{}
    for info_type_str, info_data := range(infos) {
      info_type := InfoType(info_type_str)
      info_load, exists := thread_ctx.Loads[info_type]
      if exists == false {
        return nil, fmt.Errorf("%s is not a known InfoType in ThreacExrContxt", info_type)
      }

      info, err := info_load(info_data)
      if err != nil {
        return nil, err
      }
      child_infos[info_type] = info
    }

    children[child_node.ID] = ChildInfo{
      Child: child_node,
      Infos: child_infos,
    }
  }

  return NewThreadExt(ctx, ThreadType(j.Type), parent, children, j.State, j.ActionQueue)
}

const ThreadExtType = ExtType("THREAD")
func (ext *ThreadExt) Type() ExtType {
  return ThreadExtType
}

func (ext *ThreadExt) QueueAction(end time.Time, action string) {
  ext.ActionQueue = append(ext.ActionQueue, QueuedAction{end, action})
  ext.NextAction, ext.TimeoutChan = SoonestAction(ext.ActionQueue)
}

func (ext *ThreadExt) ClearActionQueue() {
  ext.ActionQueue = []QueuedAction{}
  ext.NextAction = nil
  ext.TimeoutChan = nil
}

func SoonestAction(actions []QueuedAction) (*QueuedAction, <-chan time.Time) {
  var soonest_action *QueuedAction
  var soonest_time time.Time
  for _, action := range(actions) {
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
func (ext *ThreadExt) Process(context *StateContext, node *Node, signal Signal) error {
  context.Graph.Log.Logf("signal", "THREAD_PROCESS: %s", node.ID)

  var err error
  switch signal.Direction() {
  case Up:
    err = UseStates(context, node, NewACLInfo(node, []string{"parent"}), func(context *StateContext) error {
      if ext.Parent != nil {
        if ext.Parent.ID != node.ID {
          return ext.Parent.Process(context, node.ID, signal)
        }
      }
      return nil
    })
  case Down:
    err = UseStates(context, node, NewACLInfo(node, []string{"children"}), func(context *StateContext) error {
      for _, info := range(ext.Children) {
        err := info.Child.Process(context, node.ID, signal)
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

  child_ext, err := GetExt[*ThreadExt](child)
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

type ChildInfo struct {
  Child *Node
  Infos map[InfoType]Info
}

func NewChildInfo(child *Node, infos map[InfoType]Info) ChildInfo {
  if infos == nil {
    infos = map[InfoType]Info{}
  }

  return ChildInfo{
    Child: child,
    Infos: infos,
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
  ext.State = state
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

func ThreadChildLinked(ctx *Context, thread *Node, thread_ext *ThreadExt, signal Signal) (string, error) {
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
    parent_info, exists := info.Infos["parent"].(*ParentInfo)
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
func ThreadStartChild(ctx *Context, thread *Node, thread_ext *ThreadExt, signal Signal) (string, error) {
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

    parent_info, exists := info.Infos["parent"].(*ParentInfo)
    if exists == false {
      return fmt.Errorf("Called ThreadStartChild from a thread that doesn't require parent child info")
    }
    parent_info.Start = true
    ChildGo(ctx, thread_ext, info.Child, sig.Action)

    return nil
  })
}

// Helper function to restore threads that should be running from a parents restore action
// Starts a write context, so cannot be called from either a write or read context
func ThreadRestore(ctx * Context, thread *Node, thread_ext *ThreadExt, start bool) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, thread, NewACLInfo(thread, []string{"children"}), func(context *StateContext) error {
    return UpdateStates(context, thread, ACLList(thread_ext.ChildList(), []string{"state"}), func(context *StateContext) error {
      for _, info := range(thread_ext.Children) {
        child_ext, err := GetExt[*ThreadExt](info.Child)
        if err != nil {
          return err
        }

        parent_info := info.Infos[ParentInfoType].(*ParentInfo)
        if parent_info.Start == true && child_ext.State != "finished" {
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
  return "wait", thread.Process(context, thread.ID, NewStatusSignal("started", thread.ID))
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
          thread_ext.NextAction, thread_ext.TimeoutChan = SoonestAction(thread_ext.ActionQueue)
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
func ThreadAbort(ctx * Context, thread *Node, thread_ext *ThreadExt, signal Signal) (string, error) {
  context := NewReadContext(ctx)
  err := thread.Process(context, thread.ID, NewStatusSignal("aborted", thread.ID))
  if err != nil {
    return "", err
  }
  return "", ThreadAbortedError
}

// Default thread action for "stop", sends a signal and returns no error
func ThreadStop(ctx * Context, thread *Node, thread_ext *ThreadExt, signal Signal) (string, error) {
  context := NewReadContext(ctx)
  err := thread.Process(context, thread.ID, NewStatusSignal("stopped", thread.ID))
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
