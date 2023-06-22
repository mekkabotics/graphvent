package graphvent

import (
  "sync"
  "github.com/google/uuid"
  "time"
  "os"
  "github.com/rs/zerolog"
  "fmt"
  "encoding/json"
)

type Logger interface {
  Init() error
  Logf(component string, format string, items ... interface{})
  Logm(component string, fields map[string]interface{}, format string, items ... interface{})
}

type DefaultLogger struct {
  init_lock sync.Mutex
  Loggers map[string]zerolog.Logger
  Components []string
}

var Log DefaultLogger = DefaultLogger{Loggers: map[string]zerolog.Logger{}, Components: []string{}}

func (logger * DefaultLogger) Init(components []string) error {
  logger.init_lock.Lock()
  defer logger.init_lock.Unlock()

  component_enabled := func (component string) bool {
    for _, c := range(components) {
      if c == component {
        return true
      }
    }
    return false
  }

  for c, _ := range(logger.Loggers) {
    if component_enabled(c) == false {
      delete(logger.Loggers, c)
    }
  }

  for _, c := range(components) {
    _, exists := logger.Loggers[c]
    if component_enabled(c) == true && exists == false {
      logger.Loggers[c] = zerolog.New(os.Stdout).With().Timestamp().Str("component", c).Logger()
    }
  }
  return nil
}

func (logger * DefaultLogger) Logm(component string, fields map[string]interface{}, format string, items ... interface{}) {
  l, exists := logger.Loggers[component]
  if exists == true {
    log := l.Log()
    for key, value := range(fields) {
      log = log.Str(key, fmt.Sprintf("%+v", value))
    }
    log.Msg(fmt.Sprintf(format, items...))
  }
}

func (logger * DefaultLogger) Logf(component string, format string, items ... interface{}) {
  l, exists := logger.Loggers[component]
  if exists == true {
    l.Log().Msg(fmt.Sprintf(format, items...))
  }
}

// Generate a random graphql id
func randid() string{
  uuid_str := uuid.New().String()
  return uuid_str
}

type GraphSignal interface {
  Downwards() bool
  Source() string
  Type() string
  String() string
}

type BaseSignal struct {
  downwards bool
  source string
  _type string
}

func (signal BaseSignal) Downwards() bool {
  return signal.downwards
}

func (signal BaseSignal) Source() string {
  return signal.source
}

func (signal BaseSignal) Type() string {
  return signal._type
}

func (signal BaseSignal) String() string {
  return fmt.Sprintf("{downwards: %t, source: %s, type: %s}", signal.downwards, signal.source, signal._type)
}

type TimeSignal struct {
  BaseSignal
  time time.Time
}

func NewBaseSignal(source GraphNode, _type string, downwards bool) BaseSignal {
  source_id := ""
  if source != nil {
    source_id = source.ID()
  }

  signal := BaseSignal{
    downwards: downwards,
    source: source_id,
    _type: _type,
  }
  return signal
}

func NewDownSignal(source GraphNode, _type string) BaseSignal {
  return NewBaseSignal(source, _type, true)
}

func NewSignal(source GraphNode, _type string) BaseSignal {
  return NewBaseSignal(source, _type, false)
}

type NodeState interface {
  Name() string
  Description() string
  DelegationMap() map[string]GraphNode
}

type BaseNodeState struct {
  name string
  description string
  delegation_map map[string]GraphNode
}

func (state * BaseNodeState) Name() string {
  return state.name
}

func (state * BaseNodeState) Description() string {
  return state.description
}

func (state * BaseNodeState) DelegationMap() map[string]GraphNode {
  return state.delegation_map
}


// GraphNode is the interface common to both DAG nodes and Event tree nodes
type GraphNode interface {
  State() NodeState
  ID() string
  UpdateListeners(update GraphSignal)
  PropagateUpdate(update GraphSignal)
  RegisterChannel(listener chan GraphSignal)
  UnregisterChannel(listener chan GraphSignal)
  SignalChannel() chan GraphSignal
}

func (node * BaseNode) StateLock() *sync.Mutex {
  return &node.state_lock
}

func NewBaseNode(id string) BaseNode {
  node := BaseNode{
    id: id,
    signal: make(chan GraphSignal, 512),
    listeners: map[chan GraphSignal]chan GraphSignal{},
  }
  Log.Logf("graph", "NEW_NODE: %s", node.ID())
  return node
}

// BaseNode is the most basic implementation of the GraphNode interface
// It is used to implement functions common to Events and Resources
type BaseNode struct {
  id string
  state NodeState
  state_lock sync.RWMutex
  signal chan GraphSignal
  listeners_lock sync.Mutex
  listeners map[chan GraphSignal]chan GraphSignal
}

func (node * BaseNode) SignalChannel() chan GraphSignal {
  return node.signal
}

func (node * BaseNode) State() NodeState {
  return node.state
}

func (node * BaseNode) ID() string {
  return node.id
}

const listener_buffer = 100
func GetUpdateChannel(node * BaseNode) chan GraphSignal {
  new_listener := make(chan GraphSignal, listener_buffer)
  node.RegisterChannel(new_listener)
  return new_listener
}

func (node * BaseNode) RegisterChannel(listener chan GraphSignal) {
  node.listeners_lock.Lock()
  _, exists := node.listeners[listener]
  if exists == false {
    node.listeners[listener] = listener
  }
  node.listeners_lock.Unlock()
}

func (node * BaseNode) UnregisterChannel(listener chan GraphSignal) {
  node.listeners_lock.Lock()
  _, exists := node.listeners[listener]
  if exists == false {
    panic("Attempting to unregister non-registered listener")
  } else {
    delete(node.listeners, listener)
  }
  node.listeners_lock.Unlock()
}

func (node * BaseNode) PropagateUpdate(update GraphSignal) {
}

func (node * BaseNode) UpdateListeners(update GraphSignal) {
  node.ListenersLock.Lock()
  defer node.ListenersLock.Unlock()
  closed := []chan GraphSignal

  for _, listener := range node.Listeners() {
    Log.Logf("listeners", "UPDATE_LISTENER %s: %p", node.ID(), listener)
    select {
    case listener <- signal:
    default:
      Log.Logf("listeners", "CLOSED_LISTENER %s: %p", node.ID(), listener)
      go func(node GraphNode, listener chan GraphSignal) {
        listener <- NewSignal(node, "listener_closed")
        close(listener)
      }(node, listener)
      closed = append(closed, listener)
    }
  }

  for _, listener := range(closed) {
    delete(node.listeners, listener)
  }
}

func SendUpdate(node GraphNode, signal GraphSignal) {
  node_name := "nil"
  if node != nil {
    node_name = node.Name()
  }
  Log.Logf("update", "UPDATE %s <- %s: %+v", node_name, signal.Source(), signal)
  node.ListenersLock.Lock()
  defer node.ListenersLock.Unlock()
  closed := []chan GraphSignal

  for _, listener := range node.Listeners() {
    Log.Logf("listeners", "UPDATE_LISTENER %s: %p", node.ID(), listener)
    select {
    case listener <- signal:
    default:
      Log.Logf("listeners", "CLOSED_LISTENER %s: %p", node.ID(), listener)
      go func(node GraphNode, listener chan GraphSignal) {
        listener <- NewSignal(node, "listener_closed")
        close(listener)
      }(node, listener)
      closed = append(closed, listener)
    }
  }

  for _, listener := range(closed) {
    delete(node.listeners, listener)
  }

  node.PropagateUpdate(signal)
}

