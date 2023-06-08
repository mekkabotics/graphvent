package main

import (
  "sync"
  "github.com/google/uuid"
  "time"
  "os"
  "github.com/rs/zerolog"
  "fmt"
  "io"
)

type Logger interface {
  Init() error
  Logf(component string, format string, items ... interface{})
  Logm(component string, fields map[string]interface{}, format string, items ... interface{})
}

type DefaultLogger struct {
  init bool
  init_lock sync.Mutex
  loggers map[string]zerolog.Logger
}

var log DefaultLogger = DefaultLogger{loggers: map[string]zerolog.Logger{}}
var all_components = []string{"update", "graph", "event", "resource", "manager", "test", "gql"}

func (logger * DefaultLogger) Init(components []string) error {
  logger.init_lock.Lock()
  defer logger.init_lock.Unlock()

  if logger.init == true {
    return nil
  }

  logger.init = true

  file, err := os.OpenFile("test.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)
  if err != nil {
    return err
  }

  component_enabled := func (component string) bool {
    for _, c := range(components) {
      if c == component {
        return true
      }
    }
    return false
  }

  writer := io.MultiWriter(file, os.Stdout)
  for _, c := range(all_components) {
    if component_enabled(c) == true {
      logger.loggers[c] = zerolog.New(writer).With().Timestamp().Str("component", c).Logger()
    }
  }
  return nil
}

func (logger * DefaultLogger) Logm(component string, fields map[string]interface{}, format string, items ... interface{}) {
  logger.Init([]string{"gql"})
  l, exists := logger.loggers[component]
  if exists == true {
    log := l.Log()
    for key, value := range(fields) {
      log = log.Str(key, fmt.Sprintf("%+v", value))
    }
    log.Msg(fmt.Sprintf(format, items...))
  }
}

func (logger * DefaultLogger) Logf(component string, format string, items ... interface{}) {
  logger.Init([]string{"gql"})
  l, exists := logger.loggers[component]
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
  Source() GraphNode
  Type() string
  Description() string
  Time() time.Time
  Last() string
  Trace(id string) GraphSignal
  String() string
}

type BaseSignal struct {
  source GraphNode
  signal_type string
  description string
  time time.Time
  last_id string
}

func (signal BaseSignal) String() string {
  source_name := "nil"
  source := signal.source
  if source != nil {
    source_name = source.Name()
  }
  return fmt.Sprintf("{type: %s, description: %s, source: %s, last: %s}", signal.signal_type, signal.description, source_name, signal.last_id)
}

func (signal BaseSignal) Time() time.Time {
  return signal.time
}

func (signal BaseSignal) Source() GraphNode {
  return signal.source
}

func (signal BaseSignal) Type() string {
  return signal.signal_type
}

func (signal BaseSignal) Description() string {
  return signal.description
}

func (signal BaseSignal) Trace(id string) GraphSignal {
  new_signal := signal
  new_signal.last_id = id
  return new_signal
}

func (signal BaseSignal) Last() string {
  return signal.last_id
}

func NewSignal(source GraphNode, signal_type string) (BaseSignal) {
  signal := BaseSignal{
    source: source,
    signal_type: signal_type,
  }
  return signal
}

// GraphNode is the interface common to both DAG nodes and Event tree nodes
type GraphNode interface {
  Name() string
  Description() string
  ID() string
  UpdateListeners(update GraphSignal)
  update(update GraphSignal)
  RegisterChannel(listener chan GraphSignal)
  UnregisterChannel(listener chan GraphSignal)
  UpdateChannel() chan GraphSignal
}

func NewBaseNode(name string, description string, id string) BaseNode {
  node := BaseNode{
    name: name,
    description: description,
    id: id,
    signal: make(chan GraphSignal, 512),
    listeners: map[chan GraphSignal]chan GraphSignal{},
  }
  log.Logf("graph", "NEW_NODE: %s - %s", node.ID(), node.Name())
  return node
}

// BaseNode is the most basic implementation of the GraphNode interface
// It is used to implement functions common to Events and Resources
type BaseNode struct {
  name string
  description string
  id string
  signal chan GraphSignal
  listeners_lock sync.Mutex
  listeners map[chan GraphSignal]chan GraphSignal
}

func (node * BaseNode) Name() string {
  return node.name
}

func (node * BaseNode) Description() string {
  return node.description
}

func (node * BaseNode) ID() string {
  return node.id
}

// Create a new listener channel for the node, add it to the nodes listener list, and return the new channel
const listener_buffer = 1000
func (node * BaseNode) UpdateChannel() chan GraphSignal {
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

// Send the update to listener channels
func (node * BaseNode) UpdateListeners(update GraphSignal) {
  node.listeners_lock.Lock()

  closed := []chan GraphSignal{}

  for _, listener := range node.listeners {
    log.Logf("update", "UPDATE_LISTENER %s: %p", node.Name(), listener)
    select {
    case listener <- update:
    default:
      log.Logf("update", "CLOSED_LISTENER: %s: %p", node.Name(), listener)
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

  node.listeners_lock.Unlock()
}

func (node * BaseNode) update(signal GraphSignal) {
}

func SendUpdate(node GraphNode, signal GraphSignal) {
  source := signal.Source()
  source_name := "nil"
  if source != nil {
    source_name = source.Name()
  }

  node_name := "nil"
  if node != nil {
    node_name = node.Name()
  }
  log.Logf("update", "UPDATE %s -> %s: %+v", source_name, node_name, signal)
  node.UpdateListeners(signal)
  node.update(signal)
}

