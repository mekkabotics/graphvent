package graphvent

import (
  "sync"
  "reflect"
  "github.com/google/uuid"
  "os"
  "github.com/rs/zerolog"
  "fmt"
  badger "github.com/dgraph-io/badger/v3"
  "encoding/json"
)

type GraphContext struct {
  DB * badger.DB
  Log Logger
}

func NewGraphContext(db * badger.DB, log Logger) * GraphContext {
  return &GraphContext{DB: db, Log: log}
}

// A Logger is passed around to record events happening to components enabled by SetComponents
type Logger interface {
  SetComponents(components []string) error
  // Log a formatted string
  Logf(component string, format string, items ... interface{})
  // Log a map of attributes and a format string
  Logm(component string, fields map[string]interface{}, format string, items ... interface{})
  // Log a structure to a file by marshalling and unmarshalling the json
  Logj(component string, s interface{}, format string, items ... interface{})
}

func NewConsoleLogger(components []string) *ConsoleLogger {
  logger := &ConsoleLogger{
    loggers: map[string]zerolog.Logger{},
    components: []string{},
  }

  logger.SetComponents(components)

  return logger
}

// A ConsoleLogger logs to stdout
type ConsoleLogger struct {
  loggers map[string]zerolog.Logger
  components_lock sync.Mutex
  components []string
}

func (logger * ConsoleLogger) SetComponents(components []string) error {
  logger.components_lock.Lock()
  defer logger.components_lock.Unlock()

  component_enabled := func (component string) bool {
    for _, c := range(components) {
      if c == component {
        return true
      }
    }
    return false
  }

  for c, _ := range(logger.loggers) {
    if component_enabled(c) == false {
      delete(logger.loggers, c)
    }
  }

  for _, c := range(components) {
    _, exists := logger.loggers[c]
    if component_enabled(c) == true && exists == false {
      logger.loggers[c] = zerolog.New(os.Stdout).With().Timestamp().Str("component", c).Logger()
    }
  }
  return nil
}

func (logger * ConsoleLogger) Logm(component string, fields map[string]interface{}, format string, items ... interface{}) {
  l, exists := logger.loggers[component]
  if exists == true {
    log := l.Log()
    for key, value := range(fields) {
      log = log.Str(key, fmt.Sprintf("%+v", value))
    }
    log.Msg(fmt.Sprintf(format, items...))
  }
}

func (logger * ConsoleLogger) Logf(component string, format string, items ... interface{}) {
  l, exists := logger.loggers[component]
  if exists == true {
    l.Log().Msg(fmt.Sprintf(format, items...))
  }
}

func (logger * ConsoleLogger) Logj(component string, s interface{}, format string, items ... interface{}) {
  m := map[string]interface{}{}
  ser, err := json.Marshal(s)
  if err != nil {
    panic("LOG_MARSHAL_ERR")
  }
  err = json.Unmarshal(ser, &m)
  if err != nil {
    panic("LOG_UNMARSHAL_ERR")
  }
  logger.Logm(component, m, format, items...)
}

type NodeID string
// Generate a random id
func RandID() NodeID {
  uuid_str := uuid.New().String()
  return NodeID(uuid_str)
}

type SignalDirection int
const (
  Up SignalDirection = iota
  Down
  Direct
)

// GraphSignals are passed around the event tree/resource DAG and cast by Type()
type GraphSignal interface {
  // How to propogate the signal
  Direction() SignalDirection
  Source() NodeID
  Type() string
  String() string
}

// BaseSignal is the most basic type of signal, it has no additional data
type BaseSignal struct {
  FDirection SignalDirection `json:"direction"`
  FSource NodeID `json:"source"`
  FType string `json:"type"`
}

func (state BaseSignal) String() string {
  ser, err := json.Marshal(state)
  if err != nil {
    return "STATE_SER_ERR"
  }
  return string(ser)
}

func (signal BaseSignal) Direction() SignalDirection {
  return signal.FDirection
}

func (signal BaseSignal) Source() NodeID {
  return signal.FSource
}

func (signal BaseSignal) Type() string {
  return signal.FType
}

func NewBaseSignal(source GraphNode, _type string, direction SignalDirection) BaseSignal {
  var source_id NodeID = ""
  if source != nil {
    source_id = source.ID()
  }

  signal := BaseSignal{
    FDirection: direction,
    FSource: source_id,
    FType: _type,
  }
  return signal
}

func NewDownSignal(source GraphNode, _type string) BaseSignal {
  return NewBaseSignal(source, _type, Down)
}

func NewSignal(source GraphNode, _type string) BaseSignal {
  return NewBaseSignal(source, _type, Up)
}

func NewDirectSignal(source GraphNode, _type string) BaseSignal {
  return NewBaseSignal(source, _type, Direct)
}

func AbortSignal(source GraphNode) BaseSignal {
  return NewBaseSignal(source, "abort", Down)
}

func CancelSignal(source GraphNode) BaseSignal {
  return NewBaseSignal(source, "cancel", Down)
}

type NodeState interface {
  Name() string
}

// GraphNode is the interface common to both DAG nodes and Event tree nodes
// They have a NodeState interface which is saved to the database every update
type GraphNode interface {
  ID() NodeID

  State() NodeState
  StateLock() *sync.RWMutex

  SetState(new_state NodeState)

  // Signal propagation function for listener channels
  UpdateListeners(ctx * GraphContext, update GraphSignal)
  // Signal propagation function for connected nodes(defined in state)
  PropagateUpdate(ctx * GraphContext, update GraphSignal)

  // Get an update channel for the node to be notified of signals
  UpdateChannel(buffer int) chan GraphSignal

  // Register and unregister a channel to propogate updates to
  RegisterChannel(listener chan GraphSignal)
  UnregisterChannel(listener chan GraphSignal)
  // Get a handle to the nodes internal signal channel
  SignalChannel() chan GraphSignal
}

// Create a new base node with a new ID
func NewNode(ctx * GraphContext, state NodeState) (BaseNode, error) {

  node := BaseNode{
    id: RandID(),
    signal: make(chan GraphSignal, 512),
    listeners: map[chan GraphSignal]chan GraphSignal{},
    state: state,
  }

  err := WriteDBState(ctx, node.id, state)
  if err != nil {
    return node, fmt.Errorf("DB_NEW_WRITE_ERROR: %e", err)
  }

  ctx.Log.Logf("graph", "NEW_NODE: %s - %+v", node.id, state)
  return node, nil
}

// BaseNode is the minimum set of fields needed to implement a GraphNode,
// and provides a template for more complicated Nodes
type BaseNode struct {
  id NodeID

  state NodeState
  state_lock sync.RWMutex

  signal chan GraphSignal

  listeners_lock sync.Mutex
  listeners map[chan GraphSignal]chan GraphSignal
}

func (node * BaseNode) ID() NodeID {
  return node.id
}

func (node * BaseNode) State() NodeState {
  return node.state
}

func (node * BaseNode) StateLock() * sync.RWMutex {
  return &node.state_lock
}

func WriteDBState(ctx * GraphContext, id NodeID, state NodeState) error {
  ctx.Log.Logf("db", "DB_WRITE: %s - %+v", id, state)

  var serialized_state []byte = nil
  if state != nil {
    ser, err := json.Marshal(state)
    if err != nil {
      return fmt.Errorf("DB_MARSHAL_ERROR: %e", err)
    }
    serialized_state = ser
  } else {
    serialized_state = []byte{}
  }

  err := ctx.DB.Update(func(txn *badger.Txn) error {
    err := txn.Set([]byte(id), serialized_state)
    return err
  })

  return err
}

func (node * BaseNode) SetState(new_state NodeState) {
  node.state = new_state
}

func checkForDuplicate(nodes []GraphNode) error {
  found := map[NodeID]bool{}
  for _, node := range(nodes) {
    if node == nil {
      return fmt.Errorf("Cannot get state of nil node")
    }

    _, exists := found[node.ID()]
    if exists == true {
      return fmt.Errorf("Attempted to get state of %s twice", node.ID())
    }
    found[node.ID()] = true
  }
  return nil
}

func UseStates(ctx * GraphContext, nodes []GraphNode, states_fn func(states []NodeState)(interface{}, error)) (interface{}, error) {
  err := checkForDuplicate(nodes)
  if err != nil {
    return nil, err
  }

  for _, node := range(nodes) {
    node.StateLock().RLock()
  }

  states := make([]NodeState, len(nodes))
  for i, node := range(nodes) {
    states[i] = node.State()
  }

  val, err := states_fn(states)

  for _, node := range(nodes) {
    node.StateLock().RUnlock()
  }

  return val, err
}

func UpdateStates(ctx * GraphContext, nodes []GraphNode, states_fn func(states []NodeState)([]NodeState, interface{}, error)) (interface{}, error) {
  err := checkForDuplicate(nodes)
  if err != nil {
    return nil, err
  }

  for _, node := range(nodes) {
    node.StateLock().Lock()
  }

  states := make([]NodeState, len(nodes))
  for i, node := range(nodes) {
    states[i] = node.State()
  }

  new_states, val, err := states_fn(states)

  if new_states != nil {
    if len(new_states) != len(nodes) {
      panic(fmt.Sprintf("NODE_NEW_STATE_LEN_MISMATCH: %d/%d", len(new_states), len(nodes)))
    }

    for i, new_state := range(new_states) {
      if new_state != nil {
        old_state_type := reflect.TypeOf(states[i])
        new_state_type := reflect.TypeOf(new_state)

        if old_state_type != new_state_type {
          panic(fmt.Sprintf("NODE_STATE_MISMATCH: old - %+v, new - %+v", old_state_type, new_state_type))
        }

        err := WriteDBState(ctx, nodes[i].ID(), new_state)
        if err != nil {
          panic(fmt.Sprintf("DB_WRITE_ERROR: %s", err))
        }

        nodes[i].SetState(new_state)
      }
    }
  }

  for _, node := range(nodes) {
    node.StateLock().Unlock()
  }

  return val, err
}

func (node * BaseNode) UpdateListeners(ctx * GraphContext, update GraphSignal) {
  node.listeners_lock.Lock()
  defer node.listeners_lock.Unlock()
  closed := []chan GraphSignal{}

  for _, listener := range node.listeners {
    ctx.Log.Logf("listeners", "UPDATE_LISTENER %s: %p", node.ID(), listener)
    select {
    case listener <- update:
    default:
      ctx.Log.Logf("listeners", "CLOSED_LISTENER %s: %p", node.ID(), listener)
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

func (node * BaseNode) PropagateUpdate(ctx * GraphContext, update GraphSignal) {
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

func (node * BaseNode) SignalChannel() chan GraphSignal {
  return node.signal
}

// Create a new GraphSinal channel with a buffer of size buffer and register it to a node
func (node * BaseNode) UpdateChannel(buffer int) chan GraphSignal {
  new_listener := make(chan GraphSignal, buffer)
  node.RegisterChannel(new_listener)
  return new_listener
}

// Propogate a signal starting at a node
func SendUpdate(ctx * GraphContext, node GraphNode, signal GraphSignal) {
  if node == nil {
    panic("Cannot start an update from no node")
  }

  ctx.Log.Logf("update", "UPDATE %s <- %s: %+v", node.ID(), signal.Source(), signal)

  node.UpdateListeners(ctx, signal)
  node.PropagateUpdate(ctx, signal)
}

