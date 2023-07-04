package graphvent

import (
  "sync"
  "reflect"
  "github.com/google/uuid"
  "github.com/graphql-go/graphql"
  "os"
  "github.com/rs/zerolog"
  "fmt"
  badger "github.com/dgraph-io/badger/v3"
  "encoding/json"
)

// For persistance, each node needs the following functions(* is a placeholder for the node/state type):
// Load*State - StateLoadFunc that returns the NodeState interface to attach to the node
// Load* - NodeLoadFunc that returns the GraphNode restored from it's loaded state

// For convenience, the following functions are a good idea to define for composability:
// Restore*State - takes in the nodes serialized data to allow for easier nesting of inherited Load*State functions
// Save*State - serialize the node into it's json counterpart to be included as part of a larger json

type StateLoadFunc func(*GraphContext, []byte, NodeMap)(NodeState, error)
type StateLoadMap map[string]StateLoadFunc
type NodeLoadFunc func(*GraphContext, NodeID)(GraphNode, error)
type NodeLoadMap map[string]NodeLoadFunc
type InfoLoadFunc func(*GraphContext, map[string]interface{})(ThreadInfo, error)
type InfoLoadMap map[string]InfoLoadFunc
type GraphContext struct {
  DB * badger.DB
  Log Logger
  NodeLoadFuncs NodeLoadMap
  StateLoadFuncs StateLoadMap
  InfoLoadFuncs InfoLoadMap
  GQL * GQLContext
}

type GQLContext struct {
  Schema graphql.Schema
  ValidNodes ObjTypeMap
  NodeType reflect.Type
  ValidLockables ObjTypeMap
  LockableType reflect.Type
  ValidThreads ObjTypeMap
  ThreadType reflect.Type
}

func NewGQLContext(additional_types TypeList, extended_types ObjTypeMap, extended_queries FieldMap, extended_subscriptions FieldMap, extended_mutations FieldMap) (*GQLContext, error) {
  type_list := TypeList{
    GQLTypeSignalInput(),
  }

  for _, gql_type := range(additional_types) {
    type_list = append(type_list, gql_type)
  }

  type_map := ObjTypeMap{}
  type_map[reflect.TypeOf((*BaseLockable)(nil))] =  GQLTypeBaseLockable()
  type_map[reflect.TypeOf((*BaseThread)(nil))] = GQLTypeBaseThread()
  type_map[reflect.TypeOf((*GQLThread)(nil))] = GQLTypeGQLThread()
  type_map[reflect.TypeOf((*BaseSignal)(nil))] = GQLTypeSignal()

  for go_t, gql_t := range(extended_types) {
    type_map[go_t] = gql_t
  }

  valid_nodes := ObjTypeMap{}
  valid_lockables := ObjTypeMap{}
  valid_threads := ObjTypeMap{}

  node_type := reflect.TypeOf((*GraphNode)(nil)).Elem()
  lockable_type := reflect.TypeOf((*Lockable)(nil)).Elem()
  thread_type := reflect.TypeOf((*Thread)(nil)).Elem()

  for go_t, gql_t := range(type_map) {
    if go_t.Implements(node_type) {
      valid_nodes[go_t] = gql_t
    }
    if go_t.Implements(lockable_type) {
      valid_lockables[go_t] = gql_t
    }
    if go_t.Implements(thread_type) {
      valid_threads[go_t] = gql_t
    }
    type_list = append(type_list, gql_t)
  }

  queries := graphql.Fields{
    "Self": GQLQuerySelf(),
  }

  for key, val := range(extended_queries) {
    queries[key] = val
  }

  subscriptions := graphql.Fields{
    "Update": GQLSubscriptionUpdate(),
    "Self": GQLSubscriptionSelf(),
  }

  for key, val := range(extended_subscriptions) {
    subscriptions[key] = val
  }

  mutations := graphql.Fields{
    "SendUpdate": GQLMutationSendUpdate(),
  }

  for key, val := range(extended_mutations) {
    mutations[key] = val
  }

  schemaConfig  := graphql.SchemaConfig{
    Types: type_list,
    Query: graphql.NewObject(graphql.ObjectConfig{
      Name: "Query",
      Fields: queries,
    }),
    Mutation: graphql.NewObject(graphql.ObjectConfig{
      Name: "Mutation",
      Fields: mutations,
    }),
    Subscription: graphql.NewObject(graphql.ObjectConfig{
      Name: "Subscription",
      Fields: subscriptions,
    }),
  }

  schema, err := graphql.NewSchema(schemaConfig)
  if err != nil{
    return nil, err
  }

  ctx := GQLContext{
    Schema: schema,
    ValidNodes: valid_nodes,
    NodeType: node_type,
    ValidThreads: valid_threads,
    ThreadType: thread_type,
    ValidLockables: valid_lockables,
    LockableType: lockable_type,
  }

  return &ctx, nil
}

func LoadNode(ctx * GraphContext, id NodeID) (GraphNode, error) {
  // Initialize an empty list of loaded nodes, then start loading them from id
  loaded_nodes := map[NodeID]GraphNode{}
  return LoadNodeRecurse(ctx, id, loaded_nodes)
}

type DBJSONBase struct {
  Type string `json:"type"`
}

// Check if a node is already loaded, load it's state bytes from the DB and parse the type if it's not already loaded
// Call the node load function related to the type, which will call this parse function recusively as needed
func LoadNodeRecurse(ctx * GraphContext, id NodeID, loaded_nodes map[NodeID]GraphNode) (GraphNode, error) {
  node, exists := loaded_nodes[id]
  if exists == false {
    state_bytes, err := ReadDBState(ctx, id)
    if err != nil {
      return nil, err
    }

    var base DBJSONBase
    err = json.Unmarshal(state_bytes, &base)
    if err != nil {
      return nil, err
    }

    ctx.Log.Logf("graph", "GRAPH_DB_LOAD: %s(%s)", base.Type, id)

    node_fn, exists := ctx.NodeLoadFuncs[base.Type]
    if exists == false {
      return nil, fmt.Errorf("%s is not a known node type: %s", base.Type, state_bytes)
    }

    node, err = node_fn(ctx, id)
    if err != nil {
      return nil, err
    }

    loaded_nodes[id] = node

    state_fn, exists := ctx.StateLoadFuncs[base.Type]
    if exists == false {
      return nil, fmt.Errorf("%s is not a known node state type", base.Type)
    }

    state, err := state_fn(ctx, state_bytes, loaded_nodes)
    if err != nil {
      return nil, err
    }

    node.SetState(state)
  }
  return node, nil
}

func NewGraphContext(db * badger.DB, log Logger, state_loads StateLoadMap, node_loads NodeLoadMap, info_loads InfoLoadMap, types TypeList, type_map ObjTypeMap, queries FieldMap, subscriptions FieldMap, mutations FieldMap) * GraphContext {
  gql, err := NewGQLContext(types, type_map, queries, subscriptions, mutations)
  if err != nil {
    panic(err)
  }

  ctx := GraphContext{
    GQL: gql,
    DB: db,
    Log: log,
    NodeLoadFuncs: NodeLoadMap{
      "simple_lockable": LoadSimpleLockable,
      "simple_thread": LoadSimpleThread,
      "gql_thread": LoadGQLThread,
    },
    StateLoadFuncs: StateLoadMap{
      "simple_lockable": LoadSimpleLockableState,
      "simple_thread": LoadSimpleThreadState,
      "gql_thread": LoadGQLThreadState,
    },
    InfoLoadFuncs: InfoLoadMap{
      "gql_thread": LoadGQLThreadInfo,
    },
  }

  for name, fn := range(state_loads) {
    ctx.StateLoadFuncs[name] = fn
  }

  for name, fn := range(node_loads) {
    ctx.NodeLoadFuncs[name] = fn
  }

  for name, fn := range(info_loads) {
    ctx.InfoLoadFuncs[name] = fn
  }

  return &ctx
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
  // Human-readable name of the node, not guaranteed to be unique
  Name() string
  // Type of the node this state is attached to. Used to deserialize the state to a node from the database
  Type() string
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

const NODE_SIGNAL_BUFFER = 256

func RestoreNode(ctx * GraphContext, id NodeID) BaseNode {
  node := BaseNode{
    id: id,
    signal: make(chan GraphSignal, NODE_SIGNAL_BUFFER),
    listeners: map[chan GraphSignal]chan GraphSignal{},
    state: nil,
  }

  ctx.Log.Logf("graph", "RESTORE_NODE: %s", node.id)
  return node
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

// Create a new base node with a new ID
func NewNode(ctx * GraphContext, state NodeState) (BaseNode, error) {
  node := BaseNode{
    id: RandID(),
    signal: make(chan GraphSignal, NODE_SIGNAL_BUFFER),
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

func ReadDBState(ctx * GraphContext, id NodeID) ([]byte, error) {
  var bytes []byte
  err := ctx.DB.View(func(txn *badger.Txn) error {
    item, err := txn.Get([]byte(id))
    if err != nil {
      return err
    }

    return item.Value(func(val []byte) error {
      bytes = append([]byte{}, val...)
      return nil
    })
  })

  if err != nil {
    ctx.Log.Logf("db", "DB_READ_ERR: %s - %e", id, err)
    return nil, err
  }

  ctx.Log.Logf("db", "DB_READ: %s - %s", id, string(bytes))

  return bytes, nil
}

func WriteDBStates(ctx * GraphContext, nodes NodeMap) error{
  ctx.Log.Logf("db", "DB_WRITES: %d", len(nodes))
  serialized_states := map[NodeID][]byte{}
  for _, node := range(nodes) {
    ser, err := json.Marshal(node.State())
    if err != nil {
      return fmt.Errorf("DB_MARSHAL_ERROR: %e", err)
    }
    serialized_states[node.ID()] = ser
  }

  err := ctx.DB.Update(func(txn *badger.Txn) error {
    i := 0
    for id, _ := range(nodes) {
      ctx.Log.Logf("db", "DB_WRITE: %s - %s", id, string(serialized_states[id]))
      err := txn.Set([]byte(id), serialized_states[id])
      if err != nil {
        return fmt.Errorf("DB_MARSHAL_ERROR: %e", err)
      }
      i++
    }
    return nil
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

type NodeStateMap map[NodeID]NodeState
type NodeMap map[NodeID]GraphNode
type StatesFn func(states NodeStateMap) error
type NodesFn func(nodes NodeMap) error
func UseStates(ctx * GraphContext, nodes []GraphNode, states_fn StatesFn) error {
  states := NodeStateMap{}
  return UseMoreStates(ctx, nodes, states, states_fn)
}
func UseMoreStates(ctx * GraphContext, nodes []GraphNode, states NodeStateMap, states_fn StatesFn) error {
  err := checkForDuplicate(nodes)
  if err != nil {
    return err
  }

  locked_nodes := []GraphNode{}
  for _, node := range(nodes) {
    _, locked := states[node.ID()]
    if locked == false {
      node.StateLock().RLock()
      states[node.ID()] = node.State()
      locked_nodes = append(locked_nodes, node)
    }
  }

  err = states_fn(states)

  for _, node := range(locked_nodes) {
    delete(states, node.ID())
    node.StateLock().RUnlock()
  }

  return err
}

func UpdateStates(ctx * GraphContext, nodes []GraphNode, nodes_fn NodesFn) error {
  locked_nodes := NodeMap{}
  err := UpdateMoreStates(ctx, nodes, locked_nodes, nodes_fn)
  if err == nil {
    err = WriteDBStates(ctx, locked_nodes)
  }

  for _, node := range(locked_nodes) {
    node.StateLock().Unlock()
  }
  return err
}
func UpdateMoreStates(ctx * GraphContext, nodes []GraphNode, locked_nodes NodeMap, nodes_fn NodesFn) error {
  for _, node := range(nodes) {
    _, locked := locked_nodes[node.ID()]
    if locked == false {
      node.StateLock().Lock()
      locked_nodes[node.ID()] = node
    }
  }

  return nodes_fn(locked_nodes)
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

