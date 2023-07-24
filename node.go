package graphvent

import (
  "sync"
  "github.com/google/uuid"
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "encoding/binary"
  "encoding/json"
  "crypto/sha512"
  "crypto/ecdsa"
  "crypto/elliptic"
)

// IDs are how nodes are uniquely identified, and can be serialized for the database
type NodeID uuid.UUID

var ZeroUUID = uuid.UUID{}
var ZeroID = NodeID(ZeroUUID)

func (id NodeID) Serialize() []byte {
  ser, _ := (uuid.UUID)(id).MarshalBinary()
  return ser
}

func (id NodeID) String() string {
  return (uuid.UUID)(id).String()
}

func ParseID(str string) (NodeID, error) {
  id_uuid, err := uuid.Parse(str)
  if err != nil {
    return NodeID{}, err
  }
  return NodeID(id_uuid), nil
}

func KeyID(pub *ecdsa.PublicKey) NodeID {
  ser := elliptic.Marshal(pub.Curve, pub.X, pub.Y)
  str := uuid.NewHash(sha512.New(), ZeroUUID, ser, 3)
  return NodeID(str)
}

// Types are how nodes are associated with structs at runtime(and from the DB)
type NodeType string
func (node_type NodeType) Hash() uint64 {
  hash := sha512.Sum512([]byte(node_type))

  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}

// Generate a random NodeID
func RandID() NodeID {
  return NodeID(uuid.New())
}

// A Node represents data that can be read by multiple goroutines and written to by one, with a unique ID attached, and a method to process updates(including propagating them to connected nodes)
// RegisterChannel and UnregisterChannel are used to connect arbitrary listeners to the node
type Node interface {
  // State Locking interface
  sync.Locker
  RLock()
  RUnlock()

  // Serialize the Node for the database
  Serialize() ([]byte, error)

  // Nodes have an ID, type, and ACL policies
  ID() NodeID
  Type() NodeType

  Policies() map[NodeID]Policy
  AddPolicy(Policy) error
  RemovePolicy(Policy) error

  // Send a GraphSignal to the node, requires that the node is locked for read so that it can propagate
  Process(context *StateContext, princ Node, signal GraphSignal) error
  // Register a channel to receive updates sent to the node
  RegisterChannel(id NodeID, listener chan GraphSignal)
  // Unregister a channel from receiving updates sent to the node
  UnregisterChannel(id NodeID)
}

// A GraphNode is an implementation of a Node that can be embedded into more complex structures
type GraphNode struct {
  sync.RWMutex
  listeners_lock sync.Mutex

  id NodeID
  listeners map[NodeID]chan GraphSignal
  policies map[NodeID]Policy
}

type GraphNodeJSON struct {
  Policies []string `json:"policies"`
}

func (node * GraphNode) Policies() map[NodeID]Policy {
  return node.policies
}

func (node * GraphNode) Serialize() ([]byte, error) {
  node_json := NewGraphNodeJSON(node)
  return json.MarshalIndent(&node_json, "", "  ")
}

func Allowed(context *StateContext, policies map[NodeID]Policy, node Node, resource string, action string, princ Node) error {
  if princ == nil {
    context.Graph.Log.Logf("policy", "POLICY_CHECK_ERR: %s %s.%s.%s", princ.ID(), node.ID(), resource, action)
    return fmt.Errorf("nil is not allowed to perform any actions")
  }
  if node.ID() == princ.ID() {
    return nil
  }
  for _, policy := range(policies) {
    if policy.Allows(node, resource, action, princ) == true {
      context.Graph.Log.Logf("policy", "POLICY_CHECK_PASS: %s %s.%s.%s", princ.ID(), node.ID(), resource, action)
      return nil
    }
  }
  context.Graph.Log.Logf("policy", "POLICY_CHECK_FAIL: %s %s.%s.%s", princ.ID(), node.ID(), resource, action)
  return fmt.Errorf("%s is not allowed to perform %s.%s on %s", princ.ID(), resource, action, node.ID())
}

func (node *GraphNode) AddPolicy(policy Policy) error {
  if policy == nil {
    return fmt.Errorf("Cannot add nil as a policy")
  }

  _, exists := node.policies[policy.ID()]
  if exists == true {
    return fmt.Errorf("%s is already a policy for %s", policy.ID().String(), node.ID().String())
  }

  node.policies[policy.ID()] = policy
  return nil
}

func (node *GraphNode) RemovePolicy(policy Policy) error {
  if policy == nil {
    return fmt.Errorf("Cannot add nil as a policy")
  }

  _, exists := node.policies[policy.ID()]
  if exists == false {
    return fmt.Errorf("%s is not a policy for %s", policy.ID().String(), node.ID().String())
  }

  delete(node.policies, policy.ID())
  return nil
}

func NewGraphNodeJSON(node *GraphNode) GraphNodeJSON {
  policies := make([]string, len(node.policies))
  i := 0
  for _, policy := range(node.policies) {
    policies[i] = policy.ID().String()
    i += 1
  }
  return GraphNodeJSON{
    Policies: policies,
  }
}

func RestoreGraphNode(ctx *Context, node Node, j GraphNodeJSON, nodes NodeMap) error {
  for _, policy_str := range(j.Policies) {
    policy_id, err := ParseID(policy_str)
    if err != nil {
      return err
    }
    policy_ptr, err := LoadNodeRecurse(ctx, policy_id, nodes)
    if err != nil {
      return err
    }

    policy, ok := policy_ptr.(Policy)
    if ok == false {
      return fmt.Errorf("%s is not a Policy", policy_id)
    }
    node.AddPolicy(policy)
  }
  return nil
}

func LoadGraphNode(ctx * Context, id NodeID, data []byte, nodes NodeMap)(Node, error) {
  if len(data) > 0 {
    return nil, fmt.Errorf("Attempted to load a graph_node with data %+v, should have been 0 length", string(data))
  }
  node := NewGraphNode(id)
  return &node, nil
}

func (node * GraphNode) ID() NodeID {
  return node.id
}

func (node * GraphNode) Type() NodeType {
  return NodeType("graph_node")
}

// Propagate the signal to registered listeners, if a listener isn't ready to receive the update
// send it a notification that it was closed and then close it
func Signal(context *StateContext, node Node, princ Node, signal GraphSignal) error {
  context.Graph.Log.Logf("signal", "SIGNAL: %s - %s", node.ID(), signal.String())

  err := UseStates(context, princ, NewLockInfo(node, []string{}), func(context *StateContext) error {
    return Allowed(context, node.Policies(), node, "signal", signal.Type(), princ)
  })

  if err != nil {
    return nil
  }

  return node.Process(context, princ, signal)
}

func (node * GraphNode) Process(context *StateContext, princ Node, signal GraphSignal) error {
  node.listeners_lock.Lock()
  defer node.listeners_lock.Unlock()
  closed := []NodeID{}

  for id, listener := range node.listeners {
    context.Graph.Log.Logf("signal", "UPDATE_LISTENER %s: %s", node.ID(), id)
    select {
    case listener <- signal:
    default:
      context.Graph.Log.Logf("signal", "CLOSED_LISTENER %s: %s", node.ID(), id)
      go func(node Node, listener chan GraphSignal) {
        listener <- NewDirectSignal("listener_closed")
        close(listener)
      }(node, listener)
      closed = append(closed, id)
    }
  }

  for _, id := range(closed) {
    delete(node.listeners, id)
  }
  return nil
}

func (node * GraphNode) RegisterChannel(id NodeID, listener chan GraphSignal) {
  node.listeners_lock.Lock()
  _, exists := node.listeners[id]
  if exists == false {
    node.listeners[id] = listener
  }
  node.listeners_lock.Unlock()
}

func (node * GraphNode) UnregisterChannel(id NodeID) {
  node.listeners_lock.Lock()
  _, exists := node.listeners[id]
  if exists == false {
    panic("Attempting to unregister non-registered listener")
  } else {
    delete(node.listeners, id)
  }
  node.listeners_lock.Unlock()
}

func AttachPolicies(ctx *Context, node Node, policies ...Policy) error {
  context := NewWriteContext(ctx)
  return UpdateStates(context, node, NewLockInfo(node, []string{"policies"}), func(context *StateContext) error {
    for _, policy := range(policies) {
      err := node.AddPolicy(policy)
      if err != nil {
        return err
      }
    }
    return nil
  })
}

func NewGraphNode(id NodeID) GraphNode {
  return GraphNode{
    id: id,
    listeners: map[NodeID]chan GraphSignal{},
    policies: map[NodeID]Policy{},
  }
}

// Magic first four bytes of serialized DB content, stored big endian
const NODE_DB_MAGIC = 0x2491df14
// Total length of the node database header, has magic to verify and type_hash to map to load function
const NODE_DB_HEADER_LEN = 12
// A DBHeader is parsed from the first NODE_DB_HEADER_LEN bytes of a serialized DB node
type DBHeader struct {
  Magic uint32
  TypeHash uint64
}

func (header DBHeader) Serialize() []byte {
  if header.Magic != NODE_DB_MAGIC {
    panic(fmt.Sprintf("Serializing header with invalid magic %0x", header.Magic))
  }

  ret := make([]byte, NODE_DB_HEADER_LEN)
  binary.BigEndian.PutUint32(ret[0:4], header.Magic)
  binary.BigEndian.PutUint64(ret[4:12], header.TypeHash)
  return ret
}

func NewDBHeader(node_type NodeType) DBHeader {
  return DBHeader{
    Magic: NODE_DB_MAGIC,
    TypeHash: node_type.Hash(),
  }
}

// Internal function to serialize a node and wrap it with the DB Header
func getNodeBytes(node Node) ([]byte, error) {
  if node == nil {
    return nil, fmt.Errorf("DB_SERIALIZE_ERROR: cannot serialize nil node")
  }
  ser, err := node.Serialize()
  if err != nil {
    return nil, fmt.Errorf("DB_SERIALIZE_ERROR: %s", err)
  }

  header := NewDBHeader(node.Type())

  db_data := append(header.Serialize(), ser...)

  return db_data, nil
}

// Write multiple nodes to the database in a single transaction
func WriteNodes(context *StateContext) error {
  err := ValidateStateContext(context, "write", true)
  if err != nil {
    return err
  }

  context.Graph.Log.Logf("db", "DB_WRITES: %d", len(context.Locked))

  serialized_bytes := make([][]byte, len(context.Locked))
  serialized_ids := make([][]byte, len(context.Locked))
  i := 0
  for _, node := range(context.Locked) {
    node_bytes, err := getNodeBytes(node)
    context.Graph.Log.Logf("db", "DB_WRITE: %+v", node)
    if err != nil {
      return err
    }

    id_ser := node.ID().Serialize()

    serialized_bytes[i] = node_bytes
    serialized_ids[i] = id_ser

    i++
  }

  return context.Graph.DB.Update(func(txn *badger.Txn) error {
    for i, id := range(serialized_ids) {
      err := txn.Set(id, serialized_bytes[i])
      if err != nil {
        return err
      }
    }
    return nil
  })
}

// Get the bytes associates with `id` from the database after unwrapping the header, or error
func readNodeBytes(ctx * Context, id NodeID) (uint64, []byte, error) {
  var bytes []byte
  err := ctx.DB.View(func(txn *badger.Txn) error {
    item, err := txn.Get(id.Serialize())
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
    return 0, nil, err
  }

  if len(bytes) < NODE_DB_HEADER_LEN {
    return 0, nil, fmt.Errorf("header for %s is %d/%d bytes", id, len(bytes), NODE_DB_HEADER_LEN)
  }

  header := DBHeader{}
  header.Magic = binary.BigEndian.Uint32(bytes[0:4])
  header.TypeHash = binary.BigEndian.Uint64(bytes[4:12])

  if header.Magic != NODE_DB_MAGIC {
    return 0, nil, fmt.Errorf("header for %s, invalid magic 0x%x", id, header.Magic)
  }

  node_bytes := make([]byte, len(bytes) - NODE_DB_HEADER_LEN)
  copy(node_bytes, bytes[NODE_DB_HEADER_LEN:])

  ctx.Log.Logf("db", "DB_READ: %s - %s", id, string(bytes))

  return header.TypeHash, node_bytes, nil
}

// Load a Node from the database by ID
func LoadNode(ctx * Context, id NodeID) (Node, error) {
  nodes := NodeMap{}
  return LoadNodeRecurse(ctx, id, nodes)
}


// Recursively load a node from the database.
// It's expected that node_type.Load adds the newly loaded node to nodes before calling LoadNodeRecurse again.
func LoadNodeRecurse(ctx * Context, id NodeID, nodes NodeMap) (Node, error) {
  node, exists := nodes[id]
  if exists == false {
    type_hash, bytes, err := readNodeBytes(ctx, id)
    if err != nil {
      return nil, err
    }

    node_type, exists := ctx.Types[type_hash]
    if exists == false {
      return nil, fmt.Errorf("0x%x is not a known node type: %+s", type_hash, bytes)
    }

    if node_type.Load == nil {
      return nil, fmt.Errorf("0x%x is an invalid node type, nil Load", type_hash)
    }

    node, err = node_type.Load(ctx, id, bytes, nodes)
    if err != nil {
      return nil, err
    }

    ctx.Log.Logf("db", "DB_NODE_LOADED: %s", id)
  }
  return node, nil
}

func NewLockInfo(node Node, resources []string) LockMap {
  return LockMap{
    node.ID(): LockInfo{
      Node: node,
      Resources: resources,
    },
  }
}

func NewLockMap(requests ...LockMap) LockMap {
  reqs := LockMap{}
  for _, req := range(requests) {
    for id, info := range(req) {
      reqs[id] = info
    }
  }
  return reqs
}

func LockList[K Node](list []K, resources []string) LockMap {
  reqs := LockMap{}
  for _, node := range(list) {
    reqs[node.ID()] = LockInfo{
      Node: node,
      Resources: resources,
    }
  }
  return reqs
}


type NodeMap map[NodeID]Node

type LockInfo struct {
  Node Node
  Resources []string
}

type LockMap map[NodeID]LockInfo

type StateContext struct {
  Type string
  Graph *Context
  Permissions map[NodeID]LockMap
  Locked NodeMap
  Started bool
  Finished bool
}

func ValidateStateContext(context *StateContext, Type string, Finished bool) error {
  if context == nil {
    return fmt.Errorf("context is nil")
  }
  if context.Finished != Finished {
    return fmt.Errorf("context in wrong Finished state")
  }
  if context.Type != Type {
    return fmt.Errorf("%s is not a %s context", context.Type, Type)
  }
  if context.Locked == nil || context.Graph == nil || context.Permissions == nil {
    return fmt.Errorf("context is not initialized correctly")
  }
  return nil
}

func NewReadContext(ctx *Context) *StateContext {
  return &StateContext{
    Type: "read",
    Graph: ctx,
    Permissions: map[NodeID]LockMap{},
    Locked: NodeMap{},
    Started: false,
    Finished: false,
  }
}

func NewWriteContext(ctx *Context) *StateContext {
  return &StateContext{
    Type: "write",
    Graph: ctx,
    Permissions: map[NodeID]LockMap{},
    Locked: NodeMap{},
    Started: false,
    Finished: false,
  }
}

type StateFn func(*StateContext)(error)

func del[K comparable](list []K, val K) []K {
  idx := -1
  for i, v := range(list) {
    if v == val {
      idx = i
      break
    }
  }
  if idx == -1 {
    return nil
  }

  list[idx] = list[len(list)-1]
  return list[:len(list)-1]
}

// Add nodes to an existing read context and call nodes_fn with new_nodes locked for read
// Check that the node has read permissions for the nodes, then add them to the read context and call nodes_fn with the nodes locked for read
func UseStates(context *StateContext, princ Node, new_nodes LockMap, state_fn StateFn) error {
  if princ == nil || new_nodes == nil || state_fn == nil {
    return fmt.Errorf("nil passed to UseStates")
  }

  err := ValidateStateContext(context, "read", false)
  if err != nil {
    return err
  }

  if context.Started == false {
    context.Started = true
  }

  new_locks := []Node{}
  _, princ_locked := context.Locked[princ.ID()]
  if princ_locked == false {
    new_locks = append(new_locks, princ)
    context.Graph.Log.Logf("mutex", "RLOCKING_PRINC %s", princ.ID().String())
    princ.RLock()
  }

  princ_permissions, princ_exists := context.Permissions[princ.ID()]
  new_permissions := LockMap{}
  if princ_exists == true {
    for id, info := range(princ_permissions) {
      new_permissions[id] = info
    }
  }

  for _, request := range(new_nodes) {
    node := request.Node
    if node == nil {
      return fmt.Errorf("node in request list is nil")
    }
    id := node.ID()

    if id != princ.ID() {
      _, locked := context.Locked[id]
      if locked == false {
        new_locks = append(new_locks, node)
        context.Graph.Log.Logf("mutex", "RLOCKING %s", id.String())
        node.RLock()
      }
    }

    node_permissions, node_exists := new_permissions[id]
    if node_exists == false {
      node_permissions = LockInfo{Node: node, Resources: []string{}}
    }

    for _, resource := range(request.Resources) {
      already_granted := false
      for _, granted := range(node_permissions.Resources) {
        if resource == granted {
          already_granted = true
        }
      }

      if already_granted == false {
        err := Allowed(context, node.Policies(), node, resource, "read", princ)
        if err != nil {
          for _, n := range(new_locks) {
            context.Graph.Log.Logf("mutex", "RUNLOCKING_ON_ERROR %s", id.String())
            n.RUnlock()
          }
          return err
        }
      }
    }
    new_permissions[id] = node_permissions
  }

  for _, node := range(new_locks) {
    context.Locked[node.ID()] = node
  }

  context.Permissions[princ.ID()] = new_permissions

  err = state_fn(context)

  context.Permissions[princ.ID()] = princ_permissions

  for _, node := range(new_locks) {
    context.Graph.Log.Logf("mutex", "RUNLOCKING %s", node.ID().String())
    delete(context.Locked, node.ID())
    node.RUnlock()
  }

  return err
}

// Add nodes to an existing write context and call nodes_fn with nodes locked for read
// If context is nil
func UpdateStates(context *StateContext, princ Node, new_nodes LockMap, state_fn StateFn) error {
  if princ == nil || new_nodes == nil || state_fn == nil {
    return fmt.Errorf("nil passed to UpdateStates")
  }

  err := ValidateStateContext(context, "write", false)
  if err != nil {
    return err
  }

  final := false
  if context.Started == false {
    context.Started = true
    final = true
  }

  new_locks := []Node{}
  _, princ_locked := context.Locked[princ.ID()]
  if princ_locked == false {
    new_locks = append(new_locks, princ)
    context.Graph.Log.Logf("mutex", "LOCKING_PRINC %s", princ.ID().String())
    princ.Lock()
  }

  princ_permissions, princ_exists := context.Permissions[princ.ID()]
  new_permissions := LockMap{}
  if princ_exists == true {
    for id, info := range(princ_permissions) {
      new_permissions[id] = info
    }
  }

  for _, request := range(new_nodes) {
    node := request.Node
    if node == nil {
      return fmt.Errorf("node in request list is nil")
    }
    id := node.ID()

    if id != princ.ID() {
      _, locked := context.Locked[id]
      if locked == false {
        new_locks = append(new_locks, node)
        context.Graph.Log.Logf("mutex", "LOCKING %s", id.String())
        node.Lock()
      }
    }

    node_permissions, node_exists := new_permissions[id]
    if node_exists == false {
      node_permissions = LockInfo{Node: node, Resources: []string{}}
    }

    for _, resource := range(request.Resources) {
      already_granted := false
      for _, granted := range(node_permissions.Resources) {
        if resource == granted {
          already_granted = true
        }
      }

      if already_granted == false {
        err := Allowed(context, node.Policies(), node, resource, "write", princ)
        if err != nil {
          for _, n := range(new_locks) {
            context.Graph.Log.Logf("mutex", "UNLOCKING_ON_ERROR %s", id.String())
            n.Unlock()
          }
          return err
        }
      }
    }
    new_permissions[id] = node_permissions
  }

  for _, node := range(new_locks) {
    context.Locked[node.ID()] = node
  }

  context.Permissions[princ.ID()] = new_permissions

  err = state_fn(context)

  if final == true {
    context.Finished = true
    if err == nil {
      err = WriteNodes(context)
    }
    for id, node := range(context.Locked) {
      context.Graph.Log.Logf("mutex", "UNLOCKING %s", id.String())
      node.Unlock()
    }
  }

  return err
}

// Create a new channel with a buffer the size of buffer, and register it to node with the id
func UpdateChannel(node Node, buffer int, id NodeID) chan GraphSignal {
  if node == nil {
    panic("Cannot get an update channel to nil")
  }
  new_listener := make(chan GraphSignal, buffer)
  node.RegisterChannel(id, new_listener)
  return new_listener
}
