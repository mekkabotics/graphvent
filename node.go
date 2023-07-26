package graphvent

import (
  "sync"
  "reflect"
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
func (id NodeID) MarshalJSON() ([]byte, error) {
  str := id.String()
  return json.Marshal(&str)
}

var ZeroUUID = uuid.UUID{}
var ZeroID = NodeID(ZeroUUID)

func (id NodeID) Serialize() []byte {
  ser, _ := (uuid.UUID)(id).MarshalBinary()
  return ser
}

func (id NodeID) String() string {
  return (uuid.UUID)(id).String()
}

// Ignore the error since we're enforcing 16 byte length at compile time
func IDFromBytes(bytes [16]byte) NodeID {
  id, _ := uuid.FromBytes(bytes[:])
  return NodeID(id)
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

// Generate a random NodeID
func RandID() NodeID {
  return NodeID(uuid.New())
}

type Serializable[I comparable] interface {
  Type() I
  Serialize() ([]byte, error)
}

// NodeExtensions are additional data that can be attached to nodes, and used in node functions
type Extension interface {
  Serializable[ExtType]
  // Send a signal to this extension to process,
  // this typically triggers signals to be sent to nodes linked in the extension
  Process(context *StateContext, node *Node, signal GraphSignal) error
}

// Nodes represent an addressible group of extensions
type Node struct {
  ID NodeID
  Type NodeType
  Lock sync.RWMutex
  ExtensionMap map[ExtType]Extension
}

func GetExt[T Extension](node *Node) (T, error) {
  var zero T
  ext_type := zero.Type()
  ext, exists := node.ExtensionMap[ext_type]
  if exists == false {
    return zero, fmt.Errorf("%s does not have %s extension", node.ID, ext_type)
  }

  ret, ok := ext.(T)
  if ok == false {
    return zero, fmt.Errorf("%s in %s is wrong type(%+v), expecting %+v", ext_type, node.ID, reflect.TypeOf(ext), reflect.TypeOf(zero))
  }

  return ret, nil
}

func (node *Node) Serialize() ([]byte, error) {
  extensions := make([]ExtensionDB, len(node.ExtensionMap))
  node_db := NodeDB{
    Header: NodeDBHeader{
      Magic: NODE_DB_MAGIC,
      NumExtensions: uint32(len(extensions)),
    },
    Extensions: extensions,
  }

  i := 0
  for ext_type, info := range(node.ExtensionMap) {
    ser, err := info.Serialize()
    if err != nil {
      return nil, err
    }
    node_db.Extensions[i] = ExtensionDB{
      Header: ExtensionDBHeader{
        TypeHash: ext_type.Hash(),
        Length: uint64(len(ser)),
      },
      Data: ser,
    }
    i += 1
  }

  return node_db.Serialize(), nil
}

func NewNode(id NodeID, node_type NodeType) Node {
  return Node{
    ID: id,
    Type: node_type,
    ExtensionMap: map[ExtType]Extension{},
  }
}

func Allowed(context *StateContext, principal *Node, action string, node *Node) error {
  if principal == nil {
    context.Graph.Log.Logf("policy", "POLICY_CHECK_ERR: %s %s.%s", principal.ID, node.ID, action)
    return fmt.Errorf("nil is not allowed to perform any actions")
  }

  ext, exists := node.ExtensionMap[ACLExtType]
  if exists == false {
    return fmt.Errorf("%s does not have ACL extension, other nodes cannot perform actions on it", node.ID)
  }
  acl_ext := ext.(ACLExt)

  for _, policy_node := range(acl_ext.Delegations) {
    ext, exists := policy_node.ExtensionMap[ACLPolicyExtType]
    if exists == false {
      context.Graph.Log.Logf("policy", "WARNING: %s has dependency %s which doesn't have ACLPolicyExt")
      continue
    }
    policy_ext := ext.(ACLPolicyExt)
    if policy_ext.Allows(context, principal, action, node) == true {
      context.Graph.Log.Logf("policy", "POLICY_CHECK_PASS: %s %s.%s", principal.ID, node.ID, action)
      return nil
    }
  }
  context.Graph.Log.Logf("policy", "POLICY_CHECK_FAIL: %s %s.%s.%s", principal.ID, node.ID, action)
  return fmt.Errorf("%s is not allowed to perform %s on %s", principal.ID, action, node.ID)
}

// Check that princ is allowed to signal this action,
// then send the signal to all the extensions of the node
func Signal(context *StateContext, node *Node, princ *Node, signal GraphSignal) error {
  context.Graph.Log.Logf("signal", "SIGNAL: %s - %s", node.ID, signal.String())

  err := UseStates(context, princ, NewACLInfo(node, []string{}), func(context *StateContext) error {
    return Allowed(context, princ, fmt.Sprintf("signal.%s", signal.Type()), node)
  })

  for _, ext := range(node.ExtensionMap) {
    err = ext.Process(context, node, signal)
    if err != nil {
      return nil
    }
  }

  return nil
}

// Magic first four bytes of serialized DB content, stored big endian
const NODE_DB_MAGIC = 0x2491df14
// Total length of the node database header, has magic to verify and type_hash to map to load function
const NODE_DB_HEADER_LEN = 16
// A DBHeader is parsed from the first NODE_DB_HEADER_LEN bytes of a serialized DB node
type NodeDBHeader struct {
  Magic uint32
  NumExtensions uint32
  TypeHash uint64
}

type NodeDB struct {
  Header NodeDBHeader
  Extensions []ExtensionDB
}

//TODO: add size safety checks
func NewNodeDB(data []byte) (NodeDB, error) {
  var zero NodeDB

  ptr := 0

  magic := binary.BigEndian.Uint32(data[0:4])
  num_extensions := binary.BigEndian.Uint32(data[4:8])
  node_type_hash := binary.BigEndian.Uint64(data[8:16])

  ptr += NODE_DB_HEADER_LEN

  if magic != NODE_DB_MAGIC {
    return zero, fmt.Errorf("header has incorrect magic 0x%x", magic)
  }

  extensions := make([]ExtensionDB, num_extensions)
  for i, _ := range(extensions) {
    cur := data[ptr:]

    type_hash := binary.BigEndian.Uint64(cur[0:8])
    length := binary.BigEndian.Uint64(cur[8:16])

    data_start := uint64(EXTENSION_DB_HEADER_LEN)
    data_end := data_start + length
    ext_data := cur[data_start:data_end]

    extensions[i] = ExtensionDB{
      Header: ExtensionDBHeader{
        TypeHash: type_hash,
        Length: length,
      },
      Data: ext_data,
    }

    ptr += int(EXTENSION_DB_HEADER_LEN + length)
  }

  return NodeDB{
    Header: NodeDBHeader{
      Magic: magic,
      TypeHash: node_type_hash,
      NumExtensions: num_extensions,
    },
    Extensions: extensions,
  }, nil
}

func (header NodeDBHeader) Serialize() []byte {
  if header.Magic != NODE_DB_MAGIC {
    panic(fmt.Sprintf("Serializing header with invalid magic %0x", header.Magic))
  }

  ret := make([]byte, NODE_DB_HEADER_LEN)
  binary.BigEndian.PutUint32(ret[0:4], header.Magic)
  binary.BigEndian.PutUint32(ret[4:8], header.NumExtensions)
  binary.BigEndian.PutUint64(ret[8:16], header.TypeHash)
  return ret
}

func (node NodeDB) Serialize() []byte {
  ser := node.Header.Serialize()
  for _, extension := range(node.Extensions) {
    ser = append(ser, extension.Serialize()...)
  }

  return ser
}

func (header ExtensionDBHeader) Serialize() []byte {
  ret := make([]byte, EXTENSION_DB_HEADER_LEN)
  binary.BigEndian.PutUint64(ret[0:8], header.TypeHash)
  binary.BigEndian.PutUint64(ret[8:16], header.Length)
  return ret
}

func (extension ExtensionDB) Serialize() []byte {
  header_bytes := extension.Header.Serialize()
  return append(header_bytes, extension.Data...)
}

const EXTENSION_DB_HEADER_LEN = 16
type ExtensionDBHeader struct {
  TypeHash uint64
  Length uint64
}

type ExtensionDB struct {
  Header ExtensionDBHeader
  Data []byte
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
  // TODO, just write states from the context, and store the current states in the context
  for id, _ := range(context.Locked) {
    node, _ := context.Graph.Nodes[id]
    if node == nil {
      return fmt.Errorf("DB_SERIALIZE_ERROR: cannot serialize nil node, maybe node isn't in the context")
    }

    ser, err := node.Serialize()
    if err != nil {
      return fmt.Errorf("DB_SERIALIZE_ERROR: %s", err)
    }

    id_ser := node.ID.Serialize()

    serialized_bytes[i] = ser
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

// Recursively load a node from the database.
func LoadNode(ctx * Context, id NodeID) (*Node, error) {
  node, exists := ctx.Nodes[id]
  if exists == true {
    return node,nil
  }

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
    return nil, err
  }

  // Parse the bytes from the DB
  node_db, err := NewNodeDB(bytes)
  if err != nil {
    return nil, err
  }

  node_type, known := ctx.Types[node_db.Header.TypeHash]
  if known == false {
    return nil, fmt.Errorf("Tried to load node %s of type 0x%x, which is not a known node type", id, node_db.Header.TypeHash)
  }

  // Create the blank node with the ID, and add it to the context
  new_node := NewNode(id, node_type.Type)
  node = &new_node
  ctx.Nodes[id] = node

  // Parse each of the extensions from the db
  for _, ext_db := range(node_db.Extensions) {
    type_hash := ext_db.Header.TypeHash
    def, known := ctx.Extensions[type_hash]
    if known == false {
      return nil, fmt.Errorf("%s tried to load extension 0x%x, which is not a known extension type", id, type_hash)
    }
    extension, err := def.Load(ctx, ext_db.Data)
    if err != nil {
      return nil, err
    }
    node.ExtensionMap[def.Type] = extension
    ctx.Log.Logf("db", "DB_EXTENSION_LOADED: %s - 0x%x", id, type_hash)
  }

  ctx.Log.Logf("db", "DB_NODE_LOADED: %s", id)
  return node, nil
}

func NewACLInfo(node *Node, resources []string) ACLMap {
  return ACLMap{
    node.ID: ACLInfo{
      Node: node,
      Resources: resources,
    },
  }
}

func NewACLMap(requests ...ACLMap) ACLMap {
  reqs := ACLMap{}
  for _, req := range(requests) {
    for id, info := range(req) {
      reqs[id] = info
    }
  }
  return reqs
}

func ACLListM(m map[NodeID]*Node, resources[]string) ACLMap {
  reqs := ACLMap{}
  for _, node := range(m) {
    reqs[node.ID] = ACLInfo{
      Node: node,
      Resources: resources,
    }
  }
  return reqs
}

func ACLList(list []*Node, resources []string) ACLMap {
  reqs := ACLMap{}
  for _, node := range(list) {
    reqs[node.ID] = ACLInfo{
      Node: node,
      Resources: resources,
    }
  }
  return reqs
}

type NodeType string
func (node NodeType) Hash() uint64 {
  hash := sha512.Sum512([]byte(fmt.Sprintf("NODE: %s", string(node))))
  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}

type PolicyType string
func (policy PolicyType) Hash() uint64 {
  hash := sha512.Sum512([]byte(fmt.Sprintf("POLICY: %s", string(policy))))
  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}

type ExtType string
func (ext ExtType) Hash() uint64 {
  hash := sha512.Sum512([]byte(fmt.Sprintf("EXTENSION: %s", string(ext))))
  return binary.BigEndian.Uint64(hash[(len(hash)-9):(len(hash)-1)])
}

type NodeMap map[NodeID]*Node

type ACLInfo struct {
  Node *Node
  Resources []string
}

type ACLMap map[NodeID]ACLInfo
type ExtMap map[uint64]Extension

// Context of running state usage(read/write)
type StateContext struct {
  // Type of the state context
  Type string
  // The wrapped graph context
  Graph *Context
  // Granted permissions in the context
  Permissions map[NodeID]ACLMap
  // Locked extensions in the context
  Locked map[NodeID]*Node

  // Context state for validation
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
    Permissions: map[NodeID]ACLMap{},
    Locked: map[NodeID]*Node{},
    Started: false,
    Finished: false,
  }
}

func NewWriteContext(ctx *Context) *StateContext {
  return &StateContext{
    Type: "write",
    Graph: ctx,
    Permissions: map[NodeID]ACLMap{},
    Locked: map[NodeID]*Node{},
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
func UseStates(context *StateContext, principal *Node, new_nodes ACLMap, state_fn StateFn) error {
  if principal == nil || new_nodes == nil || state_fn == nil {
    return fmt.Errorf("nil passed to UseStates")
  }

  err := ValidateStateContext(context, "read", false)
  if err != nil {
    return err
  }

  if context.Started == false {
    context.Started = true
  }

  new_locks := []*Node{}
  _, princ_locked := context.Locked[principal.ID]
  if princ_locked == false {
    new_locks = append(new_locks, principal)
    context.Graph.Log.Logf("mutex", "RLOCKING_PRINC %s", principal.ID.String())
    principal.Lock.RLock()
  }

  princ_permissions, princ_exists := context.Permissions[principal.ID]
  new_permissions := ACLMap{}
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
    id := node.ID

    if id != principal.ID {
      _, locked := context.Locked[id]
      if locked == false {
        new_locks = append(new_locks, node)
        context.Graph.Log.Logf("mutex", "RLOCKING %s", id.String())
        node.Lock.RLock()
      }
    }

    node_permissions, node_exists := new_permissions[id]
    if node_exists == false {
      node_permissions = ACLInfo{Node: node, Resources: []string{}}
    }

    for _, resource := range(request.Resources) {
      already_granted := false
      for _, granted := range(node_permissions.Resources) {
        if resource == granted {
          already_granted = true
        }
      }

      if already_granted == false {
        err := Allowed(context, principal, fmt.Sprintf("%s.read", resource), node)
        if err != nil {
          for _, n := range(new_locks) {
            context.Graph.Log.Logf("mutex", "RUNLOCKING_ON_ERROR %s", id.String())
            n.Lock.RUnlock()
          }
          return err
        }
      }
    }
    new_permissions[id] = node_permissions
  }

  for _, node := range(new_locks) {
    context.Locked[node.ID] = node
  }

  context.Permissions[principal.ID] = new_permissions

  err = state_fn(context)

  context.Permissions[principal.ID] = princ_permissions

  for _, node := range(new_locks) {
    context.Graph.Log.Logf("mutex", "RUNLOCKING %s", node.ID.String())
    delete(context.Locked, node.ID)
    node.Lock.RUnlock()
  }

  return err
}

// Add nodes to an existing write context and call nodes_fn with nodes locked for read
// If context is nil
func UpdateStates(context *StateContext, principal *Node, new_nodes ACLMap, state_fn StateFn) error {
  if principal == nil || new_nodes == nil || state_fn == nil {
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

  new_locks := []*Node{}
  _, princ_locked := context.Locked[principal.ID]
  if princ_locked == false {
    new_locks = append(new_locks, principal)
    context.Graph.Log.Logf("mutex", "LOCKING_PRINC %s", principal.ID.String())
    principal.Lock.Lock()
  }

  princ_permissions, princ_exists := context.Permissions[principal.ID]
  new_permissions := ACLMap{}
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
    id := node.ID

    if id != principal.ID {
      _, locked := context.Locked[id]
      if locked == false {
        new_locks = append(new_locks, node)
        context.Graph.Log.Logf("mutex", "LOCKING %s", id.String())
        node.Lock.Lock()
      }
    }

    node_permissions, node_exists := new_permissions[id]
    if node_exists == false {
      node_permissions = ACLInfo{Node: node, Resources: []string{}}
    }

    for _, resource := range(request.Resources) {
      already_granted := false
      for _, granted := range(node_permissions.Resources) {
        if resource == granted {
          already_granted = true
        }
      }

      if already_granted == false {
        err := Allowed(context, principal, fmt.Sprintf("%s.write", resource), node)
        if err != nil {
          for _, n := range(new_locks) {
            context.Graph.Log.Logf("mutex", "UNLOCKING_ON_ERROR %s", id.String())
            n.Lock.Unlock()
          }
          return err
        }
      }
    }
    new_permissions[id] = node_permissions
  }

  for _, node := range(new_locks) {
    context.Locked[node.ID] = node
  }

  context.Permissions[principal.ID] = new_permissions

  err = state_fn(context)

  if final == true {
    context.Finished = true
    if err == nil {
      err = WriteNodes(context)
    }
    for id, node := range(context.Locked) {
      context.Graph.Log.Logf("mutex", "UNLOCKING %s", id.String())
      node.Lock.Unlock()
    }
  }

  return err
}

