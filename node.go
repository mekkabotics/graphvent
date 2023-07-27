package graphvent

import (
  "time"
  "reflect"
  "github.com/google/uuid"
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "encoding/binary"
  "encoding/json"
  "crypto/sha512"
  "sync/atomic"
)

// A NodeID uniquely identifies a Node
type NodeID uuid.UUID
func (id NodeID) MarshalJSON() ([]byte, error) {
  str := id.String()
  return json.Marshal(&str)
}
func (id *NodeID) UnmarshalJSON(bytes []byte) error {
  var id_str string
  err := json.Unmarshal(bytes, &id_str)
  if err != nil {
    return err
  }

  *id, err = ParseID(id_str)
  return err
}

// Base NodeID, used as a special value
var ZeroUUID = uuid.UUID{}
var ZeroID = NodeID(ZeroUUID)

func (id NodeID) Serialize() []byte {
  ser, _ := (uuid.UUID)(id).MarshalBinary()
  return ser
}

func (id NodeID) String() string {
  return (uuid.UUID)(id).String()
}

// Create an ID from a fixed length byte array
// Ignore the error since we're enforcing 16 byte length at compile time
func IDFromBytes(bytes [16]byte) NodeID {
  id, _ := uuid.FromBytes(bytes[:])
  return NodeID(id)
}

// Parse an ID from a string
func ParseID(str string) (NodeID, error) {
  id_uuid, err := uuid.Parse(str)
  if err != nil {
    return NodeID{}, err
  }
  return NodeID(id_uuid), nil
}

// Generate a random NodeID
func RandID() NodeID {
  return NodeID(uuid.New())
}

// A Serializable has a type that can be used to map to it, and a function to serialize the current state
type Serializable[I comparable] interface {
  Type() I
  Serialize() ([]byte, error)
}

// Extensions are data attached to nodes that process signals
type Extension interface {
  Serializable[ExtType]
  Process(context *Context, source NodeID, node *Node, signal Signal)
}

// A QueuedSignal is a Signal that has been Queued to trigger at a set time
type QueuedSignal struct {
  Signal Signal
  Time time.Time
}

// Default message channel size for nodes
const NODE_MSG_CHAN_DEFAULT = 1024
// Nodes represent a group of extensions that can be collectively addressed
type Node struct {
  ID NodeID
  Type NodeType
  Extensions map[ExtType]Extension

  // Channel for this node to receive messages from the Context
  MsgChan chan Msg
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  SignalQueue []QueuedSignal
  NextSignal *QueuedSignal
}

func (node *Node) QueueSignal(time time.Time, signal Signal) {
  node.SignalQueue = append(node.SignalQueue, QueuedSignal{signal, time})
  node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
}

func (node *Node) ClearSignalQueue() {
  node.SignalQueue = []QueuedSignal{}
  node.NextSignal = nil
  node.TimeoutChan = nil
}

func SoonestSignal(signals []QueuedSignal) (*QueuedSignal, <-chan time.Time) {
  var soonest_signal *QueuedSignal
  var soonest_time time.Time
  for _, signal := range(signals) {
    if signal.Time.Compare(soonest_time) == -1 || soonest_signal == nil {
      soonest_signal = &signal
      soonest_time = signal.Time
    }
  }

  if soonest_signal != nil {
    return soonest_signal, time.After(time.Until(soonest_time))
  } else {
    return nil, nil
  }
}

func RunNode(ctx *Context, node *Node) {
  ctx.Log.Logf("node", "RUN_START: %s", node.ID)
  err := NodeLoop(ctx, node)
  if err != nil {
    panic(err)
  }
  ctx.Log.Logf("node", "RUN_STOP: %s", node.ID)
}

type Msg struct {
  Source NodeID
  Signal Signal
}

// Main Loop for Threads, starts a write context, so cannot be called from a write or read context
func NodeLoop(ctx *Context, node *Node) error {
  started := node.Active.CompareAndSwap(false, true)
  if started == false {
    return fmt.Errorf("%s is already started, will not start again", node.ID)
  }
  for true {
    var signal Signal
    var source NodeID
    select {
    case msg := <- node.MsgChan:
      signal = msg.Signal
      source = msg.Source
      err := Allowed(ctx, msg.Source, string(signal.Type()), node)
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_POLICY_ERR: %s", err)
        continue
      }
    case <-node.TimeoutChan:
      signal = node.NextSignal.Signal
      source = node.ID
      node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
      ctx.Log.Logf("node", "NODE_TIMEOUT %s - NEXT_SIGNAL: %s", node.ID, signal)
    }

    // Handle special signal types
    if signal.Type() == StopSignalType {
      node.Process(ctx, node.ID, NewStatusSignal("stopped", node.ID))
      break
    }
    node.Process(ctx, source, signal)
  }

  stopped := node.Active.CompareAndSwap(true, false)
  if stopped == false {
    panic("BAD_STATE: stopping already stopped node")
  }
  return nil
}

func (node *Node) Process(ctx *Context, source NodeID, signal Signal) {
  for ext_type, ext := range(node.Extensions) {
    ctx.Log.Logf("signal", "PROCESSING_EXTENSION: %s/%s", node.ID, ext_type)
    ext.Process(ctx, source, node, signal)
  }
}

func (node *Node) Signal(ctx *Context, dest NodeID, signal Signal) error {
  return ctx.Send(node.ID, dest, signal)
}

func GetCtx[T Extension, C any](ctx *Context) (C, error) {
  var zero T
  var zero_ctx C
  ext_type := zero.Type()
  type_hash := ext_type.Hash()
  ext_info, ok := ctx.Extensions[type_hash]
  if ok == false {
    return zero_ctx, fmt.Errorf("%s is not an extension in ctx", ext_type)
  }

  ext_ctx, ok := ext_info.Data.(C)
  if ok == false {
    return zero_ctx, fmt.Errorf("context for %s is %+v, not %+v", ext_type, reflect.TypeOf(ext_info.Data), reflect.TypeOf(zero))
  }

  return ext_ctx, nil
}

func GetExt[T Extension](node *Node) (T, error) {
  var zero T
  ext_type := zero.Type()
  ext, exists := node.Extensions[ext_type]
  if exists == false {
    return zero, fmt.Errorf("%s does not have %s extension - %+v", node.ID, ext_type, node.Extensions)
  }

  ret, ok := ext.(T)
  if ok == false {
    return zero, fmt.Errorf("%s in %s is wrong type(%+v), expecting %+v", ext_type, node.ID, reflect.TypeOf(ext), reflect.TypeOf(zero))
  }

  return ret, nil
}

func (node *Node) Serialize() ([]byte, error) {
  extensions := make([]ExtensionDB, len(node.Extensions))
  node_db := NodeDB{
    Header: NodeDBHeader{
      Magic: NODE_DB_MAGIC,
      TypeHash: node.Type.Hash(),
      NumExtensions: uint32(len(extensions)),
      NumQueuedSignals: uint32(len(node.SignalQueue)),
    },
    Extensions: extensions,
    QueuedSignals: node.SignalQueue,
  }

  i := 0
  for ext_type, info := range(node.Extensions) {
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

// Create a new node in memory and start it's event loop
func NewNode(ctx *Context, id NodeID, node_type NodeType, queued_signals []QueuedSignal, extensions ...Extension) *Node {
  _, exists := ctx.Nodes[id]
  if exists == true {
    panic("Attempted to create an existing node")
  }

  def, exists := ctx.Types[node_type.Hash()]
  if exists == false {
    panic("Node type %s not registered in Context")
  }

  ext_map := map[ExtType]Extension{}
  for _, ext := range(extensions) {
    _, exists := ext_map[ext.Type()]
    if exists == true {
      panic("Cannot add the same extension to a node twice")
    }
    ext_map[ext.Type()] = ext
  }

  for _, required_ext := range(def.Extensions) {
    _, exists := ext_map[required_ext]
    if exists == false {
      panic(fmt.Sprintf("%s requires %s", node_type, required_ext))
    }
  }

  if queued_signals == nil {
    queued_signals = []QueuedSignal{}
  }

  next_signal, timeout_chan := SoonestSignal(queued_signals)

  node := &Node{
    ID: id,
    Type: node_type,
    Extensions: ext_map,
    MsgChan: make(chan Msg, NODE_MSG_CHAN_DEFAULT),
    TimeoutChan: timeout_chan,
    SignalQueue: queued_signals,
    NextSignal: next_signal,
  }
  ctx.Nodes[id] = node
  WriteNode(ctx, node)

  go RunNode(ctx, node)

  return node
}

func Allowed(ctx *Context, principal_id NodeID, action string, node *Node) error {
  ctx.Log.Logf("policy", "POLICY_CHECK: %s %s.%s", principal_id, node.ID, action)
  // Nodes are allowed to perform all actions on themselves regardless of whether or not they have an ACL extension
  if principal_id == node.ID {
    return nil
  }

  // Check if the node has a policy extension itself, and check against the policies in it
  policy_ext, err := GetExt[*ACLExt](node)
  if err != nil {
    return err
  }

  return policy_ext.Allows(ctx, principal_id, action, node)
}

// Magic first four bytes of serialized DB content, stored big endian
const NODE_DB_MAGIC = 0x2491df14
// Total length of the node database header, has magic to verify and type_hash to map to load function
const NODE_DB_HEADER_LEN = 20
// A DBHeader is parsed from the first NODE_DB_HEADER_LEN bytes of a serialized DB node
type NodeDBHeader struct {
  Magic uint32
  NumExtensions uint32
  NumQueuedSignals uint32
  TypeHash uint64
}

type NodeDB struct {
  Header NodeDBHeader
  QueuedSignals []QueuedSignal
  Extensions []ExtensionDB
}

//TODO: add size safety checks
func NewNodeDB(data []byte) (NodeDB, error) {
  var zero NodeDB

  ptr := 0

  magic := binary.BigEndian.Uint32(data[0:4])
  num_extensions := binary.BigEndian.Uint32(data[4:8])
  num_queued_signals := binary.BigEndian.Uint32(data[8:12])
  node_type_hash := binary.BigEndian.Uint64(data[12:20])

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

  queued_signals := make([]QueuedSignal, num_queued_signals)
  for i, _ := range(queued_signals) {
    queued_signals[i] = QueuedSignal{}
  }

  return NodeDB{
    Header: NodeDBHeader{
      Magic: magic,
      TypeHash: node_type_hash,
      NumExtensions: num_extensions,
      NumQueuedSignals: num_queued_signals,
    },
    Extensions: extensions,
    QueuedSignals: queued_signals,
  }, nil
}

func (header NodeDBHeader) Serialize() []byte {
  if header.Magic != NODE_DB_MAGIC {
    panic(fmt.Sprintf("Serializing header with invalid magic %0x", header.Magic))
  }

  ret := make([]byte, NODE_DB_HEADER_LEN)
  binary.BigEndian.PutUint32(ret[0:4], header.Magic)
  binary.BigEndian.PutUint32(ret[4:8], header.NumExtensions)
  binary.BigEndian.PutUint32(ret[8:12], header.NumQueuedSignals)
  binary.BigEndian.PutUint64(ret[12:20], header.TypeHash)
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

// Write a node to the database
func WriteNode(ctx *Context, node *Node) error {
  ctx.Log.Logf("db", "DB_WRITE: %s", node.ID)

  bytes, err := node.Serialize()
  if err != nil {
    return err
  }

  id_bytes := node.ID.Serialize()

  return ctx.DB.Update(func(txn *badger.Txn) error {
    return txn.Set(id_bytes, bytes)
  })
}

func LoadNode(ctx * Context, id NodeID) (*Node, error) {
  ctx.Log.Logf("db", "LOOKING_FOR_NODE: %s", id)
  node, exists := ctx.Nodes[id]
  if exists == true {
    ctx.Log.Logf("db", "NODE_ALREADY_LOADED: %s", id)
    return node,nil
  }
  ctx.Log.Logf("db", "LOADING_NODE: %s", id)

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

  next_signal, timeout_chan := SoonestSignal(node_db.QueuedSignals)
  node = &Node{
    ID: id,
    Type: node_type.Type,
    Extensions: map[ExtType]Extension{},
    MsgChan: make(chan Msg, NODE_MSG_CHAN_DEFAULT),
    TimeoutChan: timeout_chan,
    SignalQueue: node_db.QueuedSignals,
    NextSignal: next_signal,
  }
  ctx.Nodes[id] = node

  found_extensions := []ExtType{}
  // Parse each of the extensions from the db
  for _, ext_db := range(node_db.Extensions) {
    type_hash := ext_db.Header.TypeHash
    def, known := ctx.Extensions[type_hash]
    if known == false {
      return nil, fmt.Errorf("%s tried to load extension 0x%x, which is not a known extension type", id, type_hash)
    }
    ctx.Log.Logf("db", "DB_EXTENSION_LOADING: %s/%s", id, def.Type)
    extension, err := def.Load(ctx, ext_db.Data)
    if err != nil {
      return nil, err
    }
    node.Extensions[def.Type] = extension
    found_extensions = append(found_extensions, def.Type)
    ctx.Log.Logf("db", "DB_EXTENSION_LOADED: %s/%s - %+v", id, def.Type, extension)
  }

  missing_extensions := []ExtType{}
  for _, ext := range(node_type.Extensions) {
    found := false
    for _, found_ext := range(found_extensions) {
      if found_ext == ext {
        found = true
        break
      }
    }
    if found == false {
      missing_extensions = append(missing_extensions, ext)
    }
  }

  if len(missing_extensions) > 0 {
    return nil, fmt.Errorf("DB_LOAD_MISSING_EXTENSIONS: %s - %+v - %+v", id, node_type, missing_extensions)
  }

  extra_extensions := []ExtType{}
  for _, found_ext := range(found_extensions) {
    found := false
    for _, ext := range(node_type.Extensions) {
      if ext == found_ext {
        found = true
        break
      }
    }
    if found == false {
      extra_extensions = append(extra_extensions, found_ext)
    }
  }

  if len(extra_extensions) > 0 {
    ctx.Log.Logf("db", "DB_LOAD_EXTRA_EXTENSIONS: %s - %+v - %+v", id, node_type, extra_extensions)
  }

  ctx.Log.Logf("db", "DB_NODE_LOADED: %s", id)

  go RunNode(ctx, node)

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
