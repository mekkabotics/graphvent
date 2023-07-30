package graphvent

import (
  "time"
  "errors"
  "reflect"
  "github.com/google/uuid"
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "encoding/binary"
  "encoding/json"
  "sync/atomic"
  "crypto/ecdsa"
  "crypto/elliptic"
  "crypto/sha512"
  "crypto/rand"
  "crypto/x509"
)

const (
  // Magic first four bytes of serialized DB content, stored big endian
  NODE_DB_MAGIC = 0x2491df14
  // Total length of the node database header, has magic to verify and type_hash to map to load function
  NODE_DB_HEADER_LEN = 28
  EXTENSION_DB_HEADER_LEN = 16
)

var (
  // Base NodeID, used as a special value
  ZeroUUID = uuid.UUID{}
  ZeroID = NodeID(ZeroUUID)
)

// A NodeID uniquely identifies a Node
type NodeID uuid.UUID
func (id *NodeID) MarshalJSON() ([]byte, error) {
  str := ""
  if id != nil {
    str = id.String()
  }
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
  Field(string)interface{}
  Process(context *Context, source NodeID, node *Node, signal Signal)
}

// A QueuedSignal is a Signal that has been Queued to trigger at a set time
type QueuedSignal struct {
  uuid.UUID
  Signal
  time.Time
}

// Default message channel size for nodes
// Nodes represent a group of extensions that can be collectively addressed
type Node struct {
  Key *ecdsa.PrivateKey
  ID NodeID
  Type NodeType
  Extensions map[ExtType]Extension

  // Channel for this node to receive messages from the Context
  MsgChan chan Msg
  // Size of MsgChan
  BufferSize uint32
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  SignalQueue []QueuedSignal
  NextSignal *QueuedSignal
}

func (node *Node) QueueSignal(time time.Time, signal Signal) uuid.UUID {
  id := uuid.New()
  node.SignalQueue = append(node.SignalQueue, QueuedSignal{id, signal, time})
  node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
  return id
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
    if signal.Time.Compare(soonest_time) == 1 || soonest_signal == nil {
      soonest_signal = &signal
      soonest_time = signal.Time
    }
  }

  if soonest_signal != nil {
    print("QUEUE: ")
    print(fmt.Sprintf("%+v", signals))
    print(" - SOONEST: ")
    print(soonest_signal.Time.String())
    print(" : ")
    println(soonest_signal.Signal.Type())
    return soonest_signal, time.After(time.Until(soonest_signal.Time))
  } else {
    println("NONE_QUEUED")
    return nil, nil
  }
}

func runNode(ctx *Context, node *Node) {
  ctx.Log.Logf("node", "RUN_START: %s", node.ID)
  err := nodeLoop(ctx, node)
  if err != nil {
    panic(err)
  }
  ctx.Log.Logf("node", "RUN_STOP: %s", node.ID)
}

type Msg struct {
  Source NodeID
  Signal Signal
}

func ReadNodeFields(ctx *Context, self *Node, princ NodeID, reqs map[ExtType][]string)map[ExtType]map[string]interface{} {
  exts := map[ExtType]map[string]interface{}{}
  for ext_type, field_reqs := range(reqs) {
    fields := map[string]interface{}{}
    for _, req := range(field_reqs) {
      err := Allowed(ctx, princ, MakeAction(ReadSignalType, ext_type, req), self)
      if err != nil {
        fields[req] = err
      } else {
        ext, exists := self.Extensions[ext_type]
        if exists == false {
          fields[req] = fmt.Errorf("%s does not have %s extension", self.ID, ext_type)
        } else {
          fields[req] = ext.Field(req)
        }
      }
    }
    exts[ext_type] = fields
  }
  return exts
}

// Main Loop for Threads, starts a write context, so cannot be called from a write or read context
func nodeLoop(ctx *Context, node *Node) error {
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
      err := Allowed(ctx, msg.Source, signal.Permission(), node)
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_POLICY_ERR: %s", err)
        continue
      }
    case <-node.TimeoutChan:
      signal = node.NextSignal.Signal
      t := node.NextSignal.Time
      source = node.ID
      i := -1
      for j, queued := range(node.SignalQueue) {
        if queued.UUID == node.NextSignal.UUID {
          i = j
          break
        }
      }
      if i == -1 {
        panic("node.NextSignal not in node.SignalQueue")
      }
      l := len(node.SignalQueue)
      node.SignalQueue[i] = node.SignalQueue[l-1]
      node.SignalQueue = node.SignalQueue[:(l-1)]

      node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
      if node.NextSignal == nil {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL nil", node.ID, t, signal)
      } else {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL: %s@%s", node.ID, t, signal, node.NextSignal, node.NextSignal.Time)
      }
    }

    // Handle special signal types
    if signal.Type() == StopSignalType {
      node.Process(ctx, node.ID, NewStatusSignal("stopped", node.ID))
      break
    } else if signal.Type() == ReadSignalType {
      read_signal, ok := signal.(ReadSignal)
      if ok == false {
        ctx.Log.Logf("signal", "READ_SIGNAL: bad cast %+v", signal)
      } else {
        result := ReadNodeFields(ctx, node, source, read_signal.Extensions)
        ctx.Send(node.ID, source, NewReadResultSignal(read_signal.UUID, node.Type, result))
      }
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

func GetCtx[T Extension, C any](ctx *Context) (C, error) {
  var zero T
  var zero_ctx C
  ext_type := zero.Type()
  type_hash := Hash(ext_type)
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

  key_bytes, err := x509.MarshalECPrivateKey(node.Key)
  if err != nil {
    return nil, err
  }

  node_db := NodeDB{
    Header: NodeDBHeader{
      Magic: NODE_DB_MAGIC,
      TypeHash: Hash(node.Type),
      KeyLength: uint32(len(key_bytes)),
      BufferSize: node.BufferSize,
      NumExtensions: uint32(len(extensions)),
      NumQueuedSignals: uint32(len(node.SignalQueue)),
    },
    Extensions: extensions,
    QueuedSignals: node.SignalQueue,
    KeyBytes: key_bytes,
  }

  i := 0
  for ext_type, info := range(node.Extensions) {
    ser, err := info.Serialize()
    if err != nil {
      return nil, err
    }
    node_db.Extensions[i] = ExtensionDB{
      Header: ExtensionDBHeader{
        TypeHash: Hash(ext_type),
        Length: uint64(len(ser)),
      },
      Data: ser,
    }
    i += 1
  }

  return node_db.Serialize(), nil
}

func KeyID(pub *ecdsa.PublicKey) NodeID {
  ser := elliptic.Marshal(pub.Curve, pub.X, pub.Y)
  str := uuid.NewHash(sha512.New(), ZeroUUID, ser, 3)
  return NodeID(str)
}

// Create a new node in memory and start it's event loop
// TODO: Change panics to errors
func NewNode(ctx *Context, key *ecdsa.PrivateKey, node_type NodeType, buffer_size uint32, queued_signals []QueuedSignal, extensions ...Extension) *Node {
  var err error
  if key == nil {
    key, err = ecdsa.GenerateKey(ctx.ECDSA, rand.Reader)
    if err != nil {
      panic(err)
    }
  }
  id := KeyID(&key.PublicKey)
  _, exists := ctx.Node(id)
  if exists == true {
    panic("Attempted to create an existing node")
  }

  def, exists := ctx.Types[Hash(node_type)]
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
    Key: key,
    ID: id,
    Type: node_type,
    Extensions: ext_map,
    MsgChan: make(chan Msg, buffer_size),
    BufferSize: buffer_size,
    TimeoutChan: timeout_chan,
    SignalQueue: queued_signals,
    NextSignal: next_signal,
  }
  ctx.AddNode(id, node)
  err = WriteNode(ctx, node)
  if err != nil {
    panic(err)
  }

  go runNode(ctx, node)

  return node
}

func Allowed(ctx *Context, principal_id NodeID, action Action, node *Node) error {
  ctx.Log.Logf("policy", "POLICY_CHECK: %s -> %s.%s", principal_id, node.ID, action)
  // Nodes are allowed to perform all actions on themselves regardless of whether or not they have an ACL extension
  if principal_id == node.ID {
    ctx.Log.Logf("policy", "POLICY_CHECK_SAME_NODE: %s.%s", principal_id, action)
    return nil
  }

  // Check if the node has a policy extension itself, and check against the policies in it
  policy_ext, err := GetExt[*ACLExt](node)
  if err != nil {
    ctx.Log.Logf("policy", "POLICY_CHECK_NO_ACL_EXT: %s", node.ID)
    return err
  }

  err = policy_ext.Allows(ctx, principal_id, action, node)
  if err != nil {
    ctx.Log.Logf("policy", "POLICY_CHECK_FAIL: %s -> %s.%s : %s", principal_id, node.ID, action, err)
  } else {
    ctx.Log.Logf("policy", "POLICY_CHECK_PASS: %s -> %s.%s", principal_id, node.ID, action)
  }
  return err
}

// A DBHeader is parsed from the first NODE_DB_HEADER_LEN bytes of a serialized DB node
type NodeDBHeader struct {
  Magic uint32
  NumExtensions uint32
  NumQueuedSignals uint32
  BufferSize uint32
  KeyLength uint32
  TypeHash uint64
}

type NodeDB struct {
  Header NodeDBHeader
  QueuedSignals []QueuedSignal
  Extensions []ExtensionDB
  KeyBytes []byte
}

//TODO: add size safety checks
func NewNodeDB(data []byte) (NodeDB, error) {
  var zero NodeDB

  ptr := 0

  magic := binary.BigEndian.Uint32(data[0:4])
  num_extensions := binary.BigEndian.Uint32(data[4:8])
  num_queued_signals := binary.BigEndian.Uint32(data[8:12])
  buffer_size := binary.BigEndian.Uint32(data[12:16])
  key_length := binary.BigEndian.Uint32(data[16:20])
  node_type_hash := binary.BigEndian.Uint64(data[20:28])

  ptr += NODE_DB_HEADER_LEN

  if magic != NODE_DB_MAGIC {
    return zero, fmt.Errorf("header has incorrect magic 0x%x", magic)
  }

  key_bytes := make([]byte, key_length)
  n := copy(key_bytes, data[ptr:(ptr+int(key_length))])
  if n != int(key_length) {
    return zero, fmt.Errorf("not enough key bytes: %d", n)
  }

  ptr += int(key_length)

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
      BufferSize: buffer_size,
      KeyLength: key_length,
      NumExtensions: num_extensions,
      NumQueuedSignals: num_queued_signals,
    },
    KeyBytes: key_bytes,
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
  binary.BigEndian.PutUint32(ret[12:16], header.BufferSize)
  binary.BigEndian.PutUint32(ret[16:20], header.KeyLength)
  binary.BigEndian.PutUint64(ret[20:28], header.TypeHash)
  return ret
}

func (node NodeDB) Serialize() []byte {
  ser := node.Header.Serialize()
  ser = append(ser, node.KeyBytes...)
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
  ctx.Log.Logf("db", "DB_WRITE_ID: %+v", id_bytes)

  return ctx.DB.Update(func(txn *badger.Txn) error {
    return txn.Set(id_bytes, bytes)
  })
}

func LoadNode(ctx * Context, id NodeID) (*Node, error) {
  ctx.Log.Logf("db", "LOADING_NODE: %s", id)
  var bytes []byte
  err := ctx.DB.View(func(txn *badger.Txn) error {
    id_bytes := id.Serialize()
    ctx.Log.Logf("db", "DB_READ_ID: %+v", id_bytes)
    item, err := txn.Get(id_bytes)
    if err != nil {
      return err
    }

    return item.Value(func(val []byte) error {
      bytes = append([]byte{}, val...)
      return nil
    })
  })
  if errors.Is(err, badger.ErrKeyNotFound) {
    return nil, NodeNotFoundError
  }else if err != nil {
    return nil, err
  }

  // Parse the bytes from the DB
  node_db, err := NewNodeDB(bytes)
  if err != nil {
    return nil, err
  }

  key, err := x509.ParseECPrivateKey(node_db.KeyBytes)
  if err != nil {
    return nil, err
  }

  if key.PublicKey.Curve != ctx.ECDSA {
    return nil, fmt.Errorf("%s - wrong ec curve for private key: %+v, expected %+v", id, key.PublicKey.Curve, ctx.ECDSA)
  }

  key_id := KeyID(&key.PublicKey)
  if key_id != id {
    return nil, fmt.Errorf("KeyID(%s) != %s", key_id, id)
  }

  node_type, known := ctx.Types[node_db.Header.TypeHash]
  if known == false {
    return nil, fmt.Errorf("Tried to load node %s of type 0x%x, which is not a known node type", id, node_db.Header.TypeHash)
  }

  next_signal, timeout_chan := SoonestSignal(node_db.QueuedSignals)
  node := &Node{
    Key: key,
    ID: key_id,
    Type: node_type.Type,
    Extensions: map[ExtType]Extension{},
    MsgChan: make(chan Msg, node_db.Header.BufferSize),
    BufferSize: node_db.Header.BufferSize,
    TimeoutChan: timeout_chan,
    SignalQueue: node_db.QueuedSignals,
    NextSignal: next_signal,
  }
  ctx.AddNode(id, node)

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

  go runNode(ctx, node)

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

func IDMap[S any, T map[NodeID]S](m T)map[string]S {
  ret := map[string]S{}
  for id, val := range(m) {
    ret[id.String()] = val
  }
  return ret
}

func LoadIDMap[S any, T map[string]S](m T)(map[NodeID]S, error) {
  ret := map[NodeID]S{}
  for str, val := range(m) {
    id, err := ParseID(str)
    if err != nil {
      return nil, err
    }
    ret[id] = val
  }
  return ret, nil
}
