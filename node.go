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
  "crypto/ed25519"
  "crypto/sha512"
  "crypto/rand"
  "crypto/x509"
)

const (
  // Magic first four bytes of serialized DB content, stored big endian
  NODE_DB_MAGIC = 0x2491df14
  // Total length of the node database header, has magic to verify and type_hash to map to load function
  NODE_DB_HEADER_LEN = 32
  EXTENSION_DB_HEADER_LEN = 16
  QSIGNAL_DB_HEADER_LEN = 40
  POLICY_DB_HEADER_LEN = 16
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

// A Serializable has a type that can be used to map to it, and a function to serialize` the current state
type Serializable[I comparable] interface {
  Serialize()([]byte,error)
  Deserialize(*Context,[]byte)error
  Type() I
}

// Extensions are data attached to nodes that process signals
type Extension interface {
  Serializable[ExtType]
  Field(string)interface{}
  Process(ctx *Context, node *Node, message Message)[]Message
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
  Key ed25519.PrivateKey
  ID NodeID
  Type NodeType
  Extensions map[ExtType]Extension
  Policies map[PolicyType]Policy

  // Channel for this node to receive messages from the Context
  MsgChan chan Message
  // Size of MsgChan
  BufferSize uint32
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  SignalQueue []QueuedSignal
  NextSignal *QueuedSignal
}

func (node *Node) Allows(principal_id NodeID, action Action) error {
  errs := []error{}
  for _, policy := range(node.Policies) {
    err := policy.Allows(principal_id, action, node)
    if err == nil {
      return nil
    }
    errs = append(errs, err)
  }
  return fmt.Errorf("POLICY_CHECK_ERRORS: %s %s.%s - %+v", principal_id, node.ID, action, errs)
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
  for i, signal := range(signals) {
    if signal.Time.Compare(soonest_time) == -1 || soonest_signal == nil {
      soonest_signal = &signals[i]
      soonest_time = signal.Time
    }
  }

  if soonest_signal != nil {
    return soonest_signal, time.After(time.Until(soonest_signal.Time))
  } else {
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

func ReadNodeFields(ctx *Context, self *Node, princ NodeID, reqs map[ExtType][]string)map[ExtType]map[string]interface{} {
  exts := map[ExtType]map[string]interface{}{}
  for ext_type, field_reqs := range(reqs) {
    fields := map[string]interface{}{}
    for _, req := range(field_reqs) {
      err := self.Allows(princ, MakeAction(ReadSignalType, ext_type, req))
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

  // Perform startup actions
  node.Process(ctx, Message{ZeroID, &StartSignal})

  for true {
    var msg Message
    select {
    case msg = <- node.MsgChan:
      ctx.Log.Logf("node_msg", "NODE_MSG: %s - %+v", node.ID, msg.Signal.Type())
    case <-node.TimeoutChan:
      signal := node.NextSignal.Signal
      msg = Message{node.ID, signal}

      t := node.NextSignal.Time
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
        ctx.Log.Logf("node_timeout", "NODE_TIMEOUT(%s) - PROCESSING %s@%s - NEXT_SIGNAL nil@%+v", node.ID, signal.Type(), t, node.TimeoutChan)
      } else {
        ctx.Log.Logf("node_timeout", "NODE_TIMEOUT(%s) - PROCESSING %s@%s - NEXT_SIGNAL: %s@%s", node.ID, signal.Type(), t, node.NextSignal, node.NextSignal.Time)
      }
    }

    // Unwrap Authorized Signals
    if msg.Signal.Type() == AuthorizedSignalType {
      sig, ok := msg.Signal.(*AuthorizedSignal)
      if ok == false {
        ctx.Log.Logf("signal", "AUTHORIZED_SIGNAL: bad cast %+v", reflect.TypeOf(msg.Signal))
      } else {
        // Validate
        sig_data, err := sig.Signal.Serialize()
        if err != nil {
        } else {
          validated := ed25519.Verify(sig.Principal, sig_data, sig.Signature)
          if validated == true {
            err := node.Allows(KeyID(sig.Principal), sig.Signal.Permission())
            if err != nil {
              ctx.Log.Logf("signal", "AUTHORIZED_SIGNAL_POLICY_ERR: %s", err)
              ctx.Send(node.ID, []Message{Message{msg.NodeID, NewErrorSignal(sig.Signal.ID(), err.Error())}})
            } else {
              // Unwrap the signal without changing the source
              msg = Message{msg.NodeID, sig.Signal}
            }
          } else {
            ctx.Log.Logf("signal", "AUTHORIZED_SIGNAL: failed to validate")
            ctx.Send(node.ID, []Message{Message{msg.NodeID, NewErrorSignal(sig.ID(), "signature validation failed")}})
          }
        }
      }
    }

    ctx.Log.Logf("node_signal_queue", "NODE_SIGNAL_QUEUE[%s]: %+v", node.ID, node.SignalQueue)

    // Handle special signal types
    if msg.Signal.Type() == StopSignalType {
      ctx.Send(node.ID, []Message{Message{msg.NodeID, NewErrorSignal(msg.Signal.ID(), "stopped")}})
      node.Process(ctx, Message{node.ID, NewStatusSignal("stopped", node.ID)})
      break
    } else if msg.Signal.Type() == ReadSignalType {
      read_signal, ok := msg.Signal.(*ReadSignal)
      if ok == false {
        ctx.Log.Logf("signal_read", "READ_SIGNAL: bad cast %+v", msg.Signal)
      } else {
        result := ReadNodeFields(ctx, node, msg.NodeID, read_signal.Extensions)
        ctx.Send(node.ID, []Message{Message{msg.NodeID, NewReadResultSignal(read_signal.ID(), node.Type, result)}})
      }
    }

    node.Process(ctx, msg)
    // assume that processing a signal means that this nodes state changed
    // TODO: remove a lot of database writes by only writing when things change,
    //  so need to have Process return whether or not state changed
    err := WriteNode(ctx, node)
    if err != nil {
      panic(err)
    }
  }

  stopped := node.Active.CompareAndSwap(true, false)
  if stopped == false {
    panic("BAD_STATE: stopping already stopped node")
  }
  return nil
}

type Message struct {
  NodeID
  Signal
}

func (node *Node) Process(ctx *Context, message Message) error {
  ctx.Log.Logf("node_process", "PROCESSING MESSAGE: %s - %+v", node.ID, message.Signal.Type())
  messages := []Message{}
  for ext_type, ext := range(node.Extensions) {
    ctx.Log.Logf("node_process", "PROCESSING_EXTENSION: %s/%s", node.ID, ext_type)
    //TODO: add extension and node info to log
    resp := ext.Process(ctx, node, message)
    if resp != nil {
      messages = append(messages, resp...)
    }
  }

  return ctx.Send(node.ID, messages)
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
  qsignals := make([]QSignalDB, len(node.SignalQueue))
  policies := make([]PolicyDB, len(node.Policies))

  key_bytes, err := x509.MarshalPKCS8PrivateKey(node.Key)
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
      NumPolicies: uint32(len(policies)),
      NumQueuedSignals: uint32(len(node.SignalQueue)),
    },
    Extensions: extensions,
    QueuedSignals: qsignals,
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

  for i, qsignal := range(node.SignalQueue) {
    ser, err := qsignal.Signal.Serialize()
    if err != nil {
      return nil, err
    }

    node_db.QueuedSignals[i] = QSignalDB{
      QSignalDBHeader{
        qsignal.Signal.ID(),
        qsignal.Time,
        Hash(qsignal.Signal.Type()),
        uint64(len(ser)),
      },
      ser,
    }
  }

  i = 0
  for _, policy := range(node.Policies) {
    ser, err := policy.Serialize()
    if err != nil {
      return nil, err
    }

    node_db.Policies[i] = PolicyDB{
      PolicyDBHeader{
        Hash(policy.Type()),
        uint64(len(ser)),
      },
      ser,
    }
  }

  return node_db.Serialize(), nil
}

func KeyID(pub ed25519.PublicKey) NodeID {
  str := uuid.NewHash(sha512.New(), ZeroUUID, pub, 3)
  return NodeID(str)
}

// Create a new node in memory and start it's event loop
// TODO: Change panics to errors
func NewNode(ctx *Context, key ed25519.PrivateKey, node_type NodeType, buffer_size uint32, policies map[PolicyType]Policy, extensions ...Extension) *Node {
  var err error
  var public ed25519.PublicKey
  if key == nil {
    public, key, err = ed25519.GenerateKey(rand.Reader)
    if err != nil {
      panic(err)
    }
  } else {
    public = key.Public().(ed25519.PublicKey)
  }
  id := KeyID(public)
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

  if policies == nil {
    policies = map[PolicyType]Policy{}
  }

  node := &Node{
    Key: key,
    ID: id,
    Type: node_type,
    Extensions: ext_map,
    Policies: policies,
    MsgChan: make(chan Message, buffer_size),
    BufferSize: buffer_size,
    SignalQueue: []QueuedSignal{},
  }
  ctx.AddNode(id, node)
  err = WriteNode(ctx, node)
  if err != nil {
    panic(err)
  }

  node.Process(ctx, Message{node.ID, &NewSignal})

  go runNode(ctx, node)

  return node
}

type PolicyDBHeader struct {
  TypeHash uint64
  Length uint64
}

type PolicyDB struct {
  Header PolicyDBHeader
  Data []byte
}

type QSignalDBHeader struct {
  SignalID uuid.UUID
  Time time.Time
  TypeHash uint64
  Length uint64
}

type QSignalDB struct {
  Header QSignalDBHeader
  Data []byte
}

type ExtensionDBHeader struct {
  TypeHash uint64
  Length uint64
}

type ExtensionDB struct {
  Header ExtensionDBHeader
  Data []byte
}

// A DBHeader is parsed from the first NODE_DB_HEADER_LEN bytes of a serialized DB node
type NodeDBHeader struct {
  Magic uint32
  NumExtensions uint32
  NumPolicies uint32
  NumQueuedSignals uint32
  BufferSize uint32
  KeyLength uint32
  TypeHash uint64
}

type NodeDB struct {
  Header NodeDBHeader
  Extensions []ExtensionDB
  Policies []PolicyDB
  QueuedSignals []QSignalDB
  KeyBytes []byte
}

//TODO: add size safety checks
func NewNodeDB(data []byte) (NodeDB, error) {
  var zero NodeDB

  ptr := 0

  magic := binary.BigEndian.Uint32(data[0:4])
  num_extensions := binary.BigEndian.Uint32(data[4:8])
  num_policies := binary.BigEndian.Uint32(data[8:12])
  num_queued_signals := binary.BigEndian.Uint32(data[12:16])
  buffer_size := binary.BigEndian.Uint32(data[16:20])
  key_length := binary.BigEndian.Uint32(data[20:24])
  node_type_hash := binary.BigEndian.Uint64(data[24:32])

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

  policies := make([]PolicyDB, num_policies)
  for i, _ := range(policies) {
    cur := data[ptr:]
    type_hash := binary.BigEndian.Uint64(cur[0:8])
    length := binary.BigEndian.Uint64(cur[8:16])

    data_start := uint64(POLICY_DB_HEADER_LEN)
    data_end := data_start + length
    policy_data := cur[data_start:data_end]

    policies[i] = PolicyDB{
      PolicyDBHeader{
        type_hash,
        length,
      },
      policy_data,
    }
    ptr += int(POLICY_DB_HEADER_LEN + length)
  }

  queued_signals := make([]QSignalDB, num_queued_signals)
  for i, _ := range(queued_signals) {
    cur := data[ptr:]
    // TODO: load a header for each with the signal type and the signal length, so that it can be deserialized and incremented
    // Right now causes segfault because any saved signal is loaded as nil
    signal_id_bytes := cur[0:16]
    unix_milli := binary.BigEndian.Uint64(cur[16:24])
    type_hash := binary.BigEndian.Uint64(cur[24:32])
    signal_size := binary.BigEndian.Uint64(cur[32:40])

    signal_id, err := uuid.FromBytes(signal_id_bytes)
    if err != nil {
      return zero, err
    }

    signal_data := cur[QSIGNAL_DB_HEADER_LEN:(QSIGNAL_DB_HEADER_LEN+signal_size)]

    queued_signals[i] = QSignalDB{
      QSignalDBHeader{
        signal_id,
        time.UnixMilli(int64(unix_milli)),
        type_hash,
        signal_size,
      },
      signal_data,
    }

    ptr += QSIGNAL_DB_HEADER_LEN + int(signal_size)
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
  binary.BigEndian.PutUint32(ret[8:12], header.NumPolicies)
  binary.BigEndian.PutUint32(ret[12:16], header.NumQueuedSignals)
  binary.BigEndian.PutUint32(ret[16:20], header.BufferSize)
  binary.BigEndian.PutUint32(ret[20:24], header.KeyLength)
  binary.BigEndian.PutUint64(ret[24:32], header.TypeHash)
  return ret
}

func (node NodeDB) Serialize() []byte {
  ser := node.Header.Serialize()
  ser = append(ser, node.KeyBytes...)
  for _, extension := range(node.Extensions) {
    ser = append(ser, extension.Serialize()...)
  }
  for _, qsignal := range(node.QueuedSignals) {
    ser = append(ser, qsignal.Serialize()...)
  }

  return ser
}

func (header QSignalDBHeader) Serialize() []byte {
  ret := make([]byte, QSIGNAL_DB_HEADER_LEN)
  id_ser, _ := header.SignalID.MarshalBinary()
  copy(ret, id_ser)
  binary.BigEndian.PutUint64(ret[16:24], uint64(header.Time.UnixMilli()))
  binary.BigEndian.PutUint64(ret[24:32], header.TypeHash)
  binary.BigEndian.PutUint64(ret[32:40], header.Length)
  return ret
}

func (qsignal QSignalDB) Serialize() []byte {
  header_bytes := qsignal.Header.Serialize()
  return append(header_bytes, qsignal.Data...)
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

  policies := make(map[PolicyType]Policy, node_db.Header.NumPolicies)
  for _, policy_db := range(node_db.Policies) {
    policy_info, exists := ctx.Policies[policy_db.Header.TypeHash]
    if exists == false {
      return nil, fmt.Errorf("0x%x is not a known policy type", policy_db.Header.TypeHash)
    }

    policy, err := policy_info.Load(ctx, policy_db.Data)
    if err != nil {
      return nil, err
    }

    policies[policy_info.Type] = policy
  }

  key_raw, err := x509.ParsePKCS8PrivateKey(node_db.KeyBytes)
  if err != nil {
    return nil, err
  }

  var key ed25519.PrivateKey
  switch k := key_raw.(type) {
  case ed25519.PrivateKey:
    key = k
  default:
    return nil, fmt.Errorf("Wrong type for private key loaded: %s - %s", id, reflect.TypeOf(k))
  }

  key_id := KeyID(key.Public().(ed25519.PublicKey))
  if key_id != id {
    return nil, fmt.Errorf("KeyID(%s) != %s", key_id, id)
  }

  node_type, known := ctx.Types[node_db.Header.TypeHash]
  if known == false {
    return nil, fmt.Errorf("Tried to load node %s of type 0x%x, which is not a known node type", id, node_db.Header.TypeHash)
  }

  signal_queue := make([]QueuedSignal, node_db.Header.NumQueuedSignals)
  for i, qsignal := range(node_db.QueuedSignals) {
    sig_info, exists := ctx.Signals[qsignal.Header.TypeHash]
    if exists == false {
      return nil, fmt.Errorf("0x%x is not a known signal type", qsignal.Header.TypeHash)
    }

    signal, err := sig_info.Load(ctx, qsignal.Data)
    if err != nil {
      return nil, err
    }

    signal_queue[i] = QueuedSignal{qsignal.Header.SignalID, signal, qsignal.Header.Time}
  }

  next_signal, timeout_chan := SoonestSignal(signal_queue)
  node := &Node{
    Key: key,
    ID: key_id,
    Type: node_type.Type,
    Extensions: map[ExtType]Extension{},
    Policies: policies,
    MsgChan: make(chan Message, node_db.Header.BufferSize),
    BufferSize: node_db.Header.BufferSize,
    TimeoutChan: timeout_chan,
    SignalQueue: signal_queue,
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
