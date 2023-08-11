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
  "crypto"
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
  QSIGNAL_DB_HEADER_LEN = 24
  POLICY_DB_HEADER_LEN = 16
)

var (
  // Base NodeID, used as a special value
  ZeroUUID = uuid.UUID{}
  ZeroID = NodeID(ZeroUUID)
)

// A NodeID uniquely identifies a Node
type NodeID uuid.UUID

func (id NodeID) MarshalText() ([]byte, error) {
  return json.Marshal(id.String())
}

func (id *NodeID) UnmarshalText(data []byte) error {
  return json.Unmarshal(data, id)
}

func (id *NodeID) MarshalJSON() ([]byte, error) {
  return json.Marshal(id.String())
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
  Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages
}

// A QueuedSignal is a Signal that has been Queued to trigger at a set time
type QueuedSignal struct {
  Signal
  time.Time
}

type PendingACL struct {
  Counter int
  TimeoutID uuid.UUID
  Action Tree
  Principal NodeID
  Messages Messages
  Responses []Signal
  Signal Signal
  Source NodeID
}

type PendingSignal struct {
  Policy PolicyType
  Found bool
  ID uuid.UUID
}

// Default message channel size for nodes
// Nodes represent a group of extensions that can be collectively addressed
type Node struct {
  Key ed25519.PrivateKey
  ID NodeID
  Type NodeType
  Extensions map[ExtType]Extension
  Policies map[PolicyType]Policy

  PendingACLs map[uuid.UUID]PendingACL
  PendingSignals map[uuid.UUID]PendingSignal

  // Channel for this node to receive messages from the Context
  MsgChan chan *Message
  // Size of MsgChan
  BufferSize uint32
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  SignalQueue []QueuedSignal
  NextSignal *QueuedSignal
}

type RuleResult int
const (
  Allow RuleResult = iota
  Deny
  Pending
)

func (node *Node) Allows(principal_id NodeID, action Tree)(map[PolicyType]Messages, RuleResult) {
  pends := map[PolicyType]Messages{}
  for policy_type, policy := range(node.Policies) {
    msgs, resp := policy.Allows(principal_id, action, node)
    if resp == Allow {
      return nil, Allow
    } else if resp == Pending {
      pends[policy_type] = msgs
    }
  }
  if len(pends) != 0 {
    return pends, Pending
  }
  return nil, Deny
}

func (node *Node) QueueSignal(time time.Time, signal Signal) {
  node.SignalQueue = append(node.SignalQueue, QueuedSignal{signal, time})
  node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
}

func (node *Node) DequeueSignal(id uuid.UUID) error {
  idx := -1
  for i, q := range(node.SignalQueue) {
    if q.Signal.ID() == id {
      idx = i
      break
    }
  }
  if idx == -1 {
    return fmt.Errorf("%s is not in SignalQueue", id)
  }

  node.SignalQueue[idx] = node.SignalQueue[len(node.SignalQueue)-1]
  node.SignalQueue = node.SignalQueue[:len(node.SignalQueue)-1]
  node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)

  return nil
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

func (node *Node) ReadFields(reqs map[ExtType][]string)map[ExtType]map[string]interface{} {
  exts := map[ExtType]map[string]interface{}{}
  for ext_type, field_reqs := range(reqs) {
    fields := map[string]interface{}{}
    for _, req := range(field_reqs) {
      ext, exists := node.Extensions[ext_type]
      if exists == false {
        fields[req] = fmt.Errorf("%s does not have %s extension", node.ID, ext_type)
      } else {
        fields[req] = ext.Field(req)
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
  node.Process(ctx, ZeroID, &StartSignal)

  for true {
    var signal Signal
    var source NodeID
    select {
    case msg := <- node.MsgChan:
      ctx.Log.Logf("node_msg", "NODE_MSG: %s - %+v", node.ID, msg.Signal)
      ser, err := msg.Signal.Serialize()
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_SERIALIZE_ERR: %s - %+v", node.ID, msg.Signal)
        continue
      }

      sig_data := append(msg.Dest.Serialize(), msg.Source.Serialize()...)
      sig_data = append(sig_data, ser...)
      validated := ed25519.Verify(msg.Principal, sig_data, msg.Signature)
      if validated == false {
        ctx.Log.Logf("signal", "SIGNAL_VERIFY_ERR: %s - %+v", node.ID, msg)
        continue
      }

      princ_id := KeyID(msg.Principal)
      if princ_id != node.ID {
        pends, resp := node.Allows(princ_id, msg.Signal.Permission())
        if resp == Deny {
          ctx.Log.Logf("policy", "SIGNAL_POLICY_DENY: %s->%s - %s", princ_id, node.ID, msg.Signal.Permission())
          msgs := Messages{}
          msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(msg.Signal.ID(), "acl denied"), source)
          ctx.Send(msgs)
          continue
        } else if resp == Pending {
          ctx.Log.Logf("policy", "SIGNAL_POLICY_PENDING: %s->%s - %s - %+v", princ_id, node.ID, msg.Signal.Permission(), pends)
          timeout_signal := NewACLTimeoutSignal(msg.Signal.ID())
          node.QueueSignal(time.Now().Add(100*time.Millisecond), timeout_signal)
          msgs := Messages{}
          for policy_type, sigs := range(pends) {
            for _, m := range(sigs) {
              msgs = append(msgs, m)
              node.PendingSignals[m.Signal.ID()] = PendingSignal{policy_type, false, msg.Signal.ID()}
            }
          }
          node.PendingACLs[msg.Signal.ID()] = PendingACL{len(msgs), timeout_signal.ID(), msg.Signal.Permission(), princ_id, msgs, []Signal{}, msg.Signal, msg.Source}
          ctx.Send(msgs)
          continue
        } else if resp == Allow {
          ctx.Log.Logf("policy", "SIGNAL_POLICY_ALLOW: %s->%s - %s", princ_id, node.ID, msg.Signal.Permission())
        }
      } else {
        ctx.Log.Logf("policy", "SIGNAL_POLICY_SELF: %s - %s", node.ID, msg.Signal.Permission())
      }

      signal = msg.Signal
      source = msg.Source

    case <-node.TimeoutChan:
      signal = node.NextSignal.Signal
      source = node.ID

      t := node.NextSignal.Time
      i := -1
      for j, queued := range(node.SignalQueue) {
        if queued.Signal.ID() == node.NextSignal.Signal.ID() {
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
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %s@%s - NEXT_SIGNAL nil@%+v", node.ID, signal.Type(), t, node.TimeoutChan)
      } else {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %s@%s - NEXT_SIGNAL: %s@%s", node.ID, signal.Type(), t, node.NextSignal, node.NextSignal.Time)
      }
    }

    ctx.Log.Logf("node", "NODE_SIGNAL_QUEUE[%s]: %+v", node.ID, node.SignalQueue)

    info, waiting := node.PendingSignals[signal.ReqID()]
    if waiting == true {
      if info.Found == false {
        info.Found = true
        node.PendingSignals[signal.ReqID()] = info
        ctx.Log.Logf("pending", "FOUND_PENDING_SIGNAL: %s - %s", node.ID, signal)
        req_info, exists := node.PendingACLs[info.ID]
        if exists == true {
          req_info.Counter -= 1
          req_info.Responses = append(req_info.Responses, signal)

          // TODO: call the right policy ParseResponse to check if the updated state passes the ACL check
          allowed := node.Policies[info.Policy].ContinueAllows(req_info, signal)
          if allowed == Allow {
            ctx.Log.Logf("policy", "DELAYED_POLICY_ALLOW: %s - %s", node.ID, req_info.Signal)
            signal = req_info.Signal
            source = req_info.Source
            err := node.DequeueSignal(req_info.TimeoutID)
            if err != nil {
              panic("dequeued a passed signal")
            }
            delete(node.PendingACLs, info.ID)
          } else if req_info.Counter == 0 {
            ctx.Log.Logf("policy", "DELAYED_POLICY_DENY: %s - %s", node.ID, req_info.Signal)
            // Send the denied response
            msgs := Messages{}
            msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(req_info.Signal.ID(), "ACL_DENIED"), req_info.Source)
            err := ctx.Send(msgs)
            if err != nil {
              ctx.Log.Logf("signal", "SEND_ERR: %s", err)
            }
            err = node.DequeueSignal(req_info.TimeoutID)
            if err != nil {
              panic("dequeued a passed signal")
            }
            delete(node.PendingACLs, info.ID)
          } else {
            node.PendingACLs[info.ID] = req_info
            continue
          }
        }
      }
    }

    // Handle special signal types
    if signal.Type() == StopSignalType {
      msgs := Messages{}
      msgs = msgs.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "stopped"), source)
      ctx.Send(msgs)
      node.Process(ctx, node.ID, NewStatusSignal("stopped", node.ID))
      break
    } else if signal.Type() == ReadSignalType {
      read_signal, ok := signal.(*ReadSignal)
      if ok == false {
        ctx.Log.Logf("signal", "READ_SIGNAL: bad cast %+v", signal)
      } else {
        result := node.ReadFields(read_signal.Extensions)
        msgs := Messages{}
        msgs = msgs.Add(node.ID, node.Key, NewReadResultSignal(read_signal.ID(), node.ID, node.Type, result), source)
        ctx.Send(msgs)
      }
    }

    node.Process(ctx, source, signal)
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
  Source NodeID
  Dest NodeID
  Principal ed25519.PublicKey
  Signal Signal
  Signature []byte
}

type Messages []*Message
func (msgs Messages) Add(source NodeID, principal ed25519.PrivateKey, signal Signal, dest NodeID) Messages {
  msg, err := NewMessage(dest, source, principal, signal)
  if err != nil {
    panic(err)
  } else {
    msgs = append(msgs, msg)
  }
  return msgs
}

func NewMessage(dest NodeID, source NodeID, principal ed25519.PrivateKey, signal Signal) (*Message, error) {
  ser, err := signal.Serialize()
  if err != nil {
    return nil, err
  }

  sig_data := append(dest.Serialize(), source.Serialize()...)
  sig_data = append(sig_data, ser...)

  sig, err := principal.Sign(rand.Reader, sig_data, crypto.Hash(0))
  if err != nil {
    return nil, err
  }

  return &Message{
    Dest: dest,
    Source: source,
    Principal: principal.Public().(ed25519.PublicKey),
    Signal: signal,
    Signature: sig,
  }, nil
}

func (node *Node) Process(ctx *Context, source NodeID, signal Signal) error {
  ctx.Log.Logf("node_process", "PROCESSING MESSAGE: %s - %+v", node.ID, signal.Type())
  messages := Messages{}
  for ext_type, ext := range(node.Extensions) {
    ctx.Log.Logf("node_process", "PROCESSING_EXTENSION: %s/%s", node.ID, ext_type)
    //TODO: add extension and node info to log
    resp := ext.Process(ctx, node, source, signal)
    if resp != nil {
      messages = append(messages, resp...)
    }
  }

  return ctx.Send(messages)
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
    Policies: policies,
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
    //TODO serialize/deserialize these
    PendingACLs: map[uuid.UUID]PendingACL{},
    PendingSignals: map[uuid.UUID]PendingSignal{},
    MsgChan: make(chan *Message, buffer_size),
    BufferSize: buffer_size,
    SignalQueue: []QueuedSignal{},
  }
  ctx.AddNode(id, node)
  err = WriteNode(ctx, node)
  if err != nil {
    panic(err)
  }

  node.Process(ctx, ZeroID, &NewSignal)

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
    unix_milli := binary.BigEndian.Uint64(cur[0:8])
    type_hash := binary.BigEndian.Uint64(cur[8:16])
    signal_size := binary.BigEndian.Uint64(cur[16:24])

    signal_data := cur[QSIGNAL_DB_HEADER_LEN:(QSIGNAL_DB_HEADER_LEN+signal_size)]

    queued_signals[i] = QSignalDB{
      QSignalDBHeader{
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
  binary.BigEndian.PutUint64(ret[0:8], uint64(header.Time.UnixMilli()))
  binary.BigEndian.PutUint64(ret[8:16], header.TypeHash)
  binary.BigEndian.PutUint64(ret[16:24], header.Length)
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

    signal_queue[i] = QueuedSignal{signal, qsignal.Header.Time}
  }

  next_signal, timeout_chan := SoonestSignal(signal_queue)
  node := &Node{
    Key: key,
    ID: key_id,
    Type: node_type.Type,
    Extensions: map[ExtType]Extension{},
    Policies: policies,
    MsgChan: make(chan *Message, node_db.Header.BufferSize),
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
