package graphvent

import (
  "crypto/ed25519"
  "crypto/rand"
  "crypto/sha512"
  "encoding/binary"
  "fmt"
  "reflect"
  "sync/atomic"
  "time"

  _ "github.com/dgraph-io/badger/v3"
  "github.com/google/uuid"
)

var (
  // Base NodeID, used as a special value
  ZeroUUID = uuid.UUID{}
  ZeroID = NodeID(ZeroUUID)
)

// A NodeID uniquely identifies a Node
type NodeID uuid.UUID
func (id NodeID) MarshalBinary() ([]byte, error) {
  return (uuid.UUID)(id).MarshalBinary()
}
func (id NodeID) String() string {
  return (uuid.UUID)(id).String()
}
func IDFromBytes(bytes []byte) (NodeID, error) {
  id, err := uuid.FromBytes(bytes)
  return NodeID(id), err
}

func (id NodeID) MarshalText() ([]byte, error) {
  return []byte(id.String()), nil
}

func (id *NodeID) UnmarshalText(text []byte) error {
  parsed, err := ParseID(string(text))
  *id = parsed
  return err
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

// A QueuedSignal is a Signal that has been Queued to trigger at a set time
type QueuedSignal struct {
  Signal `gv:"signal"`
  time.Time `gv:"time"`
}

func (q QueuedSignal) String() string {
  return fmt.Sprintf("%+v@%s", reflect.TypeOf(q.Signal), q.Time)
}

// Default message channel size for nodes
// Nodes represent a group of extensions that can be collectively addressed
type Node struct {
  Key ed25519.PrivateKey `gv:"key"`
  ID NodeID
  Type NodeType `gv:"type"`
  // TODO: move each extension to it's own db key, and extend changes to notify which extension was changed
  Extensions map[ExtType]Extension

  // Channel for this node to receive messages from the Context
  MsgChan chan RecvMsg
  // Size of MsgChan
  BufferSize uint32 `gv:"buffer_size"`
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  // TODO: enhance WriteNode to write SignalQueue to a different key, and use writeSignalQueue to decide whether or not to update it
  writeSignalQueue bool
  SignalQueue []QueuedSignal
  NextSignal *QueuedSignal
}

func (node *Node) PostDeserialize(ctx *Context) error {
  node.Extensions = map[ExtType]Extension{}

  public := node.Key.Public().(ed25519.PublicKey)
  node.ID = KeyID(public)

  node.MsgChan = make(chan RecvMsg, node.BufferSize)

  return nil
}

type WaitReason string
type WaitInfo struct {
  Destination NodeID `gv:"destination"`
  Timeout uuid.UUID `gv:"timeout"`
  Reason WaitReason `gv:"reason"`
}

type WaitMap map[uuid.UUID]WaitInfo

// Removes a signal from the wait_map and dequeue the associated timeout signal
// Returns the data, and whether or not the ID was found in the wait_map
func (node *Node) ProcessResponse(wait_map WaitMap, response ResponseSignal) (WaitInfo, bool) {
  wait_info, is_processed := wait_map[response.ResponseID()]
  if is_processed == true {
    delete(wait_map, response.ResponseID())
    if response.ID() != wait_info.Timeout {
      node.DequeueSignal(wait_info.Timeout)
    }
    return wait_info, true
  }
  return WaitInfo{}, false
}

func (node *Node) NewTimeout(reason WaitReason, dest NodeID, timeout time.Duration) (WaitInfo, uuid.UUID) {
  id := uuid.New()

  timeout_signal := NewTimeoutSignal(id)
  node.QueueSignal(time.Now().Add(timeout), timeout_signal)

  return WaitInfo{
    Destination: dest,
    Timeout: timeout_signal.Id,
    Reason: reason,
  }, id
}

// Creates a timeout signal for signal, queues it for the node at the timeout, and returns the WaitInfo
func (node *Node) QueueTimeout(reason WaitReason, dest NodeID, signal Signal, timeout time.Duration) WaitInfo {
  timeout_signal := NewTimeoutSignal(signal.ID())
  node.QueueSignal(time.Now().Add(timeout), timeout_signal)

  return WaitInfo{
    Destination: dest,
    Timeout: timeout_signal.Id,
    Reason: reason,
  }
}

func (node *Node) QueueSignal(time time.Time, signal Signal) {
  node.SignalQueue = append(node.SignalQueue, QueuedSignal{signal, time})
  node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
  node.writeSignalQueue = true
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
  node.writeSignalQueue = true

  return nil
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
    ctx.Log.Logf("node", "%s runNode err %s", node.ID, err)
    panic(err)
  }
  ctx.Log.Logf("node", "RUN_STOP: %s", node.ID)
}

type StringError string
func (err StringError) String() string {
  return string(err)
}
func (err StringError) Error() string {
  return err.String()
}
func (err StringError) MarshalBinary() ([]byte, error) {
  return []byte(string(err)), nil
}

func (node *Node) ReadFields(ctx *Context, reqs map[ExtType][]string)map[ExtType]map[string]any {
  ctx.Log.Logf("read_field", "Reading %+v on %+v", reqs, node.ID)
  exts := map[ExtType]map[string]any{}
  for ext_type, field_reqs := range(reqs) {
    ext_info, ext_known := ctx.Extensions[ext_type]
    if ext_known { 
      fields := map[string]any{}
      for _, req := range(field_reqs) {
        ext, exists := node.Extensions[ext_type]
        if exists == false {
          fields[req] = fmt.Errorf("%+v does not have %+v extension", node.ID, ext_type)
        } else {
          fields[req] = reflect.ValueOf(ext).FieldByIndex(ext_info.Fields[req]).Interface()
        }
      }
      exts[ext_type] = fields
    }
  }
  return exts
}

// Main Loop for nodes
func nodeLoop(ctx *Context, node *Node) error {
  started := node.Active.CompareAndSwap(false, true)
  if started == false {
    return fmt.Errorf("%s is already started, will not start again", node.ID)
  }

  run := true
  for run == true {
    var signal Signal
    var source NodeID
    select {
    case msg := <- node.MsgChan:
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
        ctx.Log.Logf("node", "node.NextSignal not in node.SignalQueue, paniccing")
        panic("node.NextSignal not in node.SignalQueue")
      }
      l := len(node.SignalQueue)
      node.SignalQueue[i] = node.SignalQueue[l-1]
      node.SignalQueue = node.SignalQueue[:(l-1)]

      node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
      node.writeSignalQueue = true

      if node.NextSignal == nil {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL nil@%+v", node.ID, signal, t, node.TimeoutChan)
      } else {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL: %s@%s", node.ID, signal, t, node.NextSignal, node.NextSignal.Time)
      }
    }

    ctx.Log.Logf("node", "NODE_SIGNAL_QUEUE[%s]: %+v", node.ID, node.SignalQueue)

    switch sig := signal.(type) {
    case *ReadSignal:
      result := node.ReadFields(ctx, sig.Extensions)
      msgs := []SendMsg{}
      msgs = append(msgs, SendMsg{source, NewReadResultSignal(sig.ID(), node.ID, node.Type, result)})
      ctx.Send(node, msgs)

    default:
      err := node.Process(ctx, source, signal)
      if err != nil {
        ctx.Log.Logf("node", "%s process error %s", node.ID, err)
        panic(err)
      }
    }
  }

  stopped := node.Active.CompareAndSwap(true, false)
  if stopped == false {
    panic("BAD_STATE: stopping already stopped node")
  }
  return nil
}

func (node *Node) Unload(ctx *Context) error {
  if node.Active.Load() {
    for _, extension := range(node.Extensions) {
      extension.Unload(ctx, node)
    }
    return nil
  } else {
    return fmt.Errorf("Node not active")
  }
}

func (node *Node) QueueChanges(ctx *Context, changes map[ExtType]Changes) error {
  node.QueueSignal(time.Now(), NewStatusSignal(node.ID, changes))
  return nil
}

func (node *Node) Process(ctx *Context, source NodeID, signal Signal) error {
  ctx.Log.Logf("node_process", "PROCESSING MESSAGE: %s - %+v", node.ID, signal)
  messages := []SendMsg{}
  changes := map[ExtType]Changes{}
  for ext_type, ext := range(node.Extensions) {
    ctx.Log.Logf("node_process", "PROCESSING_EXTENSION: %s/%s", node.ID, ext_type)
    ext_messages, ext_changes := ext.Process(ctx, node, source, signal)
    if len(ext_messages) != 0 {
      messages = append(messages, ext_messages...)
    }
    if len(ext_changes) != 0 {
      changes[ext_type] = ext_changes
    }
  }
  ctx.Log.Logf("changes", "Changes for %s after %+v - %+v", node.ID, reflect.TypeOf(signal), changes)

  if len(messages) != 0 {
    send_err := ctx.Send(node, messages)
    if send_err != nil {
      return send_err
    }
  }

  if len(changes) != 0 {
    write_err := WriteNodeChanges(ctx, node, changes)
    if write_err != nil {
      return write_err
    }

    status_err := node.QueueChanges(ctx, changes)
    if status_err != nil {
      return status_err
    }
  }

  return nil
}

func GetCtx[C any, E any, T interface { *E; Extension}](ctx *Context) (C, error) {
  var zero_ctx C
  ext_type := ExtType(SerializedTypeFor[E]())
  ext_info, ok := ctx.Extensions[ext_type]
  if ok == false {
    return zero_ctx, fmt.Errorf("%+v is not an extension in ctx", ext_type)
  }

  ext_ctx, ok := ext_info.Data.(C)
  if ok == false {
    return zero_ctx, fmt.Errorf("context for %+v is %+v, not %+v", ext_type, reflect.TypeOf(ext_info.Data), reflect.TypeOf(zero_ctx))
  }

  return ext_ctx, nil
}

func GetExt[E any, T interface { *E; Extension}](node *Node) (T, error) {
  var zero T
  ext_type := ExtType(SerializedTypeFor[E]())
  ext, exists := node.Extensions[ext_type]
  if exists == false {
    return zero, fmt.Errorf("%+v does not have %+v extension - %+v", node.ID, ext_type, node.Extensions)
  }

  ret, ok := ext.(T)
  if ok == false {
    return zero, fmt.Errorf("%+v in %+v is wrong type(%+v), expecting %+v", ext_type, node.ID, reflect.TypeOf(ext), reflect.TypeOf(zero))
  }

  return ret, nil
}

func KeyID(pub ed25519.PublicKey) NodeID {
  id := uuid.NewHash(sha512.New(), ZeroUUID, pub, 3)
  return NodeID(id)
}

// Create a new node in memory and start it's event loop
func NewNode(ctx *Context, key ed25519.PrivateKey, type_name string, buffer_size uint32, extensions ...Extension) (*Node, error) {
  node_type, known_type := ctx.NodeTypes[type_name]
  if known_type == false {
    return nil, fmt.Errorf("%s is not a known node type", type_name)
  }

  var err error
  var public ed25519.PublicKey
  if key == nil {
    public, key, err = ed25519.GenerateKey(rand.Reader)
    if err != nil {
      return nil, err
    }
  } else {
    public = key.Public().(ed25519.PublicKey)
  }
  id := KeyID(public)
  _, exists := ctx.Node(id)
  if exists == true {
    return nil, fmt.Errorf("Attempted to create an existing node")
  }

  def, exists := ctx.Nodes[node_type]
  if exists == false {
    return nil, fmt.Errorf("Node type %+v not registered in Context", node_type)
  }

  ext_map := map[ExtType]Extension{}
  for _, ext := range(extensions) {
    ext_type, exists := ctx.ExtensionTypes[reflect.TypeOf(ext).Elem()]
    if exists == false {
      return nil, fmt.Errorf(fmt.Sprintf("%+v is not a known Extension", reflect.TypeOf(ext)))
    }
    _, exists = ext_map[ext_type]
    if exists == true {
      return nil, fmt.Errorf("Cannot add the same extension to a node twice")
    }
    ext_map[ext_type] = ext
  }

  for _, required_ext := range(def.Extensions) {
    _, exists := ext_map[required_ext]
    if exists == false {
      return nil, fmt.Errorf(fmt.Sprintf("%+v requires %+v", node_type, required_ext))
    }
  }

  node := &Node{
    Key: key,
    ID: id,
    Type: node_type,
    Extensions: ext_map,
    MsgChan: make(chan RecvMsg, buffer_size),
    BufferSize: buffer_size,
    SignalQueue: []QueuedSignal{},
  }

  err = WriteNodeExtList(ctx, node)
  if err != nil {
    return nil, err
  }

  node.writeSignalQueue = true
  err = WriteNodeInit(ctx, node)
  if err != nil {
    return nil, err
  }

  // Load each extension before starting the main loop
  for _, extension := range(node.Extensions) {
    err := extension.Load(ctx, node)
    if err != nil {
      return nil, err
    }
  }

  ctx.AddNode(id, node)
  go runNode(ctx, node)

  return node, nil
}

var extension_suffix = []byte{0xEE, 0xFF, 0xEE, 0xFF}
var signal_queue_suffix = []byte{0xAB, 0xBA, 0xAB, 0xBA}
func ExtTypeSuffix(ext_type ExtType) []byte {
  ret := make([]byte, 12)
  copy(ret[0:4], extension_suffix)
  binary.BigEndian.PutUint64(ret[4:], uint64(ext_type))
  return ret
}

func WriteNodeExtList(ctx *Context, node *Node) error {
  ctx.Log.Logf("todo", "write node list")
  return nil
}

func WriteNodeInit(ctx *Context, node *Node) error {
  ctx.Log.Logf("todo", "write initial node entry")
  return nil
}

func WriteNodeChanges(ctx *Context, node *Node, changes map[ExtType]Changes) error {
  ctx.Log.Logf("todo", "write node changes")
  return nil
}

func LoadNode(ctx *Context, id NodeID) (*Node, error) {
  return nil, fmt.Errorf("TODO: load node + extensions from DB")
}
