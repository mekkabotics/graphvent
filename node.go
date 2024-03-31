package graphvent

import (
  "crypto/ed25519"
  "crypto/rand"
  "crypto/sha512"
  "fmt"
  "reflect"
  "sync/atomic"
  "time"
  "sync"

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
func (id *NodeID) UnmarshalBinary(data []byte) error {
  return (*uuid.UUID)(id).UnmarshalBinary(data)
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

type WaitMap map[uuid.UUID]NodeID

type Queue[T any] struct {
  out chan T
  in chan T
  buffer []T
  resize sync.Mutex
}

func NewQueue[T any](initial int) *Queue[T] {
  queue := Queue[T]{
    out: make(chan T, 0),
    in: make(chan T, 0),
    buffer: make([]T, 0, initial),
  }

  go func(queue *Queue[T]) {
    if len(queue.buffer) == 0 {
      select {

      }
    } else {
      select {

      }
    }
  }(&queue)

  return &queue
}

func (queue *Queue[T]) Put(value T) error {
  return nil
}

func (queue *Queue[T]) Get(value T) error {
  return nil
}

// Nodes represent a group of extensions that can be collectively addressed
type Node struct {
  Key ed25519.PrivateKey `gv:"key"`
  ID NodeID
  Type NodeType `gv:"type"`

  Extensions map[ExtType]Extension

  // Channel for this node to receive messages from the Context
  MsgChan chan Message
  // Size of MsgChan
  BufferSize uint32 `gv:"buffer_size"`
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  writeSignalQueue bool
  SignalQueue []QueuedSignal
  NextSignal *QueuedSignal
}

func (node *Node) PostDeserialize(ctx *Context) error {
  node.Extensions = map[ExtType]Extension{}

  public := node.Key.Public().(ed25519.PublicKey)
  node.ID = KeyID(public)

  node.MsgChan = make(chan Message, node.BufferSize)

  return nil
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
    if time.Now().Compare(soonest_time) == -1 {
      return soonest_signal, time.After(time.Until(soonest_signal.Time))
    } else {
      c := make(chan time.Time, 1)
      c <- soonest_time
      return soonest_signal, c
    }
  } else {
    return nil, nil
  }
}

func runNode(ctx *Context, node *Node, status chan string, control chan string) {
  ctx.Log.Logf("node", "RUN_START: %s", node.ID)
  err := nodeLoop(ctx, node, status, control)
  if err != nil {
    ctx.Log.Logf("node", "%s runNode err %s", node.ID, err)
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

func (node *Node) ReadFields(ctx *Context, fields []string)map[string]any {
  ctx.Log.Logf("read_field", "Reading %+v on %+v", fields, node.ID)
  values := map[string]any{}

  node_info := ctx.NodeTypes[node.Type]

  for _, field_name := range(fields) {
    field_info, mapped := node_info.Fields[field_name]
    if mapped {
      ext := node.Extensions[field_info.Extension]
      values[field_name] = reflect.ValueOf(ext).Elem().FieldByIndex(field_info.Index).Interface()
    } else {
      values[field_name] = fmt.Errorf("NodeType %s has no field %s", node.Type, field_name)
    }
  }

  return values
}

// Main Loop for nodes
func nodeLoop(ctx *Context, node *Node, status chan string, control chan string) error {
  is_started := node.Active.CompareAndSwap(false, true)
  if is_started == false {
    return fmt.Errorf("%s is already started, will not start again", node.ID)
  } else {
    ctx.Log.Logf("node", "Set %s active", node.ID)
  }

  ctx.Log.Logf("node_ext", "Loading extensions for %s", node.ID)

  for _, extension := range(node.Extensions) {
    ctx.Log.Logf("node_ext", "Loading extension %s for %s", reflect.TypeOf(extension), node.ID)
    err := extension.Load(ctx, node)
    if err != nil {
      ctx.Log.Logf("node_ext", "Failed to load extension %s on node %s", reflect.TypeOf(extension), node.ID)
      node.Active.Store(false)
      return err
    } else {
      ctx.Log.Logf("node_ext", "Loaded extension %s on node %s", reflect.TypeOf(extension), node.ID)
    }
  }

  ctx.Log.Logf("node_ext", "Loaded extensions for %s", node.ID)

  status <- "active"

  running := true
  for running {
    var signal Signal
    var source NodeID

    select {
    case command := <-control:
      switch command {
      case "stop":
        running = false
      case "pause":
        status <- "paused"
        command := <- control
        switch command {
        case "resume":
          status <- "resumed"
        case "stop":
          running = false
        }
      default:
        ctx.Log.Logf("node", "Unknown control command %s", command)
      }
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
    case msg := <- node.MsgChan:
      signal = msg.Signal
      source = msg.Node

    }

    switch sig := signal.(type) {
    case *ReadSignal:
      result := node.ReadFields(ctx, sig.Fields)
      msgs := []Message{}
      msgs = append(msgs, Message{source, NewReadResultSignal(sig.ID(), node.ID, node.Type, result)})
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

  status <- "stopped"

  return nil
}

func (node *Node) Unload(ctx *Context) error {
  for _, extension := range(node.Extensions) {
    extension.Unload(ctx, node)
  }
  return nil
}

func (node *Node) QueueChanges(ctx *Context, changes map[ExtType]Changes) error {
  node_info, exists := ctx.NodeTypes[node.Type]
  if exists == false {
    return fmt.Errorf("Node type not in context, can't map changes to field names")
  } else {
    fields := []string{}
    for ext_type, ext_changes := range(changes) {
      ext_map, ext_mapped := node_info.ReverseFields[ext_type]
      if ext_mapped {
        for _, ext_tag := range(ext_changes) {
          field_name, tag_mapped := ext_map[ext_tag]
          if tag_mapped {
            fields = append(fields, field_name)
          }
        }
      }
    }
    node.QueueSignal(time.Time{}, NewStatusSignal(node.ID, fields))
    return nil
  }
}

func (node *Node) Process(ctx *Context, source NodeID, signal Signal) error {
  messages := []Message{}
  changes := map[ExtType]Changes{}
  for ext_type, ext := range(node.Extensions) {
    ext_messages, ext_changes := ext.Process(ctx, node, source, signal)
    if len(ext_messages) != 0 {
      messages = append(messages, ext_messages...)
    }
    if len(ext_changes) != 0 {
      changes[ext_type] = ext_changes
    }
  }

  if len(messages) != 0 {
    send_err := ctx.Send(node, messages)
    if send_err != nil {
      return send_err
    }
  }

  if len(changes) != 0 {
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
  node_type := NodeTypeFor(type_name)
  node_info, known_type := ctx.NodeTypes[node_type]
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

  ext_map := map[ExtType]Extension{}
  for _, ext := range(extensions) {
    if ext == nil {
      return nil, fmt.Errorf("Cannot create node with nil extension")
    }

    ext_type, exists := ctx.Extensions[ExtTypeOf(reflect.TypeOf(ext))]
    if exists == false {
      return nil, fmt.Errorf("%+v(%+v) is not a known Extension", reflect.TypeOf(ext), ExtTypeOf(reflect.TypeOf(ext)))
    }
    _, exists = ext_map[ext_type.ExtType]
    if exists == true {
      return nil, fmt.Errorf("Cannot add the same extension to a node twice")
    }
    ext_map[ext_type.ExtType] = ext
  }

  for _, required_ext := range(node_info.RequiredExtensions) {
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
    MsgChan: make(chan Message, buffer_size),
    BufferSize: buffer_size,
    SignalQueue: []QueuedSignal{},
    writeSignalQueue: false,
  }

  err = ctx.DB.WriteNodeInit(ctx, node)
  if err != nil {
    return nil, err
  }

  status := make(chan string, 0)
  command := make(chan string, 0)
  go runNode(ctx, node, status, command)

  returned := <- status
  if returned != "active" {
    return nil, fmt.Errorf(returned)
  }

  ctx.AddNode(id, node, status, command)

  return node, nil
}
