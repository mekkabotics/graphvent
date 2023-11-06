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

  badger "github.com/dgraph-io/badger/v3"
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

type Changes map[ExtType][]string

func (changes *Changes) Add(ext ExtType, fields ...string) {
  if *changes == nil {
    *changes = Changes{}
  }
  current, exists := (*changes)[ext]
  if exists == false {
    current = []string{}
  }
  current = append(current, fields...)
  (*changes)[ext] = current
}

// Extensions are data attached to nodes that process signals
type Extension interface {
  Process(*Context, *Node, NodeID, Signal) (Messages, Changes)
}

// A QueuedSignal is a Signal that has been Queued to trigger at a set time
type QueuedSignal struct {
  Signal `gv:"signal"`
  time.Time `gv:"time"`
}

func (q QueuedSignal) String() string {
  return fmt.Sprintf("%+v@%s", reflect.TypeOf(q.Signal), q.Time)
}

type PendingACL struct {
  Counter int
  Responses []ResponseSignal

  TimeoutID uuid.UUID
  Action Tree
  Principal NodeID

  Signal Signal
  Source NodeID
}

type PendingACLSignal struct {
  Policy uuid.UUID
  Timeout uuid.UUID
  ID uuid.UUID
}

// Default message channel size for nodes
// Nodes represent a group of extensions that can be collectively addressed
type Node struct {
  Key ed25519.PrivateKey `gv:"key"`
  ID NodeID
  Type NodeType `gv:"type"`
  // TODO: move each extension to it's own db key, and extend changes to notify which extension was changed
  Extensions map[ExtType]Extension
  Policies []Policy `gv:"policies"`

  PendingACLs map[uuid.UUID]PendingACL `gv:"pending_acls"`
  PendingACLSignals map[uuid.UUID]PendingACLSignal `gv:"pending_signal"`

  // Channel for this node to receive messages from the Context
  MsgChan chan *Message
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

  node.MsgChan = make(chan *Message, node.BufferSize)

  return nil
}

type RuleResult int
const (
  Allow RuleResult = iota
  Deny
  Pending
)

func (node *Node) Allows(ctx *Context, principal_id NodeID, action Tree)(map[uuid.UUID]Messages, RuleResult) {
  pends := map[uuid.UUID]Messages{}
  for _, policy := range(node.Policies) {
    msgs, resp := policy.Allows(ctx, principal_id, action, node)
    if resp == Allow {
      return nil, Allow
    } else if resp == Pending {
      pends[policy.ID()] = msgs
    }
  }
  if len(pends) != 0 {
    return pends, Pending
  }
  return nil, Deny
}

type WaitInfo struct {
  NodeID NodeID `gv:"node"`
  Timeout uuid.UUID `gv:"timeout"`
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

// Creates a timeout signal for signal, queues it for the node at the timeout, and returns the WaitInfo
func (node *Node) QueueTimeout(dest NodeID, signal Signal, timeout time.Duration) WaitInfo {
  timeout_signal := NewTimeoutSignal(signal.ID())
  node.QueueSignal(time.Now().Add(timeout), timeout_signal)

  return WaitInfo{
    NodeID: dest,
    Timeout: timeout_signal.Id,
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
func NewErrorField(fstring string, args ...interface{}) SerializedValue {
  str := StringError(fmt.Sprintf(fstring, args...))
  str_ser, err := str.MarshalBinary()
  if err != nil {
    panic(err)
  }
  return SerializedValue{
    TypeStack: []SerializedType{ErrorType},
    Data: str_ser,
  }
}

func (node *Node) ReadFields(ctx *Context, reqs map[ExtType][]string)map[ExtType]map[string]SerializedValue {
  exts := map[ExtType]map[string]SerializedValue{}
  for ext_type, field_reqs := range(reqs) {
    fields := map[string]SerializedValue{}
    for _, req := range(field_reqs) {
      ext, exists := node.Extensions[ext_type]
      if exists == false {
        fields[req] = NewErrorField("%+v does not have %+v extension", node.ID, ext_type)
      } else {
        f, err := SerializeField(ctx, ext, req)
        if err != nil {
          fields[req] = NewErrorField(err.Error())
        } else {
          fields[req] = f
        }
      }
    }
    exts[ext_type] = fields
  }
  return exts
}

// Main Loop for nodes
func nodeLoop(ctx *Context, node *Node) error {
  started := node.Active.CompareAndSwap(false, true)
  if started == false {
    return fmt.Errorf("%s is already started, will not start again", node.ID)
  }

  // Perform startup actions
  node.Process(ctx, ZeroID, NewStartSignal())
  run := true
  for run == true {
    var signal Signal
    var source NodeID
    select {
    case msg := <- node.MsgChan:
      ctx.Log.Logf("node_msg", "NODE_MSG: %s - %+v", node.ID, msg.Signal)
      signal_ser, err := SerializeAny(ctx, msg.Signal)
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_SERIALIZE_ERR: %s - %+v", err, msg.Signal)
      }
      chunks, err := signal_ser.Chunks()
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_SERIALIZE_ERR: %s - %+v", err, signal_ser)
        continue
      }

      dst_id_ser, err := msg.Dest.MarshalBinary()
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_DEST_ID_SER_ERR: %e", err)
        continue
      }
      src_id_ser, err := KeyID(msg.Source).MarshalBinary()
      if err != nil {
        ctx.Log.Logf("signal", "SIGNAL_SRC_ID_SER_ERR: %e", err)
        continue
      }
      sig_data := append(dst_id_ser, src_id_ser...)
      sig_data = append(sig_data, chunks.Slice()...)
      if msg.Authorization != nil {
        sig_data = append(sig_data, msg.Authorization.Signature...)
      }
      validated := ed25519.Verify(msg.Source, sig_data, msg.Signature)
      if validated == false {
        ctx.Log.Logf("signal_verify", "SIGNAL_VERIFY_ERR: %s - %s", node.ID, reflect.TypeOf(msg.Signal))
        continue
      }

      var princ_id NodeID
      if msg.Authorization == nil {
        princ_id = KeyID(msg.Source)
      } else {
        err := ValidateAuthorization(*msg.Authorization, time.Hour)
        if err != nil {
          ctx.Log.Logf("node", "Authorization validation failed: %s", err)
          continue
        }
        princ_id = KeyID(msg.Authorization.Identity)
      }
      if princ_id != node.ID {
        pends, resp := node.Allows(ctx, princ_id, msg.Signal.Permission())
        if resp == Deny {
          ctx.Log.Logf("policy", "SIGNAL_POLICY_DENY: %s->%s - %+v(%+s)", princ_id, node.ID, reflect.TypeOf(msg.Signal), msg.Signal)
          ctx.Log.Logf("policy", "SIGNAL_POLICY_SOURCE: %s", msg.Source)
          msgs := Messages{}
          msgs = msgs.Add(ctx, KeyID(msg.Source), node, nil, NewErrorSignal(msg.Signal.ID(), "acl denied"))
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
              timeout_signal := NewTimeoutSignal(m.Signal.ID())
              node.QueueSignal(time.Now().Add(time.Second), timeout_signal)
              node.PendingACLSignals[m.Signal.ID()] = PendingACLSignal{policy_type, timeout_signal.Id, msg.Signal.ID()}
            }
          }
          node.PendingACLs[msg.Signal.ID()] = PendingACL{
            Counter: len(msgs),
            TimeoutID: timeout_signal.ID(),
            Action: msg.Signal.Permission(),
            Principal: princ_id,
            Responses: []ResponseSignal{},
            Signal: msg.Signal,
            Source: KeyID(msg.Source),
          }
          ctx.Log.Logf("policy", "Sending signals for pending ACL: %+v", msgs)
          ctx.Send(msgs)
          continue
        } else if resp == Allow {
          ctx.Log.Logf("policy", "SIGNAL_POLICY_ALLOW: %s->%s - %s", princ_id, node.ID, reflect.TypeOf(msg.Signal))
        }
      } else {
        ctx.Log.Logf("policy", "SIGNAL_POLICY_SELF: %s - %s", node.ID, reflect.TypeOf(msg.Signal))
      }

      signal = msg.Signal
      source = KeyID(msg.Source)

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
      node.writeSignalQueue = true

      if node.NextSignal == nil {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL nil@%+v", node.ID, signal, t, node.TimeoutChan)
      } else {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL: %s@%s", node.ID, signal, t, node.NextSignal, node.NextSignal.Time)
      }
    }

    ctx.Log.Logf("node", "NODE_SIGNAL_QUEUE[%s]: %+v", node.ID, node.SignalQueue)

    response, ok := signal.(ResponseSignal)
    if ok == true {
      info, waiting := node.PendingACLSignals[response.ResponseID()]
      if waiting == true {
        delete(node.PendingACLSignals, response.ResponseID())
        ctx.Log.Logf("pending", "FOUND_PENDING_SIGNAL: %s - %s", node.ID, signal)

        req_info, exists := node.PendingACLs[info.ID]
        if exists == true {
          req_info.Counter -= 1
          req_info.Responses = append(req_info.Responses, response)

          idx := -1
          for i, p := range(node.Policies) {
            if p.ID() == info.Policy {
              idx = i
              break
            }
          }
          if idx == -1 {
            ctx.Log.Logf("policy", "PENDING_FOR_NONEXISTENT_POLICY: %s - %s", node.ID, info.Policy)
            delete(node.PendingACLs, info.ID)
          } else {
            allowed := node.Policies[idx].ContinueAllows(ctx, req_info, signal)
            if allowed == Allow {
              ctx.Log.Logf("policy", "DELAYED_POLICY_ALLOW: %s - %s", node.ID, req_info.Signal)
              signal = req_info.Signal
              source = req_info.Source
              err := node.DequeueSignal(req_info.TimeoutID)
              if err != nil {
                ctx.Log.Logf("node", "dequeue error: %s", err)
              }
              delete(node.PendingACLs, info.ID)
            } else if req_info.Counter == 0 {
              ctx.Log.Logf("policy", "DELAYED_POLICY_DENY: %s - %s", node.ID, req_info.Signal)
              // Send the denied response
              msgs := Messages{}
              msgs = msgs.Add(ctx, req_info.Source, node, nil, NewErrorSignal(req_info.Signal.ID(), "acl_denied"))
              err := ctx.Send(msgs)
              if err != nil {
                ctx.Log.Logf("signal", "SEND_ERR: %s", err)
              }
              err = node.DequeueSignal(req_info.TimeoutID)
              if err != nil {
                ctx.Log.Logf("node", "ACL_DEQUEUE_ERROR: timeout signal not in queue when trying to clear after counter hit 0 %s, %s - %s", err, signal.ID(), req_info.TimeoutID)
              }
              delete(node.PendingACLs, info.ID)
            } else {
              node.PendingACLs[info.ID] = req_info
              continue
            }
          }
        }
      }
    }

    switch sig := signal.(type) {
    case *StopSignal:
      node.Process(ctx, source, signal)
      if source == node.ID {
        node.Process(ctx, source, NewStoppedSignal(sig, node.ID))
      } else {
        msgs := Messages{}
        msgs = msgs.Add(ctx, node.ID, node, nil, NewStoppedSignal(sig, node.ID))
        ctx.Send(msgs)
      }
      run = false

    case *ReadSignal:
      result := node.ReadFields(ctx, sig.Extensions)
      msgs := Messages{}
      msgs = msgs.Add(ctx, source, node, nil, NewReadResultSignal(sig.ID(), node.ID, node.Type, result))
      ctx.Send(msgs)

    default:
      err := node.Process(ctx, source, signal)
      if err != nil {
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

func (node *Node) Stop(ctx *Context) error {
  if node.Active.Load() {
    msg, err := NewMessage(ctx, node.ID, node, nil, NewStopSignal())
    if err != nil {
      return err
    }
    node.MsgChan <- msg
    return nil
  } else {
    return fmt.Errorf("Node not active")
  }
}

func (node *Node) QueueChanges(ctx *Context, changes Changes) error {
  node.QueueSignal(time.Now(), NewStatusSignal(node.ID, changes))
  return nil
}

func (node *Node) Process(ctx *Context, source NodeID, signal Signal) error {
  ctx.Log.Logf("node_process", "PROCESSING MESSAGE: %s - %+v", node.ID, signal)
  messages := Messages{}
  changes := Changes{}
  for ext_type, ext := range(node.Extensions) {
    ctx.Log.Logf("node_process", "PROCESSING_EXTENSION: %s/%s", node.ID, ext_type)
    ext_messages, ext_changes := ext.Process(ctx, node, source, signal)
    ctx.Log.Logf("gql", "%s changes %+v", reflect.TypeOf(ext), ext_changes)
    if len(ext_messages) != 0 {
      messages = append(messages, ext_messages...)
    }

    if len(ext_changes) != 0 {
      for ext, change_list := range(ext_changes) {
        changes[ext] = append(changes[ext], change_list...)
      }
    }
  }
  ctx.Log.Logf("gql", "changes after process %+v", changes)

  if len(messages) != 0 {
    send_err := ctx.Send(messages)
    if send_err != nil {
      return send_err
    }
  }

  if len(changes) != 0 {
    _, ok := signal.(*StoppedSignal)
    if (ok == false) || (source != node.ID) {
      write_err := WriteNodeChanges(ctx, node, changes)
      if write_err != nil {
        return write_err
      }

      status_err := node.QueueChanges(ctx, changes)
      if status_err != nil {
        return status_err
      }
    }
  }

  return nil
}

func GetCtx[C any](ctx *Context, ext_type ExtType) (C, error) {
  var zero_ctx C
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

func GetExt[T Extension](node *Node, ext_type ExtType) (T, error) {
  var zero T
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
func NewNode(ctx *Context, key ed25519.PrivateKey, node_type NodeType, buffer_size uint32, policies []Policy, extensions ...Extension) (*Node, error) {
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

  changes := Changes{}
  ext_map := map[ExtType]Extension{}
  for _, ext := range(extensions) {
    ext_type, exists := ctx.ExtensionTypes[reflect.TypeOf(ext)]
    if exists == false {
      return nil, fmt.Errorf(fmt.Sprintf("%+v is not a known Extension", reflect.TypeOf(ext)))
    }
    _, exists = ext_map[ext_type]
    if exists == true {
      return nil, fmt.Errorf("Cannot add the same extension to a node twice")
    }
    ext_map[ext_type] = ext
    changes.Add(ext_type, "init")
  }

  for _, required_ext := range(def.Extensions) {
    _, exists := ext_map[required_ext]
    if exists == false {
      return nil, fmt.Errorf(fmt.Sprintf("%+v requires %+v", node_type, required_ext))
    }
  }

  policies = append(policies, DefaultPolicy)

  node := &Node{
    Key: key,
    ID: id,
    Type: node_type,
    Extensions: ext_map,
    Policies: policies,
    PendingACLs: map[uuid.UUID]PendingACL{},
    PendingACLSignals: map[uuid.UUID]PendingACLSignal{},
    MsgChan: make(chan *Message, buffer_size),
    BufferSize: buffer_size,
    SignalQueue: []QueuedSignal{},
  }

  err = WriteNodeExtList(ctx, node)
  if err != nil {
    return nil, err
  }

  node.writeSignalQueue = true
  err = WriteNodeChanges(ctx, node, changes)
  if err != nil {
    return nil, err
  }

  ctx.AddNode(id, node)

  err = node.Process(ctx, ZeroID, NewCreateSignal())
  if err != nil {
    return nil, err
  }

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
  ext_list := make([]ExtType, len(node.Extensions))
  i := 0
  for ext_type := range(node.Extensions) {
    ext_list[i] = ext_type
    i += 1
  }

  id_bytes, err := node.ID.MarshalBinary()
  if err != nil {
    return err
  }

  ext_list_serialized, err := SerializeAny(ctx, ext_list)
  if err != nil {
    return err
  }

  return ctx.DB.Update(func(txn *badger.Txn) error {
    return txn.Set(append(id_bytes, extension_suffix...), ext_list_serialized.Data)
  })
}

func WriteNodeChanges(ctx *Context, node *Node, changes Changes) error {
  ctx.Log.Logf("db", "Writing changes for %s - %+v", node.ID, changes)

  ext_serialized := map[ExtType]SerializedValue{}
  for ext_type := range(changes) {
    ext, ext_exists := node.Extensions[ext_type]
    if ext_exists == false {
      ctx.Log.Logf("db", "extension 0x%x does not exist for %s", ext_type, node.ID)
    } else {
      serialized_ext, err := SerializeAny(ctx, ext)
      if err != nil {
        return err
      }
      ext_serialized[ext_type] = serialized_ext
      ctx.Log.Logf("db", "extension 0x%x - %+v - %+v", ext_type, serialized_ext.TypeStack, serialized_ext.Data)
    }
  }

  var sq_serialized *SerializedValue = nil
  if node.writeSignalQueue == true {
    node.writeSignalQueue = false
    ser, err := SerializeAny(ctx, node.SignalQueue)
    if err != nil {
      return err
    }
    sq_serialized = &ser
  }

  node_serialized, err := SerializeAny(ctx, node)
  if err != nil {
    return err
  }

  id_bytes, err := node.ID.MarshalBinary()
  return ctx.DB.Update(func(txn *badger.Txn) error {
    err := txn.Set(id_bytes, node_serialized.Data)
    if err != nil {
      return err
    }
    if sq_serialized != nil {
      err := txn.Set(append(id_bytes, signal_queue_suffix...), sq_serialized.Data)
      if err != nil {
        return err
      }
    }
    for ext_type, data := range(ext_serialized) {
      err := txn.Set(append(id_bytes, ExtTypeSuffix(ext_type)...), data.Data)
      if err != nil {
        return err
      }
    }
    return nil
  })
}

func LoadNode(ctx *Context, id NodeID) (*Node, error) {
  ctx.Log.Logf("db", "LOADING_NODE: %s", id)
  var node_bytes []byte = nil
  var sq_bytes []byte = nil
  var ext_bytes = map[ExtType][]byte{}

  err := ctx.DB.View(func(txn *badger.Txn) error {
    id_bytes, err := id.MarshalBinary()
    if err != nil {
      return err
    }

    node_item, err := txn.Get(id_bytes)
    if err != nil {
      ctx.Log.Logf("db", "node key not found")
      return err
    }

    node_bytes, err = node_item.ValueCopy(nil)
    if err != nil {
      return err
    }

    sq_item, err := txn.Get(append(id_bytes, signal_queue_suffix...))
    if err != nil {
      ctx.Log.Logf("db", "sq key not found")
      return err
    }
    sq_bytes, err = sq_item.ValueCopy(nil)
    if err != nil {
      return err
    }

    ext_list_item, err := txn.Get(append(id_bytes, extension_suffix...))
    if err != nil {
      ctx.Log.Logf("db", "ext_list key not found")
      return err
    }

    ext_list_bytes, err := ext_list_item.ValueCopy(nil)
    if err != nil {
      return err
    }

    ext_list_value, remaining, err := DeserializeValue(ctx, reflect.TypeOf([]ExtType{}), ext_list_bytes)
    if err != nil {
      return err
    } else if len(remaining) > 0 {
      return fmt.Errorf("Data remaining after ext_list deserialize %d", len(remaining))
    }
    ext_list, ok := ext_list_value.Interface().([]ExtType)
    if ok == false {
      return fmt.Errorf("deserialize returned wrong type %s", ext_list_value.Type())
    }

    for _, ext_type := range(ext_list) {
      ext_item, err := txn.Get(append(id_bytes, ExtTypeSuffix(ext_type)...))
      if err != nil {
        ctx.Log.Logf("db", "ext %s key not found", ext_type)
        return err
      }

      ext_bytes[ext_type], err = ext_item.ValueCopy(nil)
      if err != nil {
        return err
      }
    }
    return nil
  })
  if err != nil {
    return nil, err
  }

  node_value, remaining, err := DeserializeValue(ctx, reflect.TypeOf((*Node)(nil)), node_bytes)
  if err != nil {
    return nil, err
  } else if len(remaining) != 0 {
    return nil, fmt.Errorf("data left after deserializing node %d", len(remaining))
  }

  node, node_ok := node_value.Interface().(*Node)
  if node_ok == false {
    return nil, fmt.Errorf("node wrong type %s", node_value.Type())
  }

  signal_queue_value, remaining, err := DeserializeValue(ctx, reflect.TypeOf([]QueuedSignal{}), sq_bytes)
  if err != nil {
    return nil, err
  } else if len(remaining) != 0 {
    return nil, fmt.Errorf("data left after deserializing signal_queue %d", len(remaining))
  }

  signal_queue, sq_ok := signal_queue_value.Interface().([]QueuedSignal)
  if sq_ok == false {
    return nil, fmt.Errorf("signal queue wrong type %s", signal_queue_value.Type())
  }

  for ext_type, data := range(ext_bytes) {
    ext_info, exists := ctx.Extensions[ext_type]
    if exists == false {
      return nil, fmt.Errorf("0x%0x is not a known extension type", ext_type)
    }

    ext_value, remaining, err := DeserializeValue(ctx, ext_info.Type, data)
    if err != nil {
      return nil, err
    } else if len(remaining) > 0 {
      return nil, fmt.Errorf("data left after deserializing ext(0x%x) %d", ext_type, len(remaining))
    }
    ext, ext_ok := ext_value.Interface().(Extension)
    if ext_ok == false {
      return nil, fmt.Errorf("extension wrong type %s", ext_value.Type())
    }

    node.Extensions[ext_type] = ext
  }

  node.SignalQueue = signal_queue
  node.NextSignal, node.TimeoutChan = SoonestSignal(signal_queue)

  ctx.AddNode(id, node) 
  ctx.Log.Logf("db", "loaded %s", id)
  go runNode(ctx, node)

  return node, nil
}
