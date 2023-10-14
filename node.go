package graphvent

import (
  "time"
  "errors"
  "reflect"
  "github.com/google/uuid"
  badger "github.com/dgraph-io/badger/v3"
  "fmt"
  "sync/atomic"
  "crypto"
  "crypto/ed25519"
  "crypto/sha512"
  "crypto/rand"
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

type Changes []string

func (changes Changes) Add(detail string) Changes {
  return append(changes, detail)
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

type PendingSignal struct {
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
  Extensions map[ExtType]Extension `gv:"extensions"`
  Policies []Policy `gv:"policies"`

  PendingACLs map[uuid.UUID]PendingACL `gv:"pending_acls"`
  PendingSignals map[uuid.UUID]PendingSignal `gv:"pending_signal"`

  // Channel for this node to receive messages from the Context
  MsgChan chan *Message
  // Size of MsgChan
  BufferSize uint32 `gv:"buffer_size"`
  // Channel for this node to process delayed signals
  TimeoutChan <-chan time.Time

  Active atomic.Bool

  SignalQueue []QueuedSignal `gv:"signal_queue"`
  NextSignal *QueuedSignal
}

func (node *Node) PostDeserialize(ctx *Context) error {
  public := node.Key.Public().(ed25519.PublicKey)
  node.ID = KeyID(public)

  node.MsgChan = make(chan *Message, node.BufferSize)

  node.NextSignal, node.TimeoutChan = SoonestSignal(node.SignalQueue)
  ctx.Log.Logf("node", "signal_queue: %+v", node.SignalQueue)
  ctx.Log.Logf("node", "next_signal: %+v - %+v", node.NextSignal, node.TimeoutChan)

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
  err := WriteNode(ctx, node)
  if err != nil {
    panic(err)
  }
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
      ser, err := signal_ser.MarshalBinary()
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
      sig_data = append(sig_data, ser...)
      validated := ed25519.Verify(msg.Source, sig_data, msg.Signature)
      if validated == false {
        ctx.Log.Logf("signal", "SIGNAL_VERIFY_ERR: %s - %+v", node.ID, msg)
        continue
      }

      princ_id := KeyID(msg.Source)
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
              node.PendingSignals[m.Signal.ID()] = PendingSignal{policy_type, timeout_signal.Id, msg.Signal.ID()}
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
      if node.NextSignal == nil {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL nil@%+v", node.ID, signal, t, node.TimeoutChan)
      } else {
        ctx.Log.Logf("node", "NODE_TIMEOUT(%s) - PROCESSING %+v@%s - NEXT_SIGNAL: %s@%s", node.ID, signal, t, node.NextSignal, node.NextSignal.Time)
      }
    }

    ctx.Log.Logf("node", "NODE_SIGNAL_QUEUE[%s]: %+v", node.ID, node.SignalQueue)

    response, ok := signal.(ResponseSignal)
    if ok == true {
      info, waiting := node.PendingSignals[response.ResponseID()]
      if waiting == true {
        delete(node.PendingSignals, response.ResponseID())
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

type AuthInfo struct {
  // The Node that issued the authorization
  Identity ed25519.PublicKey

  // Time the authorization was generated
  Start time.Time

  // Signature of Start + Principal with Identity private key
  Signature []byte
}

type AuthorizationToken struct {
  AuthInfo

  // The private key generated by the client, encrypted with the servers public key
  KeyEncrypted []byte
}

type ClientAuthorization struct {
  AuthInfo

  // The private key generated by the client
  Key ed25519.PrivateKey
}

// Authorization structs can be passed in a message that originated from a different node than the sender
type Authorization struct {
  AuthInfo

  // The public key generated for this authorization
  Key ed25519.PublicKey
}

type Message struct {
  Dest NodeID
  Source ed25519.PublicKey

  Authorization *Authorization

  Signal Signal
  Signature []byte
}

type Messages []*Message
func (msgs Messages) Add(ctx *Context, dest NodeID, source *Node, authorization *ClientAuthorization, signal Signal) Messages {
  msg, err := NewMessage(ctx, dest, source, authorization, signal)
  if err != nil {
    panic(err)
  } else {
    msgs = append(msgs, msg)
  }
  return msgs
}

func NewMessage(ctx *Context, dest NodeID, source *Node, authorization *ClientAuthorization, signal Signal) (*Message, error) {
  signal_ser, err := SerializeAny(ctx, signal)
  if err != nil {
    return nil, err
  }

  ser, err := signal_ser.MarshalBinary()
  if err != nil {
    return nil, err
  }

  dest_ser, err := dest.MarshalBinary()
  if err != nil {
    return nil, err
  }
  source_ser, err := source.ID.MarshalBinary()
  if err != nil {
    return nil, err
  }
  sig_data := append(dest_ser, source_ser...)
  sig_data = append(sig_data, ser...)
  var message_auth *Authorization = nil
  if authorization != nil {
    sig_data = append(sig_data, authorization.Signature...)
    message_auth = &Authorization{
      authorization.AuthInfo,
      authorization.Key.Public().(ed25519.PublicKey),
    }
  }

  sig, err := source.Key.Sign(rand.Reader, sig_data, crypto.Hash(0))
  if err != nil {
    return nil, err
  }

  return &Message{
    Dest: dest,
    Source: source.Key.Public().(ed25519.PublicKey),
    Authorization: message_auth,
    Signal: signal,
    Signature: sig,
  }, nil
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
    if len(ext_messages) != 0 {
      messages = append(messages, ext_messages...)
    }

    if len(ext_changes) != 0 {
      changes = append(changes, ext_changes...)
    }
  }

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
    PendingSignals: map[uuid.UUID]PendingSignal{},
    MsgChan: make(chan *Message, buffer_size),
    BufferSize: buffer_size,
    SignalQueue: []QueuedSignal{},
  }
  ctx.AddNode(id, node)

  err = node.Process(ctx, ZeroID, NewCreateSignal())
  if err != nil {
    return nil, err
  }

  err = WriteNode(ctx, node)
  if err != nil {
    return nil, err
  }

  go runNode(ctx, node)

  return node, nil
}

func WriteNodeChanges(ctx *Context, node *Node, changes Changes) error {
  // TODO: optimize to not re-serialize unchanged extensions/fields(might need to cache the serialized values)
  return WriteNode(ctx, node)
}

// Write a node to the database
func WriteNode(ctx *Context, node *Node) error {
  ctx.Log.Logf("db", "DB_WRITE: %s", node.ID)

  node_serialized, err := SerializeAny(ctx, node)
  if err != nil {
    return err
  }
  bytes, err := node_serialized.MarshalBinary()
  if err != nil {
    return err
  }

  ctx.Log.Logf("db_data", "DB_DATA: %+v", bytes)

  id_bytes, err := node.ID.MarshalBinary()
  if err != nil {
    return err
  }
  ctx.Log.Logf("db", "DB_WRITE_ID: %+v", id_bytes)

  return ctx.DB.Update(func(txn *badger.Txn) error {
    return txn.Set(id_bytes, bytes)
  })
}

func LoadNode(ctx * Context, id NodeID) (*Node, error) {
  ctx.Log.Logf("db", "LOADING_NODE: %s", id)
  var bytes []byte
  err := ctx.DB.View(func(txn *badger.Txn) error {
    id_bytes, err := id.MarshalBinary()
    if err != nil {
      return err
    }
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

  value, remaining, err := ParseSerializedValue(bytes)
  if err != nil {
    return nil, err
  } else if len(remaining) != 0 {
    return nil, fmt.Errorf("%d bytes left after parsing node from DB", len(remaining))
  }
  _, node_val, remaining_data, err := DeserializeValue(ctx, value)
  if err != nil {
    return nil, err
  }

  if len(remaining_data.TypeStack) != 0 {
    return nil, fmt.Errorf("%d entries left in typestack after deserializing *Node", len(remaining_data.TypeStack))
  }
  if len(remaining_data.Data) != 0 {
    return nil, fmt.Errorf("%d bytes left after desrializing *Node", len(remaining_data.Data))
  }

  node, ok := node_val.Interface().(*Node)
  if ok == false {
    return nil, fmt.Errorf("Deserialized %+v when expecting *Node", node_val.Type())
  }

  for ext_type, ext := range(node.Extensions){
    ctx.Log.Logf("serialize", "Deserialized extension: %+v - %+v", ext_type, ext)
  }

  ctx.AddNode(id, node) 
  ctx.Log.Logf("db", "DB_NODE_LOADED: %s", id)
  go runNode(ctx, node)

  return node, nil
}
