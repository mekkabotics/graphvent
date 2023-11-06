package graphvent

import (
  "crypto/ecdh"
  "errors"
  "fmt"
  "reflect"
  "runtime"
  "sync"
  "time"
  "github.com/google/uuid"

  badger "github.com/dgraph-io/badger/v3"
)

var (
  NodeNotFoundError = errors.New("Node not found in DB")
  ECDH = ecdh.X25519()
)

type ExtensionInfo struct {
  Type reflect.Type
  Data interface{}
}

type NodeInfo struct {
  Extensions []ExtType
}

type TypeInfo struct {
  Reflect reflect.Type
  Type SerializedType
  TypeSerialize TypeSerializeFn
  Serialize SerializeFn
  TypeDeserialize TypeDeserializeFn
  Deserialize DeserializeFn
}

type KindInfo struct {
  Reflect reflect.Kind
  Base reflect.Type
  Type SerializedType
  TypeSerialize TypeSerializeFn
  Serialize SerializeFn
  TypeDeserialize TypeDeserializeFn
  Deserialize DeserializeFn
}

// A Context stores all the data to run a graphvent process
type Context struct {
  // DB is the database connection used to load and write nodes
  DB * badger.DB
  // Logging interface
  Log Logger
  // Map between database extension hashes and the registered info
  Extensions map[ExtType]ExtensionInfo
  ExtensionTypes map[reflect.Type]ExtType
  // Map between databse policy hashes and the registered info
  Policies map[PolicyType]reflect.Type
  PolicyTypes map[reflect.Type]PolicyType
  // Map between serialized signal hashes and the registered info
  Signals map[SignalType]reflect.Type
  SignalTypes map[reflect.Type]SignalType
  // Map between database type hashes and the registered info
  Nodes map[NodeType]NodeInfo
  // Map between go types and registered info
  Types map[SerializedType]*TypeInfo
  TypeReflects map[reflect.Type]*TypeInfo

  Kinds map[reflect.Kind]*KindInfo
  KindTypes map[SerializedType]*KindInfo

  // Routing map to all the nodes local to this context
  nodeMapLock sync.RWMutex
  nodeMap map[NodeID]*Node
}

// Register a NodeType to the context, with the list of extensions it requires
func (ctx *Context) RegisterNodeType(node_type NodeType, extensions []ExtType) error {
  _, exists := ctx.Nodes[node_type]
  if exists == true {
    return fmt.Errorf("Cannot register node type %+v, type already exists in context", node_type)
  }

  ext_found := map[ExtType]bool{}
  for _, extension := range(extensions) {
    _, in_ctx := ctx.Extensions[extension]
    if in_ctx == false {
      return fmt.Errorf("Cannot register node type %+v, required extension %+v not in context", node_type, extension)
    }

    _, duplicate := ext_found[extension]
    if duplicate == true {
      return fmt.Errorf("Duplicate extension %+v found in extension list", extension)
    }

    ext_found[extension] = true
  }

  ctx.Nodes[node_type] = NodeInfo{
    Extensions: extensions,
  }
  return nil
}

func (ctx *Context) RegisterPolicy(reflect_type reflect.Type, policy_type PolicyType) error {
  _, exists := ctx.Policies[policy_type]
  if exists == true {
    return fmt.Errorf("Cannot register policy of type %+v, type already exists in context", policy_type)
  }

  policy_info, err := GetStructInfo(ctx, reflect_type)
  if err != nil {
    return err
  }

  err = ctx.RegisterType(reflect_type, SerializedType(policy_type), nil, SerializeStruct(policy_info), nil, DeserializeStruct(policy_info))
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered PolicyType: %+v - %+v", reflect_type, policy_type)

  ctx.Policies[policy_type] = reflect_type
  ctx.PolicyTypes[reflect_type] = policy_type
  return nil
}

func (ctx *Context)RegisterSignal(reflect_type reflect.Type, signal_type SignalType) error {
  _, exists := ctx.Signals[signal_type]
  if exists == true {
    return fmt.Errorf("Cannot register signal of type %+v, type already exists in context", signal_type)
  }

  signal_info, err := GetStructInfo(ctx, reflect_type)
  if err != nil {
    return err
  }

  err = ctx.RegisterType(reflect_type, SerializedType(signal_type), nil, SerializeStruct(signal_info), nil, DeserializeStruct(signal_info))
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered SignalType: %+v - %+v", reflect_type, signal_type)

  ctx.Signals[signal_type] = reflect_type
  ctx.SignalTypes[reflect_type] = signal_type
  return nil
}

func (ctx *Context)RegisterExtension(reflect_type reflect.Type, ext_type ExtType, data interface{}) error {
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension of type %+v, type already exists in context", ext_type)
  }

  elem_type := reflect_type.Elem()
  elem_info, err := GetStructInfo(ctx, elem_type)
  if err != nil {
    return err
  }

  err = ctx.RegisterType(elem_type, SerializedType(ext_type), nil, SerializeStruct(elem_info), nil, DeserializeStruct(elem_info))
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered ExtType: %+v - %+v", reflect_type, ext_type)

  ctx.Extensions[ext_type] = ExtensionInfo{
    Type: reflect_type,
    Data: data,
  }
  ctx.ExtensionTypes[reflect_type] = ext_type

  return nil
}

func (ctx *Context)RegisterKind(kind reflect.Kind, base reflect.Type, ctx_type SerializedType, type_serialize TypeSerializeFn, serialize SerializeFn, type_deserialize TypeDeserializeFn, deserialize DeserializeFn) error {
  _, exists := ctx.Kinds[kind]
  if exists == true {
    return fmt.Errorf("Cannot register kind %+v, kind already exists in context", kind)
  }
  _, exists = ctx.KindTypes[ctx_type]
  if exists == true {
    return fmt.Errorf("0x%x is already registered, cannot use for %+v", ctx_type, kind)
  }
  if deserialize == nil {
    return fmt.Errorf("Cannot register field without deserialize function")
  }
  if serialize == nil {
    return fmt.Errorf("Cannot register field without serialize function")
  }

  info := KindInfo{
    Reflect: kind,
    Base: base,
    Type: ctx_type,
    TypeSerialize: type_serialize,
    Serialize: serialize,
    TypeDeserialize: type_deserialize,
    Deserialize: deserialize,
  }
  ctx.KindTypes[ctx_type] = &info
  ctx.Kinds[kind] = &info

  ctx.Log.Logf("serialize_types", "Registered kind %+v, %+v", kind, ctx_type)

  return nil
}

func (ctx *Context)RegisterType(reflect_type reflect.Type, ctx_type SerializedType, type_serialize TypeSerializeFn, serialize SerializeFn, type_deserialize TypeDeserializeFn, deserialize DeserializeFn) error {
  _, exists := ctx.Types[ctx_type]
  if exists == true {
    return fmt.Errorf("Cannot register field of type %+v, type already exists in context", ctx_type)
  }
  _, exists = ctx.TypeReflects[reflect_type]
  if exists == true {
    return fmt.Errorf("Cannot register field with type %+v, type already registered in context", reflect_type)
  }

  if type_serialize == nil || type_deserialize == nil {
    kind_info, kind_registered := ctx.Kinds[reflect_type.Kind()]
    if kind_registered == true {
      if type_serialize == nil {
        type_serialize = kind_info.TypeSerialize
      }
      if type_deserialize == nil {
        type_deserialize = kind_info.TypeDeserialize
      }
    }
  }

  if serialize == nil || deserialize == nil {
    kind_info, kind_registered := ctx.Kinds[reflect_type.Kind()]
    if kind_registered == false {
      return fmt.Errorf("No serialize/deserialize passed and none registered for kind %+v", reflect_type.Kind())
    } else {
      if serialize == nil {
        serialize = kind_info.Serialize
      }
      if deserialize == nil {
        deserialize = kind_info.Deserialize
      }
    }
  }

  type_info := TypeInfo{
    Reflect: reflect_type,
    Type: ctx_type,
    TypeSerialize: type_serialize,
    Serialize: serialize,
    TypeDeserialize: type_deserialize,
    Deserialize: deserialize,
  }

  ctx.Types[ctx_type] = &type_info
  ctx.TypeReflects[reflect_type] = &type_info

  ctx.Log.Logf("serialize_types", "Registered Type: %+v - %+v", reflect_type, ctx_type)

  return nil
}

func (ctx *Context) AddNode(id NodeID, node *Node) {
  ctx.nodeMapLock.Lock()
  ctx.nodeMap[id] = node
  ctx.nodeMapLock.Unlock()
}

func (ctx *Context) Node(id NodeID) (*Node, bool) {
  ctx.nodeMapLock.RLock()
  node, exists := ctx.nodeMap[id]
  ctx.nodeMapLock.RUnlock()
  return node, exists
}

func (ctx *Context) Stop(id NodeID) error {
  ctx.nodeMapLock.Lock()
  defer ctx.nodeMapLock.Unlock()
  node, exists := ctx.nodeMap[id]
  if exists == false {
    return fmt.Errorf("%s is not a node in ctx", id)
  }

  err := node.Stop(ctx)
  delete(ctx.nodeMap, id)
  return err
}

func (ctx *Context) StopAll() {
  ctx.nodeMapLock.Lock()
  for id, node := range(ctx.nodeMap) {
    node.Stop(ctx)
    delete(ctx.nodeMap, id)
  }
  ctx.nodeMapLock.Unlock()
}

// Get a node from the context, or load from the database if not loaded
func (ctx *Context) getNode(id NodeID) (*Node, error) {
  target, exists := ctx.Node(id)

  if exists == false {
    var err error
    target, err = LoadNode(ctx, id)
    if err != nil {
      return nil, err
    }
  }
  return target, nil
}

// Route Messages to dest. Currently only local context routing is supported
func (ctx *Context) Send(messages Messages) error {
  for _, msg := range(messages) {
    ctx.Log.Logf("signal", "Sending %s -> %+v", msg.Dest, msg)
    if msg.Dest == ZeroID {
      panic("Can't send to null ID")
    }
    target, err := ctx.getNode(msg.Dest)
    if err == nil {
      select {
      case target.MsgChan <- msg:
        ctx.Log.Logf("signal", "Sent %s -> %+v", target.ID, msg)
      default:
        buf := make([]byte, 4096)
        n := runtime.Stack(buf, false)
        stack_str := string(buf[:n])
        return fmt.Errorf("SIGNAL_OVERFLOW: %s - %s", msg.Dest, stack_str)
      }
    } else if errors.Is(err, NodeNotFoundError) {
      // TODO: Handle finding nodes in other contexts
      return err
    } else {
      return err
    }
  }
  return nil
}

// Create a new Context with the base library content added
func NewContext(db * badger.DB, log Logger) (*Context, error) {
  ctx := &Context{
    DB: db,
    Log: log,
    Policies: map[PolicyType]reflect.Type{},
    PolicyTypes: map[reflect.Type]PolicyType{},
    Extensions: map[ExtType]ExtensionInfo{},
    ExtensionTypes: map[reflect.Type]ExtType{},
    Signals: map[SignalType]reflect.Type{},
    SignalTypes: map[reflect.Type]SignalType{},
    Nodes: map[NodeType]NodeInfo{},
    nodeMap: map[NodeID]*Node{},
    Types: map[SerializedType]*TypeInfo{},
    TypeReflects: map[reflect.Type]*TypeInfo{},
    Kinds: map[reflect.Kind]*KindInfo{},
    KindTypes: map[SerializedType]*KindInfo{},
  }

  var err error
  err = ctx.RegisterKind(reflect.Pointer, nil, PointerType, SerializeTypeElem, SerializePointer, DeserializeTypePointer, DeserializePointer)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Bool, reflect.TypeOf(true), BoolType, nil, SerializeBool, nil, DeserializeBool[bool])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.String, reflect.TypeOf(""), StringType, nil, SerializeString, nil, DeserializeString[string])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Float32, reflect.TypeOf(float32(0)), Float32Type, nil, SerializeFloat32, nil, DeserializeFloat32[float32])
  if err != nil  {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Float64, reflect.TypeOf(float64(0)), Float64Type, nil, SerializeFloat64, nil, DeserializeFloat64[float64])
  if err != nil  {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint, reflect.TypeOf(uint(0)), UIntType, nil, SerializeUint32, nil, DeserializeUint32[uint])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint8, reflect.TypeOf(uint8(0)), UInt8Type, nil, SerializeUint8, nil, DeserializeUint8[uint8])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint16, reflect.TypeOf(uint16(0)), UInt16Type, nil, SerializeUint16, nil, DeserializeUint16[uint16])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint32, reflect.TypeOf(uint32(0)), UInt32Type, nil, SerializeUint32, nil, DeserializeUint32[uint32])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint64, reflect.TypeOf(uint64(0)), UInt64Type, nil, SerializeUint64, nil, DeserializeUint64[uint64])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int, reflect.TypeOf(int(0)), IntType, nil, SerializeInt32, nil, DeserializeUint32[int])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int8, reflect.TypeOf(int8(0)), Int8Type, nil, SerializeInt8, nil, DeserializeUint8[int8])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int16, reflect.TypeOf(int16(0)), Int16Type, nil, SerializeInt16, nil, DeserializeUint16[int16])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int32, reflect.TypeOf(int32(0)), Int32Type, nil, SerializeInt32, nil, DeserializeUint32[int32])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int64, reflect.TypeOf(int64(0)), Int64Type, nil, SerializeInt64, nil, DeserializeUint64[int64])
  if err != nil {
    return nil, err
  }

  wait_info_type := reflect.TypeOf(WaitInfo{})
  wait_info_info, err := GetStructInfo(ctx, wait_info_type)
  if err != nil {
    return nil, err
  }
  err = ctx.RegisterType(wait_info_type, WaitInfoType, nil, SerializeStruct(wait_info_info), nil, DeserializeStruct(wait_info_info))
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(time.Duration(0)), DurationType, nil, nil, nil, DeserializeUint64[time.Duration])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(time.Time{}), TimeType, nil, SerializeGob, nil, DeserializeGob[time.Time])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Map, nil, MapType, SerializeTypeMap, SerializeMap, DeserializeTypeMap, DeserializeMap)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Array, nil, ArrayType, SerializeTypeArray, SerializeArray, DeserializeTypeArray, DeserializeArray)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Slice, nil, SliceType, SerializeTypeElem, SerializeSlice, DeserializeTypeSlice, DeserializeSlice)
  if err != nil {
    return nil, err
  }

  var ptr interface{} = nil
  err = ctx.RegisterKind(reflect.Interface, reflect.TypeOf(&ptr).Elem(), InterfaceType, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(SerializedType(0)), SerializedTypeSerialized, nil, SerializeUint64, nil, DeserializeUint64[SerializedType])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(Changes{}), ChangesSerialized, SerializeTypeStub, SerializeMap, DeserializeTypeStub[Changes], DeserializeMap)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(ExtType(0)), ExtTypeSerialized, nil, SerializeUint64, nil, DeserializeUint64[ExtType])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(NodeType(0)), NodeTypeSerialized, nil, SerializeUint64, nil, DeserializeUint64[NodeType])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(PolicyType(0)), PolicyTypeSerialized, nil, SerializeUint64, nil, DeserializeUint64[PolicyType])
  if err != nil {
    return nil, err
  }

  node_id_type := reflect.TypeOf(RandID())
  err = ctx.RegisterType(node_id_type, NodeIDType, SerializeTypeStub, SerializeUUID, DeserializeTypeStub[NodeID], DeserializeUUID[NodeID])
  if err != nil {
    return nil, err
  }

  uuid_type := reflect.TypeOf(uuid.UUID{})
  err = ctx.RegisterType(uuid_type, UUIDType, SerializeTypeStub, SerializeUUID, DeserializeTypeStub[uuid.UUID], DeserializeUUID[uuid.UUID])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(Up), SignalDirectionType, nil, SerializeUint8, nil, DeserializeUint8[SignalDirection])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(ReqState(0)), ReqStateType, nil, SerializeUint8, nil, DeserializeUint8[ReqState])
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(Tree{}), TreeType, SerializeTypeStub, nil, DeserializeTypeStub[Tree], nil)

  var extension Extension = nil
  err = ctx.RegisterType(reflect.ValueOf(&extension).Type().Elem(), ExtSerialized, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  var policy Policy = nil
  err = ctx.RegisterType(reflect.ValueOf(&policy).Type().Elem(), PolicySerialized, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  var signal Signal = nil
  err = ctx.RegisterType(reflect.ValueOf(&signal).Type().Elem(), SignalSerialized, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  pending_acl_type := reflect.TypeOf(PendingACL{})
  pending_acl_info, err := GetStructInfo(ctx, pending_acl_type)
  if err != nil {
    return nil, err
  }
  err = ctx.RegisterType(pending_acl_type, PendingACLType, nil, SerializeStruct(pending_acl_info), nil, DeserializeStruct(pending_acl_info))
  if err != nil {
    return nil, err
  }

  pending_signal_type := reflect.TypeOf(PendingACLSignal{})
  pending_signal_info, err := GetStructInfo(ctx, pending_signal_type)
  if err != nil {
    return nil, err
  }
  err = ctx.RegisterType(pending_signal_type, PendingACLSignalType, nil, SerializeStruct(pending_signal_info), nil, DeserializeStruct(pending_signal_info))
  if err != nil {
    return nil, err
  }

  queued_signal_type := reflect.TypeOf(QueuedSignal{})
  queued_signal_info, err := GetStructInfo(ctx, queued_signal_type)
  if err != nil {
    return nil, err
  }
  err = ctx.RegisterType(queued_signal_type, QueuedSignalType, nil, SerializeStruct(queued_signal_info), nil, DeserializeStruct(queued_signal_info))
  if err != nil {
    return nil, err
  }

  node_type := reflect.TypeOf(Node{})
  node_info, err := GetStructInfo(ctx, node_type)
  if err != nil {
    return nil, err
  }
  err = ctx.RegisterType(node_type, NodeStructType, nil, SerializeStruct(node_info), nil, DeserializeStruct(node_info))
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(reflect.TypeOf((*LockableExt)(nil)), LockableExtType, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(reflect.TypeOf((*ListenerExt)(nil)), ListenerExtType, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(reflect.TypeOf((*GroupExt)(nil)), GroupExtType, nil)
  if err != nil {
    return nil, err
  }

  gql_ctx := NewGQLExtContext()
  err = ctx.RegisterExtension(reflect.TypeOf((*GQLExt)(nil)), GQLExtType, gql_ctx)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(reflect.TypeOf((*ACLExt)(nil)), ACLExtType, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterExtension(reflect.TypeOf((*EventExt)(nil)), EventExtType, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterPolicy(reflect.TypeOf(ParentOfPolicy{}), ParentOfPolicyType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterPolicy(reflect.TypeOf(MemberOfPolicy{}), MemberOfPolicyType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterPolicy(reflect.TypeOf(AllNodesPolicy{}), AllNodesPolicyType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterPolicy(reflect.TypeOf(PerNodePolicy{}), PerNodePolicyType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterPolicy(reflect.TypeOf(ACLProxyPolicy{}), ACLProxyPolicyType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(StoppedSignal{}), StoppedSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(AddSubGroupSignal{}), AddSubGroupSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(RemoveSubGroupSignal{}), RemoveSubGroupSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(ACLTimeoutSignal{}), ACLTimeoutSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(ACLSignal{}), ACLSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(RemoveMemberSignal{}), RemoveMemberSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(AddMemberSignal{}), AddMemberSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(StopSignal{}), StopSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(CreateSignal{}), CreateSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(StartSignal{}), StartSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(StatusSignal{}), StatusSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(ReadSignal{}), ReadSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(LockSignal{}), LockSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(TimeoutSignal{}), TimeoutSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(LinkSignal{}), LinkSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(ErrorSignal{}), ErrorSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(SuccessSignal{}), SuccessSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(ReadResultSignal{}), ReadResultSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(EventControlSignal{}), EventControlSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf(EventStateSignal{}), EventStateSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterNodeType(BaseNodeType, []ExtType{})
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterNodeType(GroupNodeType, []ExtType{GroupExtType})
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterNodeType(GQLNodeType, []ExtType{GQLExtType})
  if err != nil {
    return nil, err
  }

  schema, err := BuildSchema(gql_ctx)
  if err != nil {
    return nil, err
  }

  gql_ctx.Schema = schema

  return ctx, nil
}
