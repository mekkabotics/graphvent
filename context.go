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
  "github.com/graphql-go/graphql"

  badger "github.com/dgraph-io/badger/v3"
)

var (
  NodeNotFoundError = errors.New("Node not found in DB")
  ECDH = ecdh.X25519()
)

type ExtensionInfo struct {
  Reflect reflect.Type
  Interface graphql.Interface

  Data interface{}
}

type FieldIndex struct {
  Extension ExtType
  Field string
}

type NodeInfo struct {
  Extensions []ExtType
  Policies []Policy
  Fields map[string]FieldIndex
}

type GQLValueConverter func(*Context, interface{})(reflect.Value, error)

type TypeInfo struct {
  Reflect reflect.Type
  GQL graphql.Type

  Type SerializedType
  TypeSerialize TypeSerializeFn
  Serialize SerializeFn
  TypeDeserialize TypeDeserializeFn
  Deserialize DeserializeFn

  GQLValue GQLValueConverter
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
  NodeTypes map[string]NodeType

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
func RegisterNodeType(ctx *Context, name string, extensions []ExtType, mappings map[string]FieldIndex) error {
  node_type := NodeTypeFor(name, extensions, mappings)
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
    Fields: mappings,
  }
  ctx.NodeTypes[name] = node_type

  return nil
}

func RegisterPolicy[P Policy](ctx *Context) error {
  reflect_type := reflect.TypeFor[P]()
  policy_type := PolicyTypeFor[P]()

  _, exists := ctx.Policies[policy_type]
  if exists == true {
    return fmt.Errorf("Cannot register policy of type %+v, type already exists in context", policy_type)
  }

  policy_info, err := GetStructInfo(ctx, reflect_type)
  if err != nil {
    return err
  }

  err = RegisterType[P](ctx, nil, SerializeStruct(policy_info), nil, DeserializeStruct(policy_info))
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered PolicyType: %+v - %+v", reflect_type, policy_type)

  ctx.Policies[policy_type] = reflect_type
  ctx.PolicyTypes[reflect_type] = policy_type
  return nil
}

func RegisterSignal[S Signal](ctx *Context) error {
  reflect_type := reflect.TypeFor[S]()
  signal_type := SignalTypeFor[S]()

  _, exists := ctx.Signals[signal_type]
  if exists == true {
    return fmt.Errorf("Cannot register signal of type %+v, type already exists in context", signal_type)
  }

  signal_info, err := GetStructInfo(ctx, reflect_type)
  if err != nil {
    return err
  }

  err = RegisterType[S](ctx, nil, SerializeStruct(signal_info), nil, DeserializeStruct(signal_info))
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered SignalType: %+v - %+v", reflect_type, signal_type)

  ctx.Signals[signal_type] = reflect_type
  ctx.SignalTypes[reflect_type] = signal_type
  return nil
}

func RegisterExtension[E any, T interface { *E; Extension}](ctx *Context, data interface{}) error {
  reflect_type := reflect.TypeFor[T]()
  ext_type := ExtType(SerializedTypeFor[E]())
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension %+v of type %+v, type already exists in context", reflect_type, ext_type)
  }

  elem_type := reflect_type.Elem()
  elem_info, err := GetStructInfo(ctx, elem_type)
  if err != nil {
    return err
  }

  err = RegisterType[E](ctx, nil, SerializeStruct(elem_info), nil, DeserializeStruct(elem_info))
  if err != nil {
    return err
  }

  ctx.Log.Logf("serialize_types", "Registered ExtType: %+v - %+v", reflect_type, ext_type)

  ctx.Extensions[ext_type] = ExtensionInfo{
    Reflect: reflect_type,
    Data: data,
  }
  ctx.ExtensionTypes[reflect_type] = ext_type

  return nil
}

func RegisterKind(ctx *Context, kind reflect.Kind, base reflect.Type, type_serialize TypeSerializeFn, serialize SerializeFn, type_deserialize TypeDeserializeFn, deserialize DeserializeFn) error {
  ctx_type := SerializedKindFor(kind)
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
    Type: ctx_type,
    Base: base,
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

func RegisterType[T any](ctx *Context, type_serialize TypeSerializeFn, serialize SerializeFn, type_deserialize TypeDeserializeFn, deserialize DeserializeFn) error {
  reflect_type := reflect.TypeFor[T]()
  ctx_type := SerializedTypeFor[T]()

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

func RegisterStruct[T any](ctx *Context) error {
  struct_info, err := GetStructInfo(ctx, reflect.TypeFor[T]())
  if err != nil {
    return err
  }
  return RegisterType[T](ctx, nil, SerializeStruct(struct_info), nil, DeserializeStruct(struct_info))
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
    NodeTypes: map[string]NodeType{},
    Types: map[SerializedType]*TypeInfo{},
    TypeReflects: map[reflect.Type]*TypeInfo{},
    Kinds: map[reflect.Kind]*KindInfo{},
    KindTypes: map[SerializedType]*KindInfo{},

    nodeMap: map[NodeID]*Node{},
  }

  var err error
  err = RegisterKind(ctx, reflect.Pointer, nil, SerializeTypeElem, SerializePointer, DeserializeTypePointer, DeserializePointer)
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Bool, reflect.TypeFor[bool](), nil, SerializeBool, nil, DeserializeBool[bool])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.String, reflect.TypeFor[string](), nil, SerializeString, nil, DeserializeString[string])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Float32, reflect.TypeFor[float32](), nil, SerializeFloat32, nil, DeserializeFloat32[float32])
  if err != nil  {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Float64, reflect.TypeFor[float64](), nil, SerializeFloat64, nil, DeserializeFloat64[float64])
  if err != nil  {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Uint, reflect.TypeFor[uint](), nil, SerializeUint32, nil, DeserializeUint32[uint])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Uint8, reflect.TypeFor[uint8](), nil, SerializeUint8, nil, DeserializeUint8[uint8])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Uint16, reflect.TypeFor[uint16](), nil, SerializeUint16, nil, DeserializeUint16[uint16])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Uint32, reflect.TypeFor[uint32](), nil, SerializeUint32, nil, DeserializeUint32[uint32])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Uint64, reflect.TypeFor[uint64](), nil, SerializeUint64, nil, DeserializeUint64[uint64])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Int, reflect.TypeFor[int](), nil, SerializeInt32, nil, DeserializeUint32[int])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Int8, reflect.TypeFor[int8](), nil, SerializeInt8, nil, DeserializeUint8[int8])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Int16, reflect.TypeFor[int16](), nil, SerializeInt16, nil, DeserializeUint16[int16])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Int32, reflect.TypeFor[int32](), nil, SerializeInt32, nil, DeserializeUint32[int32])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Int64, reflect.TypeFor[int64](), nil, SerializeInt64, nil, DeserializeUint64[int64])
  if err != nil {
    return nil, err
  }

  err = RegisterType[WaitReason](ctx, nil, nil, nil, DeserializeString[WaitReason])
  if err != nil {
    return nil, err
  }

  err = RegisterType[EventCommand](ctx, nil, nil, nil, DeserializeString[EventCommand])
  if err != nil {
    return nil, err
  }

  err = RegisterType[EventState](ctx, nil, nil, nil, DeserializeString[EventState])
  if err != nil {
    return nil, err
  }

  err = RegisterStruct[WaitInfo](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterType[time.Duration](ctx, nil, nil, nil, DeserializeUint64[time.Duration])
  if err != nil {
    return nil, err
  }

  err = RegisterType[time.Time](ctx, nil, SerializeGob, nil, DeserializeGob[time.Time])
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Map, nil, SerializeTypeMap, SerializeMap, DeserializeTypeMap, DeserializeMap)
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Array, nil, SerializeTypeArray, SerializeArray, DeserializeTypeArray, DeserializeArray)
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Slice, nil, SerializeTypeElem, SerializeSlice, DeserializeTypeSlice, DeserializeSlice)
  if err != nil {
    return nil, err
  }

  err = RegisterKind(ctx, reflect.Interface, reflect.TypeFor[interface{}](), nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  err = RegisterType[SerializedType](ctx, nil, SerializeUint64, nil, DeserializeUint64[SerializedType])
  if err != nil {
    return nil, err
  }

  err = RegisterType[Changes](ctx, SerializeTypeStub, SerializeMap, DeserializeTypeStub[Changes], DeserializeMap)
  if err != nil {
    return nil, err
  }

  err = RegisterType[ExtType](ctx, nil, SerializeUint64, nil, DeserializeUint64[ExtType])
  if err != nil {
    return nil, err
  }

  err = RegisterType[NodeType](ctx, nil, SerializeUint64, nil, DeserializeUint64[NodeType])
  if err != nil {
    return nil, err
  }

  err = RegisterType[PolicyType](ctx, nil, SerializeUint64, nil, DeserializeUint64[PolicyType])
  if err != nil {
    return nil, err
  }

  err = RegisterType[NodeID](ctx, SerializeTypeStub, SerializeUUID, DeserializeTypeStub[NodeID], DeserializeUUID[NodeID])
  if err != nil {
    return nil, err
  }

  err = RegisterType[uuid.UUID](ctx, SerializeTypeStub, SerializeUUID, DeserializeTypeStub[uuid.UUID], DeserializeUUID[uuid.UUID])
  if err != nil {
    return nil, err
  }

  err = RegisterType[SignalDirection](ctx, nil, SerializeUint8, nil, DeserializeUint8[SignalDirection])
  if err != nil {
    return nil, err
  }

  err = RegisterType[ReqState](ctx, nil, SerializeUint8, nil, DeserializeUint8[ReqState])
  if err != nil {
    return nil, err
  }

  err = RegisterType[Tree](ctx, SerializeTypeStub, nil, DeserializeTypeStub[Tree], nil)
  if err != nil {
    return nil, err
  }

  err = RegisterType[Extension](ctx, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  err = RegisterType[Policy](ctx, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  err = RegisterType[Signal](ctx, nil, SerializeInterface, nil, DeserializeInterface)
  if err != nil {
    return nil, err
  }

  err = RegisterStruct[PendingACL](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterStruct[PendingACLSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterStruct[QueuedSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterStruct[Node](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[LockableExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[ListenerExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[GroupExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  gql_ctx := NewGQLExtContext()
  err = RegisterExtension[GQLExt](ctx, gql_ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[ACLExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterExtension[EventExt](ctx, nil)
  if err != nil {
    return nil, err
  }

  err = RegisterPolicy[OwnerOfPolicy](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterPolicy[ParentOfPolicy](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterPolicy[MemberOfPolicy](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterPolicy[AllNodesPolicy](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterPolicy[PerNodePolicy](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterPolicy[ACLProxyPolicy](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[StoppedSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[AddSubGroupSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[RemoveSubGroupSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[ACLTimeoutSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[ACLSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[RemoveMemberSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[AddMemberSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[StopSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[CreateSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[StartSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[StatusSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[ReadSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[LockSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[TimeoutSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[LinkSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[ErrorSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[SuccessSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[ReadResultSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[EventControlSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterSignal[EventStateSignal](ctx)
  if err != nil {
    return nil, err
  }

  err = RegisterNodeType(ctx, "Base", []ExtType{}, map[string]FieldIndex{})
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
