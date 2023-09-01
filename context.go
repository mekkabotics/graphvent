package graphvent

import (
  "crypto/ecdh"
  "crypto/sha512"
  "encoding/binary"
  "errors"
  "fmt"
  "reflect"
  "runtime"
  "sync"

  badger "github.com/dgraph-io/badger/v3"
)

func Hash(base string, name string) uint64 {
  digest := append([]byte(base), 0x00)
  digest = append(digest, []byte(name)...)
  hash := sha512.Sum512(digest)
  return binary.BigEndian.Uint64(hash[0:8])
}

type ExtType uint64
type NodeType uint64
type SignalType uint64
type PolicyType uint64
type SerializedType uint64

func NewExtType(name string) ExtType {
  return ExtType(Hash(ExtTypeBase, name))
}

func NewNodeType(name string) NodeType {
  return NodeType(Hash(NodeTypeBase, name))
}

func NewSignalType(name string) SignalType {
  return SignalType(Hash(SignalTypeBase, name))
}

func NewPolicyType(name string) PolicyType {
  return PolicyType(Hash(PolicyTypeBase, name))
}

func NewSerializedType(name string) SerializedType {
  return SerializedType(Hash(SerializedTypeBase, name))
}

const (
  ExtTypeBase = "ExtType"
  NodeTypeBase = "NodeType"
  SignalTypeBase = "SignalType"
  PolicyTypeBase = "PolicyType"
  SerializedTypeBase = "SerializedType"
  FieldNameBase = "FieldName"
)

var (
  ListenerExtType = NewExtType("LISTENER")
  LockableExtType = NewExtType("LOCKABLE")
  GQLExtType      = NewExtType("GQL")
  GroupExtType    = NewExtType("GROUP")
  ECDHExtType     = NewExtType("ECDH")

  GQLNodeType = NewNodeType("GQL")

  StopSignalType       = NewSignalType("STOP")
  CreateSignalType     = NewSignalType("CREATE")
  StartSignalType      = NewSignalType("START")
  ErrorSignalType      = NewSignalType("ERROR")
  StatusSignalType     = NewSignalType("STATUS")
  LinkSignalType       = NewSignalType("LINK")
  LockSignalType       = NewSignalType("LOCK")
  ReadSignalType       = NewSignalType("READ")
  ReadResultSignalType = NewSignalType("READ_RESULT")
  ACLTimeoutSignalType = NewSignalType("ACL_TIMEOUT")

  MemberOfPolicyType      = NewPolicyType("USER_OF")
  RequirementOfPolicyType = NewPolicyType("REQUIEMENT_OF")
  PerNodePolicyType       = NewPolicyType("PER_NODE")
  AllNodesPolicyType      = NewPolicyType("ALL_NODES")

  StructType = NewSerializedType("struct")
  SliceType = NewSerializedType("slice")
  ArrayType = NewSerializedType("array")
  PointerType = NewSerializedType("pointer")
  MapType = NewSerializedType("map")
  ErrorType = NewSerializedType("error")
  ExtensionType = NewSerializedType("extension")

  StringType = NewSerializedType("string")
  NodeKeyType = NewSerializedType("node_key")

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

type TypeSerialize func(*Context,interface{}) ([]byte, error)
type TypeDeserialize func(*Context,[]byte) (interface{}, error)
type TypeInfo struct {
  Type reflect.Type
  Serialize TypeSerialize
  Deserialize TypeDeserialize
}

type Int int
func (i Int) MarshalBinary() ([]byte, error) {
  ret := make([]byte, 8)
  binary.BigEndian.PutUint64(ret, uint64(i))
  return ret, nil
}

type String string
func (str String) MarshalBinary() ([]byte, error) {
  return []byte(str), nil
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
  Types map[SerializedType]TypeInfo
  TypeReflects map[reflect.Type]SerializedType

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

  ctx.Policies[policy_type] = reflect_type
  ctx.PolicyTypes[reflect_type] = policy_type
  return nil
}

func (ctx *Context)RegisterSignal(reflect_type reflect.Type, signal_type SignalType) error {
  _, exists := ctx.Signals[signal_type]
  if exists == true {
    return fmt.Errorf("Cannot register signal of type %+v, type already exists in context", signal_type)
  }

  ctx.Signals[signal_type] = reflect_type
  ctx.SignalTypes[reflect_type] = signal_type
  return nil
}

// Add a node to a context, returns an error if the def is invalid or already exists in the context
func (ctx *Context)RegisterExtension(reflect_type reflect.Type, ext_type ExtType, data interface{}) error {
  _, exists := ctx.Extensions[ext_type]
  if exists == true {
    return fmt.Errorf("Cannot register extension of type %+v, type already exists in context", ext_type)
  }

  ctx.Extensions[ext_type] = ExtensionInfo{
    Type: reflect_type,
    Data: data,
  }
  ctx.ExtensionTypes[reflect_type] = ext_type

  return nil
}

func (ctx *Context)RegisterType(reflect_type reflect.Type, ctx_type SerializedType, serialize TypeSerialize, deserialize TypeDeserialize) error {
  _, exists := ctx.Types[ctx_type]
  if exists == true {
    return fmt.Errorf("Cannot register field of type %+v, type already exists in context", ctx_type)
  }
  _, exists = ctx.TypeReflects[reflect_type]
  if exists == true {
    return fmt.Errorf("Cannot register field with type %+v, type already registered in context", reflect_type)
  }
  if deserialize == nil {
    return fmt.Errorf("Cannot register field without deserialize function")
  }
  if serialize == nil {
    return fmt.Errorf("Cannot register field without serialize function")
  }

  ctx.Types[ctx_type] = TypeInfo{
    Type: reflect_type,
    Serialize: serialize,
    Deserialize: deserialize,
  }
  ctx.TypeReflects[reflect_type] = ctx_type

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

// Route a Signal to dest. Currently only local context routing is supported
func (ctx *Context) Send(messages Messages) error {
  for _, msg := range(messages) {
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

type defaultKind struct {
  Type SerializedType
  Serialize func(interface{})([]byte, error)
  Deserialize func([]byte)(interface{}, error)
}

var defaultKinds = map[reflect.Kind]defaultKind{
  reflect.Int: {
    Deserialize: func(data []byte)(interface{}, error){
      if len(data) != 8 {
        return nil, fmt.Errorf("invalid length: %d/8", len(data))
      }
      return int(binary.BigEndian.Uint64(data)), nil
    },
    Serialize: func(val interface{})([]byte, error){
      i, ok := val.(int)
      if ok == false {
        return nil, fmt.Errorf("invalid type %+v", reflect.TypeOf(val))
      } else {
        bytes := make([]byte, 8)
        binary.BigEndian.PutUint64(bytes, uint64(i))
        return bytes, nil
      }
    },
  },
}

type SerializedValue struct {
  TypeStack []uint64
  Data []byte
}

func (field SerializedValue) MarshalBinary() ([]byte, error) {
  data := []byte{}
  for _, t := range(field.TypeStack) {
    t_ser := make([]byte, 8)
    binary.BigEndian.PutUint64(t_ser, uint64(t))
    data = append(data, t_ser...)
  }
  data = append(data, field.Data...)
  return data, nil
}

func RecurseTypes(ctx *Context, t reflect.Type) ([]uint64, []reflect.Kind, error) {
  var ctx_type uint64 = 0x00
  ctype, exists := ctx.TypeReflects[t]
  if exists == true {
    ctx_type = uint64(ctype)
  }

  var new_types []uint64
  var new_kinds []reflect.Kind
  kind := t.Kind()
  switch kind {
  case reflect.Array:
    if ctx_type == 0x00 {
      ctx_type = uint64(ArrayType)
    }
    elem_types, elem_kinds, err := RecurseTypes(ctx, t.Elem())
    if err != nil {
      return nil, nil, err
    }
    new_types = append(new_types, ctx_type)
    new_types = append(new_types, elem_types...)

    new_kinds = append(new_kinds, reflect.Array)
    new_kinds = append(new_kinds, elem_kinds...)
  case reflect.Map:
    if ctx_type == 0x00 {
      ctx_type = uint64(MapType)
    }
    key_types, key_kinds, err := RecurseTypes(ctx, t.Key())
    if err != nil {
      return nil, nil, err
    }
    elem_types, elem_kinds, err := RecurseTypes(ctx, t.Elem())
    if err != nil {
      return nil, nil, err
    }
    new_types = append(new_types, ctx_type)
    new_types = append(new_types, key_types...)
    new_types = append(new_types, elem_types...)

    new_kinds = append(new_kinds, reflect.Map)
    new_kinds = append(new_kinds, key_kinds...)
    new_kinds = append(new_kinds, elem_kinds...)
  case reflect.Slice:
    if ctx_type == 0x00 {
      ctx_type = uint64(SliceType)
    }
    elem_types, elem_kinds, err := RecurseTypes(ctx, t.Elem())
    if err != nil {
      return nil, nil, err
    }
    new_types = append(new_types, ctx_type)
    new_types = append(new_types, elem_types...)

    new_kinds = append(new_kinds, reflect.Slice)
    new_kinds = append(new_kinds, elem_kinds...)
  case reflect.Pointer:
    if ctx_type == 0x00 {
      ctx_type = uint64(PointerType)
    }
    elem_types, elem_kinds, err := RecurseTypes(ctx, t.Elem())
    if err != nil {
      return nil, nil, err
    }
    new_types = append(new_types, ctx_type)
    new_types = append(new_types, elem_types...)

    new_kinds = append(new_kinds, reflect.Pointer)
    new_kinds = append(new_kinds, elem_kinds...)
  case reflect.String:
    if ctx_type == 0x00 {
      ctx_type = uint64(StringType)
    }
    new_types = append(new_types, ctx_type)
    new_kinds = append(new_kinds, reflect.String)
  default:
    return nil, nil, fmt.Errorf("unhandled kind: %+v - %+v", kind, t)
  }
  return new_types, new_kinds, nil
}

func serializeValue(ctx *Context, kind_stack []reflect.Kind, value reflect.Value) ([]byte, error) {
  kind := kind_stack[len(kind_stack) - 1]
  switch kind {
  default:
    return nil, fmt.Errorf("unhandled kind: %+v", kind)
  }
}

func SerializeValue(ctx *Context, value reflect.Value) (SerializedValue, error) {
  if value.IsValid() == false {
    return SerializedValue{}, fmt.Errorf("Cannot serialize invalid value: %+v", value)
  }

  type_stack, kind_stack, err := RecurseTypes(ctx, value.Type())
  if err != nil {
    return SerializedValue{}, err
  }

  bytes, err := serializeValue(ctx, kind_stack, value)
  if err != nil {
    return SerializedValue{}, err
  }
  return SerializedValue{
    type_stack,
    bytes,
  }, nil
}

/*
  default:
    kind_def, handled := defaultKinds[kind]
    if handled == false {
      ctx_type, handled := ctx.TypeReflects[value.Type()]
      if handled == false {
        err = fmt.Errorf("%+v is not a handled reflect type", value.Type())
        break
      }
      type_info, handled := ctx.Types[ctx_type]
      if handled == false {
        err = fmt.Errorf("%+v is not a handled reflect type(INTERNAL_ERROR)", value.Type())
        break
      }
      field_ser, err := type_info.Serialize(ctx, value.Interface())
      if err != nil {
        err = fmt.Errorf(err.Error())
        break
      }
      ret = SerializedValue{
        []uint64{uint64(ctx_type)},
        field_ser,
      }
    }
    field_ser, err := kind_def.Serialize(value.Interface())
    if err != nil {
      err = fmt.Errorf(err.Error())
    } else {
      ret = SerializedValue{
        []uint64{uint64(kind_def.Type)},
        field_ser,
      }
    }
*/

func SerializeField(ctx *Context, ext Extension, field_name string) (SerializedValue, error) {
  if ext == nil {
    return SerializedValue{}, fmt.Errorf("Cannot get fields on nil Extension")
  }
  ext_value := reflect.ValueOf(ext).Elem()
  field := ext_value.FieldByName(field_name)
  if field.IsValid() == false {
    return SerializedValue{}, fmt.Errorf("%s is not a field in %+v", field_name, ext)
  } else {
    return SerializeValue(ctx, field)
  }
}

func SerializeSignal(ctx *Context, signal Signal, ctx_type SignalType) (SerializedValue, error) {
  return SerializedValue{}, nil
}

func SerializeExtension(ctx *Context, ext Extension, ctx_type ExtType) (SerializedValue, error) {
  if ext == nil {
    return SerializedValue{}, fmt.Errorf("Cannot serialize nil Extension ")
  }
  ext_type := reflect.TypeOf(ext).Elem()
  ext_value := reflect.ValueOf(ext).Elem()

  m := map[string]SerializedValue{}
  for _, field := range(reflect.VisibleFields(ext_type)) {
    ext_tag, tagged_ext := field.Tag.Lookup("ext")
    if tagged_ext == false {
      continue
    } else {
      field_value := ext_value.FieldByIndex(field.Index)
      var err error
      m[ext_tag], err = SerializeValue(ctx, field_value)
      if err != nil {
        return SerializedValue{}, err
      }
    }
  }
  map_value := reflect.ValueOf(m)
  map_ser, err := SerializeValue(ctx, map_value)
  if err != nil {
    return SerializedValue{}, err
  }
  return SerializedValue{
    append([]uint64{uint64(ctx_type)}, map_ser.TypeStack...),
    map_ser.Data,
  }, nil
}

func DeserializeValue(ctx *Context, value SerializedValue) (interface{}, error) {
  // TODO: do the opposite of SerializeValue.
  // 1) Check the type to handle special types(array, list, map, pointer)
  // 2) Check if the type is registered in the context, handle if so
  // 3) Check if the type is a default type, handle if so
  // 4) Return error if we don't know how to deserialize the type
  return nil, fmt.Errorf("Undefined")
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
    Types: map[SerializedType]TypeInfo{},
    TypeReflects: map[reflect.Type]SerializedType{},
  }

  var err error
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

  err = ctx.RegisterSignal(reflect.TypeOf((*StopSignal)(nil)), StopSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf((*CreateSignal)(nil)), CreateSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf((*StartSignal)(nil)), StartSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf((*ReadSignal)(nil)), ReadSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterSignal(reflect.TypeOf((*ReadResultSignal)(nil)), ReadResultSignalType)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterNodeType(GQLNodeType, []ExtType{GroupExtType, GQLExtType})
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
