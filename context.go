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
  val := SerializedType(Hash(SerializedTypeBase, name))
  println(fmt.Sprintf("TYPE: %s: %d", name, val))
  return val
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
  Uint8Type = NewSerializedType("uint8")
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

func SerializeValue(ctx *Context, value reflect.Value) (SerializedValue, error) {
  val, err := serializeValue(ctx, value.Type(), &value)
  ctx.Log.Logf("serialize", "SERIALIZED_VALUE(%+v): %+v - %s", value, val, err)
  return val, err
}

func serializeValue(ctx *Context, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
  var ctx_type uint64 = 0x00
  ctype, exists := ctx.TypeReflects[t]
  ctx.Log.Logf("serialize", "TYPE_REFLECTS: %+v", ctx.TypeReflects)
  if exists == true {
    type_info := ctx.Types[ctype]
    ctx_type = uint64(ctype)
    val_ser, err := type_info.Serialize(ctx, value.Interface())
    if err != nil {
      return SerializedValue{}, err
    }
    return SerializedValue{
      []uint64{ctx_type},
      val_ser,
    }, nil
  }

  kind := t.Kind()
  switch kind {
  case reflect.Map:
    if ctx_type == 0x00 {
      ctx_type = uint64(MapType)
    }
    var data []byte 
    if value == nil {
      data = nil
    } else if value.IsZero() {
      data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      map_iter := value.MapRange()
      key_data := []byte{}
      val_data := []byte{}
      var key_types []uint64 = nil
      var val_types []uint64 = nil
      map_len := 0
      for map_iter.Next() {
        map_len += 1
        key_value := map_iter.Key()
        val_value := map_iter.Value()

        key, err := serializeValue(ctx, t.Key(), &key_value)
        if err != nil {
          return SerializedValue{}, err
        }
        val, err := serializeValue(ctx, t.Elem(), &val_value)
        if err != nil {
          return SerializedValue{}, err
        }

        if key_types == nil {
          key_types = key.TypeStack
          val_types = val.TypeStack
        }

        key_data = append(key_data, key.Data...)
        val_data = append(val_data, val.Data...)
      }

      type_stack := []uint64{ctx_type}
      type_stack = append(type_stack, key_types...)
      type_stack = append(type_stack, val_types...)

      data := make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(map_len))
      data = append(data, key_data...)
      data = append(data, val_data...)
      return SerializedValue{
        type_stack,
        data,
      }, nil
    }
    key, err := serializeValue(ctx, t.Key(), nil)
    if err != nil {
      return SerializedValue{}, err
    }
    elem, err := serializeValue(ctx, t.Elem(), nil)
    if err != nil {
      return SerializedValue{}, err
    }
    type_stack := []uint64{ctx_type}
    type_stack = append(type_stack, key.TypeStack...)
    type_stack = append(type_stack, elem.TypeStack...)
    return SerializedValue{
      type_stack,
      data,
    }, nil
  case reflect.Slice:
    if ctx_type == 0x00 {
      ctx_type = uint64(SliceType)
    }
    var data []byte
    if value == nil {
      data = nil
    } else if value.IsZero() {
      data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      data := make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Len()))
      var elem SerializedValue
      for i := 0; i < value.Len(); i += 1 {
        val := value.Index(i)
        element, err := serializeValue(ctx, t.Elem(), &val)
        if err != nil {
          return SerializedValue{}, err
        }
        if i == 0 {
          elem = element
        }
        data = append(data, elem.Data...)
      }
      return SerializedValue{
        append([]uint64{ctx_type}, elem.TypeStack...),
        data,
      }, nil
    }
    elem, err := serializeValue(ctx, t.Elem(), nil)
    if err != nil {
      return SerializedValue{}, err
    }
    return SerializedValue{
      append([]uint64{ctx_type}, elem.TypeStack...),
      data,
    }, nil
  case reflect.Pointer:
    if ctx_type == 0x00 {
      ctx_type = uint64(PointerType)
    }
    var data []byte 
    var elem_value *reflect.Value = nil
    if value == nil {
      data = nil
    } else if value.IsZero() {
      data = []byte{0x01}
    } else {
      data = []byte{0x00}
      ev := value.Elem()
      elem_value = &ev
    }
    elem, err := serializeValue(ctx, t.Elem(), elem_value)
    if err != nil {
      return SerializedValue{}, err
    }
    if elem.Data != nil {
      data = append(data, elem.Data...)
    }
    return SerializedValue{
      append([]uint64{uint64(ctx_type)}, elem.TypeStack...),
      data,
    }, nil
  case reflect.String:
    if ctx_type == 0x00 {
      ctx_type = uint64(StringType)
    }
    if value == nil {
      return SerializedValue{
        []uint64{ctx_type},
        nil,
      }, nil
    }

    data := make([]byte, 8)
    str := value.String()
    binary.BigEndian.PutUint64(data, uint64(len(str)))
    return SerializedValue{
      []uint64{uint64(ctx_type)},
      append(data, []byte(str)...),
    }, nil
  case reflect.Uint8:
    if ctx_type == 0x00 {
      ctx_type = uint64(Uint8Type)
    }
    return SerializedValue{
      []uint64{uint64(ctx_type)},
      []byte{uint8(value.Uint())},
    }, nil
  default:
    return SerializedValue{}, fmt.Errorf("unhandled kind: %+v - %+v", kind, t)
  }
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

func (value SerializedValue) MarshalBinary() ([]byte, error) {
  return nil, fmt.Errorf("SerializedValue.MarshalBinary Undefined")
}

func ParseSerializedValue(ctx *Context, data []byte) (SerializedValue, []byte, error) {
  return SerializedValue{}, nil, fmt.Errorf("ParseSerializedValue Undefined")
}

func DeserializeValue(ctx *Context, value SerializedValue) (interface{}, error) {
  return nil, fmt.Errorf("DeserializeValue Undefined")
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
  err = ctx.RegisterType(reflect.TypeOf(SerializedValue{}), NewSerializedType("SerializedValue"),
  func(ctx *Context, val interface{}) ([]byte, error) {
    value := val.(SerializedValue)
    return value.MarshalBinary()
  }, func(ctx *Context, data []byte) (interface{}, error) {
    value, data, err := ParseSerializedValue(ctx, data)
    if err != nil {
      return nil, err
    }
    if data != nil {
      return nil, fmt.Errorf("%+v remaining after parse", data)
    }
    return value, nil
  })

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
