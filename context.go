package graphvent

import (
	"crypto/ecdh"
	"crypto/sha512"
	"encoding/binary"
	"errors"
	"fmt"
 "math"
	"reflect"
	"runtime"
	"sync"
 "strconv"

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
  TagBase = "GraphventTag"
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

  ErrorType = NewSerializedType("ERROR")

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

type TypeSerialize func(*Context,uint64,reflect.Type,*reflect.Value) (SerializedValue, error)
type TypeDeserialize func(*Context,SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error)
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

  Kinds map[reflect.Kind]KindInfo
  KindTypes map[SerializedType]reflect.Kind

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

func (ctx *Context)RegisterKind(kind reflect.Kind, ctx_type SerializedType, serialize TypeSerialize, deserialize TypeDeserialize) error {
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

  ctx.Kinds[kind] = KindInfo{
    ctx_type,
    serialize,
    deserialize,
  }
  ctx.KindTypes[ctx_type] = kind

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

type KindInfo struct {
  Type SerializedType
  Serialize TypeSerialize
  Deserialize TypeDeserialize
}

type SerializedValue struct {
  TypeStack []uint64
  Data []byte
}

func SerializeValue(ctx *Context, value reflect.Value) (SerializedValue, error) {
  val, err := serializeValue(ctx, value.Type(), &value)
  ctx.Log.Logf("serialize", "SERIALIZED_VALUE(%+v): %+v - %+v", value.Type(), val.TypeStack, val.Data)
  return val, err
}

func serializeValue(ctx *Context, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
  var ctx_type uint64 = 0x00
  ctype, exists := ctx.TypeReflects[t]
  if exists == true {
    type_info := ctx.Types[ctype]
    ctx_type = uint64(ctype)
    if type_info.Serialize != nil {
      return type_info.Serialize(ctx, ctx_type, t, value)
    }
  }

  kind := t.Kind()
  kind_info, handled := ctx.Kinds[kind]
  if handled == false {
    return SerializedValue{}, fmt.Errorf("Don't know how to serialize kind %+v", kind)
  } else if ctx_type == 0x00 {
    ctx_type = uint64(kind_info.Type)
  }

  return kind_info.Serialize(ctx, ctx_type, t, value)

}

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

func (value SerializedValue) MarshalBinary() ([]byte, error) {
  data := make([]byte, value.SerializedSize())
  binary.BigEndian.PutUint64(data[0:8], uint64(len(value.TypeStack)))
  binary.BigEndian.PutUint64(data[8:16], uint64(len(value.Data)))

  for i, t := range(value.TypeStack) {
    type_start := (i+2)*8
    type_end := (i+3)*8
    binary.BigEndian.PutUint64(data[type_start:type_end], t)
  }

  return append(data, value.Data...), nil
}

func (value SerializedValue) SerializedSize() uint64 {
  return uint64((len(value.TypeStack) + 2) * 8)
}

func ParseSerializedValue(data []byte) (SerializedValue, []byte, error) {
  if len(data) < 8 {
    return SerializedValue{}, nil, fmt.Errorf("SerializedValue required to have at least 8 bytes when serialized")
  }
  num_types := int(binary.BigEndian.Uint64(data[0:8]))
  data_size := int(binary.BigEndian.Uint64(data[8:16]))
  type_stack := make([]uint64, num_types)
  for i := 0; i < num_types; i += 1 {
    type_start := (i+2) * 8
    type_end := (i+3) * 8
    type_stack[i] = binary.BigEndian.Uint64(data[type_start:type_end])
  }

  types_end := 8*(num_types + 2)
  data_end := types_end + data_size
  return SerializedValue{
    type_stack,
    data[types_end:data_end],
  }, data[data_end:], nil
}

func DeserializeValue(ctx *Context, value SerializedValue, n int) (reflect.Type, []reflect.Value, SerializedValue, error) {
  ctx.Log.Logf("serialize", "DeserializeValue: %+v - %d", value, n)
  ret := make([]reflect.Value, n)

  var deserialize TypeDeserialize = nil
  var reflect_type reflect.Type = nil

  ctx_type := value.TypeStack[0]
  value.TypeStack = value.TypeStack[1:]

  type_info, exists := ctx.Types[SerializedType(ctx_type)]
  if exists == true {
    deserialize = type_info.Deserialize
    reflect_type = type_info.Type
  } else {
    kind, exists := ctx.KindTypes[SerializedType(ctx_type)]
    if exists == false {
      return nil, nil, SerializedValue{}, fmt.Errorf("Cannot deserialize 0x%x: unknown type/kind", ctx_type)
    }
    kind_info := ctx.Kinds[kind]
    deserialize = kind_info.Deserialize
  }

  ctx.Log.Logf("serialize", "Deserializing: %d x %d", ctx_type, n)

  if value.Data == nil {
    reflect_type, _, val, err := deserialize(ctx, value)
    if err != nil {
      return nil, nil, SerializedValue{}, err
    }
    return reflect_type, nil, val, nil
  }

  val := SerializedValue{
    value.TypeStack,
    value.Data,
  }
  for i := 0; i < n; i += 1 {
    val.TypeStack = value.TypeStack 
    var elem *reflect.Value
    var err error
    reflect_type, elem, val, err = deserialize(ctx, val)
    if err != nil {
      return nil, nil, SerializedValue{}, err
    }
    ret[i] = *elem
  }
  ctx.Log.Logf("serialize", "DeserializeValue: DONE %+v - %+v", val, ret)
  return reflect_type, ret, val, nil
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
    Kinds: map[reflect.Kind]KindInfo{},
    KindTypes: map[SerializedType]reflect.Kind{},
  }

  var err error
  err = ctx.RegisterKind(reflect.Pointer, NewSerializedType("pointer"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
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
      elem, err := serializeValue(ctx, reflect_type.Elem(), elem_value)
      if err != nil {
        return SerializedValue{}, err
      }
      if elem.Data != nil {
        data = append(data, elem.Data...)
      }
      return SerializedValue{
        append([]uint64{ctx_type}, elem.TypeStack...),
        data,
      }, nil
  }, func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    if value.Data == nil {
      elem_type, _, _, err := DeserializeValue(ctx, value, 1)
      if err != nil {
        return nil, nil, SerializedValue{}, err
      }
      return reflect.PointerTo(elem_type), nil, value, nil
    } else if len(value.Data) < 1 {
      return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize pointer")
    } else {
      pointer_flags := value.Data[0]
      value.Data = value.Data[1:]
      if pointer_flags == 0x00 {
        _, elem_value, remaining_data, err := DeserializeValue(ctx, value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        pointer_value := elem_value[0].Addr()
        return pointer_value.Type(), &pointer_value, remaining_data, nil
      } else if pointer_flags == 0x01 {
        elem_type, _, remaining_data, err := DeserializeValue(ctx, value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }

        pointer_type := reflect.PointerTo(elem_type)
        pointer_value := reflect.New(pointer_type).Elem()
        return pointer_type, &pointer_value, remaining_data, nil
      } else {
        return nil, nil, SerializedValue{}, fmt.Errorf("unknown pointer flags: %d", pointer_flags)
      }
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Struct, NewSerializedType("struct"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    // TODO: Switch from serializing each field as a []byte of a SerializedValue.MarshalBinary to just running serializeValue and adding the TypeStack and Data together
    serialized_value := SerializedValue{
      []uint64{ctx_type},
      nil,
    }
    num_fields := 0
    field_values := map[int]SerializedValue{}
    for _, field := range(reflect.VisibleFields(reflect_type)) {
      gv_tag, tagged_gv := field.Tag.Lookup("gv")
      if tagged_gv == false {
        continue
      } else if gv_tag == "" {
        continue
      } else {
        // Add to the type stack and data stack
        field_index, err := strconv.Atoi(gv_tag)
        if err != nil {
          return SerializedValue{}, err
        }
        num_fields += 1
        if value == nil {
          field_ser, err := serializeValue(ctx, field.Type, nil)
          if err != nil {
            return SerializedValue{}, err
          }
          field_values[field_index] = field_ser
        } else {
          field_value := value.FieldByIndex(field.Index)
          field_ser, err := serializeValue(ctx, field.Type, &field_value)
          if err != nil {
            return SerializedValue{}, err
          }
          field_values[field_index] = field_ser
        }
      }
    }

    for i := 0; i < num_fields; i += 1 {
      field_value, exists := field_values[i]
      if exists == false {
        return SerializedValue{}, fmt.Errorf("%+v missing gv:%d", reflect_type, i)
      }
      serialized_value.TypeStack = append(serialized_value.TypeStack, field_value.TypeStack...)
      if value != nil {
        serialized_value.Data = append(serialized_value.Data, field_value.Data...)
      }
    }

    return serialized_value, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return nil, nil, SerializedValue{}, fmt.Errorf("deserialize struct not implemented")
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int, NewSerializedType("int"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Int()))
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(0), nil, value, nil
    }
    if len(value.Data) < 8 {
      return nil, nil, SerializedValue{}, fmt.Errorf("invalid length: %d/8", len(value.Data))
    }
    int_val := reflect.ValueOf(int(binary.BigEndian.Uint64(value.Data[0:8])))
    value.Data = value.Data[8:]
    return int_val.Type(), &int_val, value, nil 
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Bool, NewSerializedType("bool"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      b := value.Bool()
      if b == true {
        data = []byte{0x01}
      } else {
        data = []byte{0x00}
      }
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(true), nil, value, nil
    } else if len(value.Data) == 0 {
      return nil, nil, SerializedValue{}, fmt.Errorf("not enough data to deserialize bool")
    } else {
      b := value.Data[0]
      value.Data = value.Data[1:]
      var val reflect.Value
      switch b {
      case 0x00:
        val = reflect.ValueOf(false)
      case 0x01:
        val = reflect.ValueOf(true)
      default:
        return nil, nil, SerializedValue{}, fmt.Errorf("unknown boolean 0x%x", b)
      }
      return reflect.TypeOf(true), &val, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Float64, NewSerializedType("float64"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      // TODO: fix if underlying memory layout of float32 changes(or if it's architecture-dependent)
      data = make([]byte, 8)
      val := math.Float64bits(float64(value.Float()))
      binary.BigEndian.PutUint64(data, val) 
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(float64(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize float32")
      }
      val_int := binary.BigEndian.Uint64(value.Data[0:8])
      value.Data = value.Data[8:]
      val := math.Float64frombits(val_int)

      float_val := reflect.ValueOf(val)

      return float_val.Type(), &float_val, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Float32, NewSerializedType("float32"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      // TODO: fix if underlying memory layout of float32 changes(or if it's architecture-dependent)
      data = make([]byte, 4)
      val := math.Float32bits(float32(value.Float()))
      binary.BigEndian.PutUint32(data, val) 
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(float32(0)), nil, value, nil
    } else {
      if len(value.Data) < 4 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize float32")
      }
      val_int := binary.BigEndian.Uint32(value.Data[0:4])
      value.Data = value.Data[4:]
      val := math.Float32frombits(val_int)

      float_value := reflect.ValueOf(val)

      return float_value.Type(), &float_value, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint32, NewSerializedType("uint32"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    data := make([]byte, 4)
    if value != nil {
      binary.BigEndian.PutUint32(data, uint32(value.Uint()))
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(uint32(0)), nil, value, nil
    } else {
      if len(value.Data) < 4 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint32")
      }
      val := binary.BigEndian.Uint32(value.Data[0:4])
      value.Data = value.Data[4:]
      int_value := reflect.ValueOf(val)

      return int_value.Type(), &int_value, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.String, NewSerializedType("string"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
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
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(""), nil, value, nil
    } else if len(value.Data) < 8 {
      return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize string")
    } else {
      str_len := binary.BigEndian.Uint64(value.Data[0:8])
      value.Data = value.Data[8:]
      if len(value.Data) < int(str_len) {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize string of length %d(%d)", str_len, len(value.Data))
      }
      string_bytes := value.Data[:str_len]
      value.Data = value.Data[str_len:]
      str_value := reflect.ValueOf(string(string_bytes))
      return reflect.TypeOf(""), &str_value, value, nil
    }
  })
  if err != nil {
    return nil, err
  }


  err = ctx.RegisterKind(reflect.Array, NewSerializedType("array"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    if value == nil {
      data = nil
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      data := make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Len()))

      var type_stack []uint64 = nil
      for i := 0; i < value.Len(); i += 1 {
        val := value.Index(i)
        element, err := serializeValue(ctx, reflect_type.Elem(), &val)
        if err != nil {
          return SerializedValue{}, err
        }
        if type_stack == nil {
          type_stack = append([]uint64{ctx_type}, element.TypeStack...)
        }
        data = append(data, element.Data...)
      }
    }

    elem, err := serializeValue(ctx, reflect_type.Elem(), nil)
    if err != nil {
      return SerializedValue{}, err
    }

    return SerializedValue{
      append([]uint64{ctx_type}, elem.TypeStack...),
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return nil, nil, SerializedValue{}, fmt.Errorf("deserialize array unimplemented")
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Interface, NewSerializedType("interface"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    type_stack := []uint64{}
    if value == nil {
      data = nil
    } else if value.IsZero() {
      return SerializedValue{}, fmt.Errorf("Cannot serialize nil interfaces")
    } else {
      elem_value := value.Elem()
      elem, err := serializeValue(ctx, value.Elem().Type(), &elem_value)
      if err != nil {
        return SerializedValue{}, err
      }
      data = elem.Data
      type_stack = elem.TypeStack
    }
    return SerializedValue{
      append([]uint64{ctx_type}, type_stack...),
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return nil, nil, SerializedValue{}, fmt.Errorf("deserialize interface unimplemented")
  })
  if err != nil {
    return nil, err
  }


  err = ctx.RegisterKind(reflect.Map, NewSerializedType("map"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
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

        key, err := serializeValue(ctx, reflect_type.Key(), &key_value)
        if err != nil {
          return SerializedValue{}, err
        }
        val, err := serializeValue(ctx, reflect_type.Elem(), &val_value)
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
    key, err := serializeValue(ctx, reflect_type.Key(), nil)
    if err != nil {
      return SerializedValue{}, err
    }
    elem, err := serializeValue(ctx, reflect_type.Elem(), nil)
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
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      var key_type, elem_type reflect.Type
      key_type, _, value, err = DeserializeValue(ctx, value, 1)
      if err != nil {
        return nil, nil, SerializedValue{}, err
      }
      elem_type, _, value, err = DeserializeValue(ctx, value, 1)
      if err != nil {
        return nil, nil, SerializedValue{}, err
      }
      return reflect.MapOf(key_type, elem_type), nil, value, nil
    } else if len(value.Data) < 8 {
      return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize map")
    } else {
      map_len := binary.BigEndian.Uint64(value.Data[0:8])
      value.Data = value.Data[8:]
      if map_len == 0xFFFFFFFFFFFFFFFF {
        temp_value := SerializedValue{
          value.TypeStack,
          nil,
        }

        var key_type, elem_type reflect.Type
        key_type, _, temp_value, err = DeserializeValue(ctx, temp_value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        elem_type, _, temp_value, err = DeserializeValue(ctx, temp_value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }

        map_type := reflect.MapOf(key_type, elem_type)
        map_value := reflect.New(map_type).Elem()

        return map_type, &map_value, SerializedValue{
          temp_value.TypeStack,
          value.Data,
        }, nil
      } else if map_len == 0x00 {
        temp_value := SerializedValue{
          value.TypeStack,
          nil,
        }

        var key_type, elem_type reflect.Type
        key_type, _, temp_value, err = DeserializeValue(ctx, temp_value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        elem_type, _, temp_value, err = DeserializeValue(ctx, temp_value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }

        map_type := reflect.MapOf(key_type, elem_type)
        map_value := reflect.MakeMap(map_type)

        return map_type, &map_value, SerializedValue{
          temp_value.TypeStack,
          value.Data,
        }, nil
      } else {
        var key_type, elem_type reflect.Type
        var key_values, elem_values []reflect.Value
        key_type, key_values, value, err = DeserializeValue(ctx, value, int(map_len))
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        elem_type, elem_values, value, err = DeserializeValue(ctx, value, int(map_len))
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }

        map_type := reflect.MapOf(key_type, elem_type)
        map_value := reflect.MakeMap(map_type)

        for i := 0; i < int(map_len); i += 1 {
          map_value.SetMapIndex(key_values[i], elem_values[i])
        }

        return map_type, &map_value, value, nil
      }
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int8, NewSerializedType("int8"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = []byte{byte(value.Int())}
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(int8(0)), nil, value, nil
    } else {
      if len(value.Data) < 1 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize int8")
      }
      i := int8(value.Data[0])
      value.Data = value.Data[1:]
      val := reflect.ValueOf(i)
      return val.Type(), &val, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint8, NewSerializedType("uint8"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = []byte{uint8(value.Uint())}
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(uint8(0)), nil, value, nil
    } else {
      if len(value.Data) < 1 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint8")
      }
      i := uint8(value.Data[0])
      value.Data = value.Data[1:]
      val := reflect.ValueOf(i)
      return val.Type(), &val, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint16, NewSerializedType("uint16"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 2)
      binary.BigEndian.PutUint16(data, uint16(value.Uint()))
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(uint16(0)), nil, value, nil
    } else {
      if len(value.Data) < 2 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint16")
      }
      val := binary.BigEndian.Uint16(value.Data[0:2])
      value.Data = value.Data[2:]
      i := reflect.ValueOf(val)

      return i.Type(), &i, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int16, NewSerializedType("int16"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 2)
      binary.BigEndian.PutUint16(data, uint16(value.Int()))
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(int16(0)), nil, value, nil
    } else {
      if len(value.Data) < 2 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint16")
      }
      val := int16(binary.BigEndian.Uint16(value.Data[0:2]))
      value.Data = value.Data[2:]
      i := reflect.ValueOf(val)

      return i.Type(), &i, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int32, NewSerializedType("int32"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 4)
      binary.BigEndian.PutUint32(data, uint32(value.Int()))
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(int32(0)), nil, value, nil
    } else {
      if len(value.Data) < 4 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint16")
      }
      val := int32(binary.BigEndian.Uint32(value.Data[0:4]))
      value.Data = value.Data[4:]
      i := reflect.ValueOf(val)

      return i.Type(), &i, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint, NewSerializedType("uint"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, value.Uint())
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(uint(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint64")
      }
      val := uint(binary.BigEndian.Uint64(value.Data[0:8]))
      value.Data = value.Data[8:]
      i := reflect.ValueOf(val)

      return i.Type(), &i, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Uint64, NewSerializedType("uint64"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, value.Uint())
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(uint64(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint64")
      }
      val := binary.BigEndian.Uint64(value.Data[0:8])
      value.Data = value.Data[8:]
      i := reflect.ValueOf(val)

      return i.Type(), &i, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int64, NewSerializedType("int64"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Int()))
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(int64(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize uint64")
      }
      val := int64(binary.BigEndian.Uint64(value.Data[0:8]))
      value.Data = value.Data[8:]
      i := reflect.ValueOf(val)

      return i.Type(), &i, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Slice, NewSerializedType("slice"),
  func(ctx *Context, ctx_type uint64, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    var type_stack []uint64 = nil
    if value == nil {
      data = nil
    } else if value.IsZero() {
      data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      data := make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Len()))
      for i := 0; i < value.Len(); i += 1 {
        val := value.Index(i)
        element, err := serializeValue(ctx, reflect_type.Elem(), &val)
        if err != nil {
          return SerializedValue{}, err
        }
        if type_stack == nil {
          type_stack = append([]uint64{ctx_type}, element.TypeStack...)
        }
        data = append(data, element.Data...)
      }
      return SerializedValue{
        type_stack,
        data,
      }, nil
    }
    element, err := serializeValue(ctx, reflect_type.Elem(), nil)
    if err != nil {
      return SerializedValue{}, err
    }
    return SerializedValue{
      append([]uint64{ctx_type}, element.TypeStack...),
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      elem_type, _, _, err := DeserializeValue(ctx, value, 1)
      if err != nil {
        return nil, nil, SerializedValue{}, err
      }
      return reflect.SliceOf(elem_type), nil, value, nil
    } else if len(value.Data) < 8 {
      return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize slice")
    } else {
      slice_length := binary.BigEndian.Uint64(value.Data[0:8])
      value.Data = value.Data[8:]
      if slice_length == 0xFFFFFFFFFFFFFFFF {
        elem_type, _, remaining, err := DeserializeValue(ctx, SerializedValue{
          value.TypeStack,
          nil,
        }, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        reflect_type := reflect.SliceOf(elem_type)
        reflect_value := reflect.New(reflect_type).Elem()
        return reflect_type, &reflect_value, SerializedValue{
          remaining.TypeStack,
          value.Data,
        }, nil
      } else if slice_length == 0x00 {
        elem_type, _, remaining, err := DeserializeValue(ctx, value, 1)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        reflect_value := reflect.MakeSlice(reflect.SliceOf(elem_type), 0, 0)
        return reflect_value.Type(), &reflect_value, SerializedValue{
          remaining.TypeStack,
          value.Data,
        }, nil
      } else {
        elem_type, elements, remaining_data, err := DeserializeValue(ctx, value, int(slice_length))
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        reflect_value := reflect.MakeSlice(reflect.SliceOf(elem_type), 0, int(slice_length))
        for i := 0; i < int(slice_length); i += 1 {
          reflect_value = reflect.Append(reflect_value, elements[i])
        }
        return reflect_value.Type(), &reflect_value, remaining_data, nil
      }
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(StringError("")), ErrorType,
  func(ctx *Context, ctx_type uint64, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    if value == nil {
      return SerializedValue{
        []uint64{ctx_type},
        nil,
      }, nil
    }

    data := make([]byte, 8)
    err := value.Interface().(StringError)
    str := string(err)
    binary.BigEndian.PutUint64(data, uint64(len(str)))
    return SerializedValue{
      []uint64{uint64(ctx_type)},
      append(data, []byte(str)...),
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return nil, nil, SerializedValue{}, fmt.Errorf("unimplemented")
  })

  err = ctx.RegisterType(reflect.TypeOf(RandID()), NewSerializedType("NodeID"),
  func(ctx *Context, ctx_type uint64, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var id_ser []byte = nil
    if value != nil {
      var err error = nil
      id_ser, err = value.Interface().(NodeID).MarshalBinary()
      if err != nil {
        return SerializedValue{}, err
      }
    }
    return SerializedValue{
      []uint64{ctx_type},
      id_ser,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return nil, nil, SerializedValue{}, fmt.Errorf("unimplemented")
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(Up), NewSerializedType("SignalDirection"),
  func(ctx *Context, ctx_type uint64, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var data []byte = nil
    if value != nil {
      val := value.Interface().(SignalDirection)
      data = []byte{byte(val)}
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return reflect.TypeOf(Up), nil, SerializedValue{}, fmt.Errorf("unimplemented")
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(ReqState(0)), NewSerializedType("ReqState"),
  func(ctx *Context, ctx_type uint64, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var data []byte = nil
    if value != nil {
      val := value.Interface().(ReqState)
      data = []byte{byte(val)}
    }
    return SerializedValue{
      []uint64{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return reflect.TypeOf(ReqState(0)), nil, SerializedValue{}, fmt.Errorf("unimplemented")
  })
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
