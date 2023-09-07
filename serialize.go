package graphvent

import (
  "crypto/sha512"
  "encoding/binary"
  "fmt"
  "reflect"
)

const (
  TagBase = "GraphventTag"
  ExtTypeBase = "ExtType"
  NodeTypeBase = "NodeType"
  SignalTypeBase = "SignalType"
  PolicyTypeBase = "PolicyType"
  SerializedTypeBase = "SerializedType"
  FieldNameBase = "FieldName"
)

func Hash(base string, name string) SerializedType {
  digest := append([]byte(base), 0x00)
  digest = append(digest, []byte(name)...)
  hash := sha512.Sum512(digest)
  return SerializedType(binary.BigEndian.Uint64(hash[0:8]))
}

type SerializedType uint64
type ExtType SerializedType
type NodeType SerializedType
type SignalType SerializedType
type PolicyType SerializedType

type TypeSerialize func(*Context,SerializedType,reflect.Type,*reflect.Value) (SerializedValue, error)
type TypeDeserialize func(*Context,SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error)

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
  return Hash(SerializedTypeBase, name)
}

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
  PointerType = NewSerializedType("POINTER")
  SliceType = NewSerializedType("SLICE")
  StructType = NewSerializedType("STRUCT")
  IntType = NewSerializedType("INT")
  UIntType = NewSerializedType("UINT")
  BoolType = NewSerializedType("BOOL")
  Float64Type = NewSerializedType("FLOAT64")
  Float32Type = NewSerializedType("FLOAT32")
  UInt8Type = NewSerializedType("UINT8")
  UInt16Type = NewSerializedType("UINT16")
  UInt32Type = NewSerializedType("UINT32")
  UInt64Type = NewSerializedType("UINT64")
  Int8Type = NewSerializedType("INT8")
  Int16Type = NewSerializedType("INT16")
  Int32Type = NewSerializedType("INT32")
  Int64Type = NewSerializedType("INT64")
  StringType = NewSerializedType("STRING")
  ArrayType = NewSerializedType("ARRAY")
  InterfaceType = NewSerializedType("INTERFACE")
  MapType = NewSerializedType("MAP")

  ReqStateType = NewSerializedType("REQ_STATE")
  SignalDirectionType = NewSerializedType("SIGNAL_DIRECTION")
  NodeIDType = NewSerializedType("NODE_ID")
)

type SerializedValue struct {
  TypeStack []SerializedType
  Data []byte
}

func (value SerializedValue) PopType() (SerializedType, SerializedValue, error) {
  if len(value.TypeStack) == 0 {
    return SerializedType(0), value, fmt.Errorf("No elements in TypeStack")
  }
  ctx_type := value.TypeStack[0]
  value.TypeStack = value.TypeStack[1:]
  return ctx_type, value, nil
}

func (value SerializedValue) PopData(n int) ([]byte, SerializedValue, error) {
  if len(value.Data) < n {
    return nil, value, fmt.Errorf("Not enough data %d/%d", len(value.Data), n)
  }
  data := value.Data[0:n]
  value.Data = value.Data[n:]

  return data, value, nil
}

func SerializeAny[T any](ctx *Context, value T) (SerializedValue, error) {
  reflect_value := reflect.ValueOf(value)
  return SerializeValue(ctx, reflect_value.Type(), &reflect_value)
}

func SerializeValue(ctx *Context, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
  ctx.Log.Logf("serialize", "Serializing: %+v - %+v", t, value)
  type_info, type_exists := ctx.TypeReflects[t]
  var ctx_type SerializedType
  var ctx_name string
  var serialize TypeSerialize = nil
  if type_exists == true {
    ctx_type = type_info.Type
    ctx_name = type_info.Reflect.Name()
    if type_info.Serialize != nil {
      serialize = type_info.Serialize
    }
  }

  kind := t.Kind()
  kind_info, handled := ctx.Kinds[kind]
  if handled == false {
    return SerializedValue{}, fmt.Errorf("Don't know how to serialize kind %+v", kind)
  } else if type_exists == false {
    ctx_type = kind_info.Type
    ctx_name = kind_info.Reflect.String()
  }

  if serialize == nil {
    serialize = kind_info.Serialize
  }

  serialized_value, err :=  serialize(ctx, ctx_type, t, value)
  if err != nil {
    return serialized_value, err
  }
  ctx.Log.Logf("serialize", "Serialized %+v: %+v", ctx_name, serialized_value)
  return serialized_value, err
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
    return SerializeValue(ctx, field.Type(), &field)
  }
}

func (value SerializedValue) MarshalBinary() ([]byte, error) {
  data := make([]byte, value.SerializedSize())
  binary.BigEndian.PutUint64(data[0:8], uint64(len(value.TypeStack)))
  binary.BigEndian.PutUint64(data[8:16], uint64(len(value.Data)))

  for i, t := range(value.TypeStack) {
    type_start := (i+2)*8
    type_end := (i+3)*8
    binary.BigEndian.PutUint64(data[type_start:type_end], uint64(t))
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
  type_stack := make([]SerializedType, num_types)
  for i := 0; i < num_types; i += 1 {
    type_start := (i+2) * 8
    type_end := (i+3) * 8
    type_stack[i] = SerializedType(binary.BigEndian.Uint64(data[type_start:type_end]))
  }

  types_end := 8*(num_types + 2)
  data_end := types_end + data_size
  return SerializedValue{
    type_stack,
    data[types_end:data_end],
  }, data[data_end:], nil
}

func DeserializeValue(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {

  var deserialize TypeDeserialize = nil
  var reflect_type reflect.Type = nil
  var reflect_value *reflect.Value = nil

  ctx_type, value, err := value.PopType()
  if err != nil {
    return nil, nil, value, err
  }

  var ctx_name string

  type_info, type_exists := ctx.Types[SerializedType(ctx_type)]
  if type_exists == true {
    deserialize = type_info.Deserialize
    ctx_name = type_info.Reflect.Name()
  } else {
    kind_info, exists := ctx.KindTypes[SerializedType(ctx_type)]
    if exists == false {
      return nil, nil, value, fmt.Errorf("Cannot deserialize 0x%x: unknown type/kind", ctx_type)
    }
    deserialize = kind_info.Deserialize
    ctx_name = kind_info.Reflect.String()
  }

  ctx.Log.Logf("serialize", "Deserializing: %+v(0x%d) - %+v", ctx_name, ctx_type, value.TypeStack)

  if value.Data == nil {
    reflect_type, _, value, err = deserialize(ctx, value)
  } else {
    reflect_type, reflect_value, value, err = deserialize(ctx, value)
  }
  if err != nil {
    return nil, nil, value, err
  }

  ctx.Log.Logf("serialize", "Deserialized %+v - %+v - %+v", reflect_type, reflect_value, err)
  return reflect_type, reflect_value, value, nil
}
