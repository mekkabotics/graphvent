package graphvent

import (
  "crypto/sha512"
  "encoding/binary"
  "fmt"
  "reflect"
  "sort"
)

const (
  TagBase            = "GraphventTag"
  ExtTypeBase        = "ExtType"
  NodeTypeBase       = "NodeType"
  SignalTypeBase     = "SignalType"
  PolicyTypeBase     = "PolicyType"
  SerializedTypeBase = "SerializedType"
  FieldNameBase      = "FieldName"
)

func Hash(base string, name string) SerializedType {
  digest := append([]byte(base), 0x00)
  digest = append(digest, []byte(name)...)
  hash := sha512.Sum512(digest)
  return SerializedType(binary.BigEndian.Uint64(hash[0:8]))
}

type SerializedType uint64

func (t SerializedType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type ExtType SerializedType

func (t ExtType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type NodeType SerializedType

func (t NodeType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type SignalType SerializedType

func (t SignalType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type PolicyType SerializedType

func (t PolicyType) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type TypeSerialize func(*Context, SerializedType, reflect.Type, *reflect.Value) (SerializedValue, error)
type TypeDeserialize func(*Context, SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error)

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

  GQLNodeType = NewNodeType("GQL")

  StopSignalType       = NewSignalType("STOP")
  CreateSignalType     = NewSignalType("CREATE")
  StartSignalType      = NewSignalType("START")
  StatusSignalType     = NewSignalType("STATUS")
  LinkSignalType       = NewSignalType("LINK")
  LockSignalType       = NewSignalType("LOCK")
  ReadSignalType       = NewSignalType("READ")
  ACLTimeoutSignalType = NewSignalType("ACL_TIMEOUT")
  ErrorSignalType      = NewSignalType("ERROR")
  SuccessSignalType    = NewSignalType("SUCCESS")
  ReadResultSignalType = NewSignalType("READ_RESULT")

  MemberOfPolicyType      = NewPolicyType("USER_OF")
  RequirementOfPolicyType = NewPolicyType("REQUIEMENT_OF")
  PerNodePolicyType       = NewPolicyType("PER_NODE")
  AllNodesPolicyType      = NewPolicyType("ALL_NODES")

  ErrorType     = NewSerializedType("ERROR")
  PointerType   = NewSerializedType("POINTER")
  SliceType     = NewSerializedType("SLICE")
  StructType    = NewSerializedType("STRUCT")
  IntType       = NewSerializedType("INT")
  UIntType      = NewSerializedType("UINT")
  BoolType      = NewSerializedType("BOOL")
  Float64Type   = NewSerializedType("FLOAT64")
  Float32Type   = NewSerializedType("FLOAT32")
  UInt8Type     = NewSerializedType("UINT8")
  UInt16Type    = NewSerializedType("UINT16")
  UInt32Type    = NewSerializedType("UINT32")
  UInt64Type    = NewSerializedType("UINT64")
  Int8Type      = NewSerializedType("INT8")
  Int16Type     = NewSerializedType("INT16")
  Int32Type     = NewSerializedType("INT32")
  Int64Type     = NewSerializedType("INT64")
  StringType    = NewSerializedType("STRING")
  ArrayType     = NewSerializedType("ARRAY")
  InterfaceType = NewSerializedType("INTERFACE")
  MapType       = NewSerializedType("MAP")

  ReqStateType         = NewSerializedType("REQ_STATE")
  SignalDirectionType  = NewSerializedType("SIGNAL_DIRECTION")
  NodeStructType       = NewSerializedType("NODE_STRUCT")
  QueuedSignalType     = NewSerializedType("QUEUED_SIGNAL")
  NodeTypeSerialized   = NewSerializedType("NODE_TYPE")
  ExtTypeSerialized    = NewSerializedType("EXT_TYPE")
  PolicyTypeSerialized = NewSerializedType("POLICY_TYPE")
  ExtSerialized        = NewSerializedType("EXTENSION")
  PolicySerialized     = NewSerializedType("POLICY")
  SignalSerialized     = NewSerializedType("SIGNAL")
  NodeIDType           = NewSerializedType("NODE_ID")
  UUIDType             = NewSerializedType("UUID")
  PendingACLType       = NewSerializedType("PENDING_ACL")
  PendingSignalType    = NewSerializedType("PENDING_SIGNAL")
  TimeType             = NewSerializedType("TIME")
  ResultType           = NewSerializedType("RESULT")
  TreeType             = NewSerializedType("TREE")
  SerializedTypeSerialized   = NewSerializedType("SERIALIZED_TYPE")
)

func SerializeArray(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
  type_stack := []SerializedType{ctx_type}
  if value == nil {
    return SerializedValue{
      type_stack,
      nil,
    }, nil
  } else if value.IsZero() {
    return SerializedValue{}, fmt.Errorf("don't know what zero array means...")
  } else {
    var element SerializedValue
    var err error
    var data []byte
    for i := 0; i < value.Len(); i += 1 {
      val := value.Index(i)
      element, err = SerializeValue(ctx, reflect_type.Elem(), &val)
      if err != nil {
        return SerializedValue{}, err
      }
      data = append(data, element.Data...)
    }
    return SerializedValue{
      type_stack,
      data,
    }, nil
  }
}

func DeserializeArray[T any](ctx *Context) func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
  var zero T
  array_type := reflect.TypeOf(zero)
  array_size := array_type.Len()
  zero_value, err := SerializeValue(ctx, array_type.Elem(), nil)
  if err != nil {
    panic(err)
  }
  saved_type_stack := zero_value.TypeStack
  return func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    if value.Data == nil {
      return array_type, nil, value, nil
    } else {
      array_value := reflect.New(array_type).Elem()
      for i := 0; i < array_size; i += 1 {
        var element_value *reflect.Value
        var err error
        tmp_value := SerializedValue{
          saved_type_stack,
          value.Data,
        }
        _, element_value, tmp_value, err = DeserializeValue(ctx, tmp_value)
        if err != nil {
          return nil, nil, value, err
        }
        value.Data = tmp_value.Data
        array_elem := array_value.Index(i)
        array_elem.Set(*element_value)
      }
      return array_type, &array_value, value, nil
    }
  }
}

func SerializeUintN(size int) func(*Context, SerializedType, reflect.Type, *reflect.Value) (SerializedValue, error) {
  var fill_data func([]byte, uint64) = nil
  switch size {
  case 1:
    fill_data = func(data []byte, val uint64) {
      data[0] = byte(val)
    }
  case 2:
    fill_data = func(data []byte, val uint64) {
      binary.BigEndian.PutUint16(data, uint16(val))
    }
  case 4:
    fill_data = func(data []byte, val uint64) {
      binary.BigEndian.PutUint32(data, uint32(val))
    }
  case 8:
    fill_data = func(data []byte, val uint64) {
      binary.BigEndian.PutUint64(data, val)
    }
  default:
    panic(fmt.Sprintf("Cannot serialize uint of size %d", size))
  }
  return func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var data []byte = nil
    if value != nil {
      data = make([]byte, size)
      fill_data(data, value.Uint())
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      data,
    }, nil
  }
}

func DeserializeUintN[T interface {
  ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64
}](size int) func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
  var get_uint func([]byte) uint64
  switch size {
  case 1:
    get_uint = func(data []byte) uint64 {
      return uint64(data[0])
    }
  case 2:
    get_uint = func(data []byte) uint64 {
      return uint64(binary.BigEndian.Uint16(data))
    }
  case 4:
    get_uint = func(data []byte) uint64 {
      return uint64(binary.BigEndian.Uint32(data))
    }
  case 8:
    get_uint = func(data []byte) uint64 {
      return binary.BigEndian.Uint64(data)
    }
  default:
    panic(fmt.Sprintf("Cannot deserialize int of size %d", size))
  }
  var zero T
  uint_type := reflect.TypeOf(zero)
  return func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    if value.Data == nil {
      return uint_type, nil, value, nil
    } else {
      var uint_bytes []byte
      var err error
      uint_bytes, value, err = value.PopData(size)
      if err != nil {
        return nil, nil, value, err
      }
      uint_value := reflect.New(uint_type).Elem()
      uint_value.SetUint(get_uint(uint_bytes))
      return uint_type, &uint_value, value, nil
    }
  }
}

func SerializeIntN(size int) func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
  var fill_data func([]byte, int64) = nil
  switch size {
  case 1:
    fill_data = func(data []byte, val int64) {
      data[0] = byte(val)
    }
  case 2:
    fill_data = func(data []byte, val int64) {
      binary.BigEndian.PutUint16(data, uint16(val))
    }
  case 4:
    fill_data = func(data []byte, val int64) {
      binary.BigEndian.PutUint32(data, uint32(val))
    }
  case 8:
    fill_data = func(data []byte, val int64) {
      binary.BigEndian.PutUint64(data, uint64(val))
    }
  default:
    panic(fmt.Sprintf("Cannot serialize int of size %d", size))
  }
  return func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var data []byte = nil
    if value != nil {
      data = make([]byte, size)
      fill_data(data, value.Int())
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      data,
    }, nil
  }
}

func DeserializeIntN[T interface {
  ~int | ~int8 | ~int16 | ~int32 | ~int64
}](size int) func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
  var get_int func([]byte) int64
  switch size {
  case 1:
    get_int = func(data []byte) int64 {
      return int64(data[0])
    }
  case 2:
    get_int = func(data []byte) int64 {
      return int64(binary.BigEndian.Uint16(data))
    }
  case 4:
    get_int = func(data []byte) int64 {
      return int64(binary.BigEndian.Uint32(data))
    }
  case 8:
    get_int = func(data []byte) int64 {
      return int64(binary.BigEndian.Uint64(data))
    }
  default:
    panic(fmt.Sprintf("Cannot deserialize int of size %d", size))
  }
  var zero T
  int_type := reflect.TypeOf(zero)
  return func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    if value.Data == nil {
      return int_type, nil, value, nil
    } else {
      var int_bytes []byte
      var err error
      int_bytes, value, err = value.PopData(size)
      if err != nil {
        return nil, nil, value, err
      }
      int_value := reflect.New(int_type).Elem()
      int_value.SetInt(get_int(int_bytes))
      return int_type, &int_value, value, nil
    }
  }
}

type FieldInfo struct {
  Index     []int
  TypeStack []SerializedType
}

type StructInfo struct {
  Type               reflect.Type
  FieldOrder         []SerializedType
  FieldMap           map[SerializedType]FieldInfo
  PostDeserialize    bool
  PostDeserializeIdx int
}

type Deserializable interface {
  PostDeserialize(*Context) error
}

var deserializable_zero Deserializable = nil
var DeserializableType = reflect.TypeOf(&deserializable_zero).Elem()

func structInfo(ctx *Context, struct_type reflect.Type) StructInfo {
  field_order := []SerializedType{}
  field_map := map[SerializedType]FieldInfo{}
  for _, field := range reflect.VisibleFields(struct_type) {
    gv_tag, tagged_gv := field.Tag.Lookup("gv")
    if tagged_gv == false {
      continue
    } else {
      field_hash := Hash(FieldNameBase, gv_tag)
      _, exists := field_map[field_hash]
      if exists == true {
        panic(fmt.Sprintf("gv tag %s is repeated", gv_tag))
      } else {
        field_serialized, err := SerializeValue(ctx, field.Type, nil)
        if err != nil {
          panic(err)
        }
        field_map[field_hash] = FieldInfo{
          field.Index,
          field_serialized.TypeStack,
        }
        field_order = append(field_order, field_hash)
      }
    }
  }

  sort.Slice(field_order, func(i, j int) bool {
    return uint64(field_order[i]) < uint64(field_order[j])
  })

  post_deserialize := false
  post_deserialize_idx := 0
  ptr_type := reflect.PointerTo(struct_type)
  if ptr_type.Implements(DeserializableType) {
    post_deserialize = true
    for i := 0; i < ptr_type.NumMethod(); i += 1 {
      method := ptr_type.Method(i)
      if method.Name == "PostDeserialize" {
        post_deserialize_idx = i
        break
      }
    }
  }

  return StructInfo{
    struct_type,
    field_order,
    field_map,
    post_deserialize,
    post_deserialize_idx,
  }
}

func SerializeStruct(ctx *Context, struct_type reflect.Type) func(*Context, SerializedType, reflect.Type, *reflect.Value) (SerializedValue, error) {
  struct_info := structInfo(ctx, struct_type)
  return func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
    type_stack := []SerializedType{ctx_type}
    var data []byte
    if value == nil {
      data = nil
    } else {
      data = make([]byte, 8)
      for _, field_hash := range struct_info.FieldOrder {
        field_hash_bytes := make([]byte, 8)
        binary.BigEndian.PutUint64(field_hash_bytes, uint64(field_hash))
        field_info := struct_info.FieldMap[field_hash]
        field_value := value.FieldByIndex(field_info.Index)
        field_serialized, err := SerializeValue(ctx, field_value.Type(), &field_value)
        if err != nil {
          return SerializedValue{}, err
        }
        data = append(data, field_hash_bytes...)
        data = append(data, field_serialized.Data...)
      }
      binary.BigEndian.PutUint64(data[0:8], uint64(len(struct_info.FieldOrder)))
    }
    return SerializedValue{
      type_stack,
      data,
    }, nil
  }
}

func DeserializeStruct(ctx *Context, struct_type reflect.Type) func(*Context, SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
  struct_info := structInfo(ctx, struct_type)
  return func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    if value.Data == nil {
      return struct_info.Type, nil, value, nil
    } else {
      var num_fields_bytes []byte
      var err error
      num_fields_bytes, value, err = value.PopData(8)
      if err != nil {
        return nil, nil, value, err
      }
      num_fields := int(binary.BigEndian.Uint64(num_fields_bytes))
      ctx.Log.Logf("serialize", "Deserializing %d fields from %+v", num_fields, struct_info)

      struct_value := reflect.New(struct_info.Type).Elem()

      for i := 0; i < num_fields; i += 1 {
        var field_hash_bytes []byte
        field_hash_bytes, value, err = value.PopData(8)
        if err != nil {
          return nil, nil, value, err
        }
        field_hash := SerializedType(binary.BigEndian.Uint64(field_hash_bytes))
        field_info, exists := struct_info.FieldMap[field_hash]
        if exists == false {
          return nil, nil, value, fmt.Errorf("Field 0x%x is not valid for %+v: %d", field_hash, struct_info.Type, i)
        }
        field_value := struct_value.FieldByIndex(field_info.Index)

        tmp_value := SerializedValue{
          field_info.TypeStack,
          value.Data,
        }

        var field_reflect *reflect.Value
        _, field_reflect, tmp_value, err = DeserializeValue(ctx, tmp_value)
        if err != nil {
          return nil, nil, value, err
        }
        value.Data = tmp_value.Data
        field_value.Set(*field_reflect)
      }

      if struct_info.PostDeserialize == true {
        ctx.Log.Logf("serialize", "running post-deserialize for %+v", struct_info.Type)
        post_deserialize_method := struct_value.Addr().Method(struct_info.PostDeserializeIdx)
        ret := post_deserialize_method.Call([]reflect.Value{reflect.ValueOf(ctx)})
        if ret[0].IsZero() == false {
          return nil, nil, value, ret[0].Interface().(error)
        }
      }

      return struct_info.Type, &struct_value, value, err
    }
  }
}

func SerializeInterface(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
  var data []byte
  type_stack := []SerializedType{ctx_type}
  if value == nil {
    data = nil
  } else if value.IsZero() {
    data = []byte{0x01}
  } else {
    data = []byte{0x00}
    elem_value := value.Elem()
    elem, err := SerializeValue(ctx, elem_value.Type(), &elem_value)
    if err != nil {
      return SerializedValue{}, err
    }
    elem_data, err := elem.MarshalBinary()
    if err != nil {
      return SerializedValue{}, err
    }
    data = append(data, elem_data...)
  }
  return SerializedValue{
    type_stack,
    data,
  }, nil
}

func DeserializeInterface[T any]() func(*Context, SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
  return func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    var interface_zero T
    var interface_type = reflect.ValueOf(&interface_zero).Type().Elem()
    if value.Data == nil {
      return interface_type, nil, value, nil
    } else {
      var flag_bytes []byte
      var err error
      flag_bytes, value, err = value.PopData(1)
      if err != nil {
        return nil, nil, value, err
      }

      interface_value := reflect.New(interface_type).Elem()
      nil_flag := flag_bytes[0]
      if nil_flag == 0x01 {
      } else if nil_flag == 0x00 {
        var elem_value *reflect.Value
        var elem_ser SerializedValue
        elem_ser, value.Data, err = ParseSerializedValue(value.Data)
        _, elem_value, _, err = DeserializeValue(ctx, elem_ser)
        if err != nil {
          return nil, nil, value, err
        }
        interface_value.Set(*elem_value)
      } else {
        return nil, nil, value, fmt.Errorf("Unknown interface nil_flag value 0x%x", nil_flag)
      }
      return interface_type, &interface_value, value, nil
    }
  }
}

type SerializedValue struct {
  TypeStack []SerializedType
  Data      []byte
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

  serialized_value, err := serialize(ctx, ctx_type, t, value)
  if err != nil {
    return serialized_value, err
  }
  ctx.Log.Logf("serialize", "Serialized %+v: %+v", ctx_name, serialized_value)
  return serialized_value, err
}

func ExtField(ctx *Context, ext Extension, field_name string) (reflect.Value, error) {
  if ext == nil {
    return reflect.Value{}, fmt.Errorf("Cannot get fields on nil Extension")
  }

  ext_value := reflect.ValueOf(ext).Elem()
  for _, field := range reflect.VisibleFields(ext_value.Type()) {
    gv_tag, tagged := field.Tag.Lookup("gv")
    if tagged == true && gv_tag == field_name {
      return ext_value.FieldByIndex(field.Index), nil
    }
  }

  return reflect.Value{}, fmt.Errorf("%s is not a field in %+v", field_name, reflect.TypeOf(ext))
}

func SerializeField(ctx *Context, ext Extension, field_name string) (SerializedValue, error) {
  field_value, err := ExtField(ctx, ext, field_name)
  if err != nil {
    return SerializedValue{}, err
  }

  return SerializeValue(ctx, field_value.Type(), &field_value)
}

func (value SerializedValue) MarshalBinary() ([]byte, error) {
  data := make([]byte, value.SerializedSize())
  binary.BigEndian.PutUint64(data[0:8], uint64(len(value.TypeStack)))
  binary.BigEndian.PutUint64(data[8:16], uint64(len(value.Data)))

  for i, t := range value.TypeStack {
    type_start := (i + 2) * 8
    type_end := (i + 3) * 8
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
    type_start := (i + 2) * 8
    type_end := (i + 3) * 8
    type_stack[i] = SerializedType(binary.BigEndian.Uint64(data[type_start:type_end]))
  }

  types_end := 8 * (num_types + 2)
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

  ctx.Log.Logf("serialize", "Deserializing: %+v(%+v) - %+v", ctx_name, ctx_type, value.TypeStack)

  if value.Data == nil {
    reflect_type, _, value, err = deserialize(ctx, value)
  } else {
    reflect_type, reflect_value, value, err = deserialize(ctx, value)
  }
  if err != nil {
    return nil, nil, value, err
  }

  if reflect_value != nil {
    ctx.Log.Logf("serialize", "Deserialized %+v - %+v - %+v", reflect_type, reflect_value.Interface(), err)
  } else {
    ctx.Log.Logf("serialize", "Deserialized %+v - %+v - %+v", reflect_type, reflect_value, err)
  }
  return reflect_type, reflect_value, value, nil
}
