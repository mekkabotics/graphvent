package graphvent

import (
  "crypto/sha512"
  "encoding"
  "encoding/binary"
  "encoding/gob"
  "fmt"
  "math"
  "reflect"
  "sort"
  "bytes"
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

type FieldTag SerializedType

func (t FieldTag) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

type Chunk struct {
  Data []byte
  Next *Chunk
}

type Chunks struct {
  First *Chunk
  Last *Chunk
}

func (chunks Chunks) String() string {
  cur := chunks.First
  str := fmt.Sprintf("Chunks(")
  for cur != nil {
    str = fmt.Sprintf("%s%+v, ", str, cur)
    cur = cur.Next
  }

  return fmt.Sprintf("%s)", str)
}

func NewChunks(datas ...[]byte) Chunks {
  var first *Chunk = nil
  var last *Chunk = nil

  if len(datas) >= 1 {
    first = &Chunk{
      Data: datas[0],
      Next: nil,
    }
    last = first

    for _, data := range(datas[1:]) {
      last.Next = &Chunk{
        Data: data,
        Next: nil,
      }
      last = last.Next
    }
  }

  if (first == nil || last == nil) && (first != last) {
    panic(fmt.Sprintf("Attempted to construct invalid Chunks with NewChunks %+v - %+v", first, last))
  }
  return Chunks{
    First: first,
    Last: last,
  }
}

func (chunks Chunks) AddDataToEnd(datas ...[]byte) Chunks {
  if chunks.First == nil && chunks.Last == nil {
    return NewChunks(datas...)
  } else if chunks.First == nil || chunks.Last == nil {
    panic(fmt.Sprintf("Invalid chunks %+v", chunks))
  }

  for _, data := range(datas) {
    chunks.Last.Next = &Chunk{
      Data: data,
      Next: nil,
    }
    chunks.Last = chunks.Last.Next
  }

  return chunks
}

func (chunks Chunks) AddChunksToEnd(new_chunks Chunks) Chunks {
  if chunks.Last == nil && chunks.First == nil {
    return new_chunks
  } else if chunks.Last == nil || chunks.First == nil {
    panic(fmt.Sprintf("Invalid chunks %+v", chunks))
  } else if new_chunks.Last == nil && new_chunks.First == nil {
    return chunks
  } else if new_chunks.Last == nil || new_chunks.First == nil {
    panic(fmt.Sprintf("Invalid new_chunks %+v", new_chunks))
  } else {
    chunks.Last.Next = new_chunks.First
    chunks.Last = new_chunks.Last
    return chunks
  }
}

func (chunks Chunks) GetSerializedSize() int {
  total_size := 0
  cur := chunks.First

  for cur != nil {
    total_size += len(cur.Data)
    cur = cur.Next
  }
  return total_size
}

func (chunks Chunks) Slice() []byte {
  total_size := chunks.GetSerializedSize()
  data := make([]byte, total_size)
  data_ptr := 0

  cur := chunks.First
  for cur != nil {
    copy(data[data_ptr:], cur.Data)
    data_ptr += len(cur.Data)
    cur = cur.Next
  }

  return data
}

type TypeSerializeFn func(*Context, reflect.Type) ([]SerializedType, error)
type SerializeFn func(*Context, reflect.Value) (Chunks, error)
type TypeDeserializeFn func(*Context, []SerializedType) (reflect.Type, []SerializedType, error)
type DeserializeFn func(*Context, reflect.Type, []byte) (reflect.Value, []byte, error)

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

func NewFieldTag(tag_string string) FieldTag {
  return FieldTag(Hash(FieldNameBase, tag_string))
}

func NewSerializedType(name string) SerializedType {
  return Hash(SerializedTypeBase, name)
}

var (
  ListenerExtType = NewExtType("LISTENER")
  LockableExtType = NewExtType("LOCKABLE")
  GQLExtType      = NewExtType("GQL")
  GroupExtType    = NewExtType("GROUP")
  ACLExtType      = NewExtType("ACL")
  EventExtType    = NewExtType("EVENT")

  GQLNodeType   = NewNodeType("GQL")
  BaseNodeType  = NewNodeType("BASE")
  GroupNodeType = NewNodeType("GROUP")

  StopSignalType           = NewSignalType("STOP")
  CreateSignalType         = NewSignalType("CREATE")
  StartSignalType          = NewSignalType("START")
  StatusSignalType         = NewSignalType("STATUS")
  LinkSignalType           = NewSignalType("LINK")
  LockSignalType           = NewSignalType("LOCK")
  TimeoutSignalType        = NewSignalType("TIMEOUT")
  ReadSignalType           = NewSignalType("READ")
  ACLTimeoutSignalType     = NewSignalType("ACL_TIMEOUT")
  ErrorSignalType          = NewSignalType("ERROR")
  SuccessSignalType        = NewSignalType("SUCCESS")
  ReadResultSignalType     = NewSignalType("READ_RESULT")
  RemoveMemberSignalType   = NewSignalType("REMOVE_MEMBER")
  AddMemberSignalType      = NewSignalType("ADD_MEMBER")
  ACLSignalType            = NewSignalType("ACL")
  AddSubGroupSignalType    = NewSignalType("ADD_SUBGROUP")
  RemoveSubGroupSignalType = NewSignalType("REMOVE_SUBGROUP")
  StoppedSignalType        = NewSignalType("STOPPED")
  EventControlSignalType   = NewSignalType("EVENT_CONTORL")
  EventStateSignalType     = NewSignalType("VEX_MATCH_STATUS")

  MemberOfPolicyType      = NewPolicyType("MEMBER_OF")
  OwnerOfPolicyType       = NewPolicyType("OWNER_OF")
  ParentOfPolicyType      = NewPolicyType("PARENT_OF")
  RequirementOfPolicyType = NewPolicyType("REQUIEMENT_OF")
  PerNodePolicyType       = NewPolicyType("PER_NODE")
  AllNodesPolicyType      = NewPolicyType("ALL_NODES")
  ACLProxyPolicyType      = NewPolicyType("ACL_PROXY")

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

  EventStateType       = NewSerializedType("EVENT_STATE")
  WaitReasonType       = NewSerializedType("WAIT_REASON")
  EventCommandType     = NewSerializedType("EVENT_COMMAND")
  ReqStateType         = NewSerializedType("REQ_STATE")
  WaitInfoType         = NewSerializedType("WAIT_INFO")
  SignalDirectionType  = NewSerializedType("SIGNAL_DIRECTION")
  NodeStructType       = NewSerializedType("NODE_STRUCT")
  QueuedSignalType     = NewSerializedType("QUEUED_SIGNAL")
  NodeTypeSerialized   = NewSerializedType("NODE_TYPE")
  ChangesSerialized    = NewSerializedType("CHANGES")
  ExtTypeSerialized    = NewSerializedType("EXT_TYPE")
  PolicyTypeSerialized = NewSerializedType("POLICY_TYPE")
  ExtSerialized        = NewSerializedType("EXTENSION")
  PolicySerialized     = NewSerializedType("POLICY")
  SignalSerialized     = NewSerializedType("SIGNAL")
  NodeIDType           = NewSerializedType("NODE_ID")
  UUIDType             = NewSerializedType("UUID")
  PendingACLType       = NewSerializedType("PENDING_ACL")
  PendingACLSignalType = NewSerializedType("PENDING_ACL_SIGNAL")
  TimeType             = NewSerializedType("TIME")
  DurationType         = NewSerializedType("DURATION")
  ResponseType         = NewSerializedType("RESPONSE")
  StatusType           = NewSerializedType("STATUS")
  TreeType             = NewSerializedType("TREE")
  SerializedTypeSerialized   = NewSerializedType("SERIALIZED_TYPE")
)

type FieldInfo struct {
  Index     []int
  TypeStack []SerializedType
  Type reflect.Type
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

func GetStructInfo(ctx *Context, struct_type reflect.Type) (StructInfo, error) {
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
        return StructInfo{}, fmt.Errorf("gv tag %s is repeated", gv_tag)
      } else {
        field_type_stack, err := SerializeType(ctx, field.Type)
        if err != nil {
          return StructInfo{}, err
        }
        field_map[field_hash] = FieldInfo{
          field.Index,
          field_type_stack,
          field.Type,
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
  }, nil
}

func SerializeStruct(info StructInfo)func(*Context, reflect.Value)(Chunks, error) {
  return func(ctx *Context, value reflect.Value) (Chunks, error) {
    struct_chunks := Chunks{}
    for _, field_hash := range(info.FieldOrder) {
      field_hash_bytes := make([]byte, 8)
      binary.BigEndian.PutUint64(field_hash_bytes, uint64(field_hash))

      field_info := info.FieldMap[field_hash]
      field_value := value.FieldByIndex(field_info.Index)

      field_chunks, err := SerializeValue(ctx, field_value)
      if err != nil {
        return Chunks{}, err
      }

      struct_chunks = struct_chunks.AddDataToEnd(field_hash_bytes).AddChunksToEnd(field_chunks)
      ctx.Log.Logf("serialize", "STRUCT_FIELD_CHUNKS: %+v", field_chunks)
    }
    size_data := make([]byte, 8)
    binary.BigEndian.PutUint64(size_data, uint64(len(info.FieldOrder)))
    return NewChunks(size_data).AddChunksToEnd(struct_chunks), nil
  }
}

func DeserializeStruct(info StructInfo)func(*Context, reflect.Type, []byte)(reflect.Value, []byte, error) {
  return func(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
    if len(data) < 8 {
      return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize struct %d/8", len(data))
    }

    num_field_bytes := data[:8]
    data = data[8:]

    num_fields := binary.BigEndian.Uint64(num_field_bytes)

    struct_value := reflect.New(reflect_type).Elem()
    for i := uint64(0); i < num_fields; i ++ {
      field_hash_bytes := data[:8]
      data = data[8:]
      field_hash := SerializedType(binary.BigEndian.Uint64(field_hash_bytes))
      field_info, exists := info.FieldMap[field_hash]
      if exists == false {
        return reflect.Value{}, nil, fmt.Errorf("%+v is not a field in %+v", field_hash, info.Type)
      }

      var field_value reflect.Value
      var err error
      field_value, data, err = DeserializeValue(ctx, field_info.Type, data)
      if err != nil {
        return reflect.Value{}, nil, err
      }

      field_reflect := struct_value.FieldByIndex(field_info.Index)
      field_reflect.Set(field_value)
    }

    if info.PostDeserialize == true {
      post_deserialize_method := struct_value.Addr().Method(info.PostDeserializeIdx)
      results := post_deserialize_method.Call([]reflect.Value{reflect.ValueOf(ctx)})
      err_if := results[0].Interface()
      if err_if != nil {
        return reflect.Value{}, nil, err_if.(error)
      }
    }

    return struct_value, data, nil
  }
}

func SerializeGob(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 8)
  gob_ser, err := value.Interface().(gob.GobEncoder).GobEncode()
  if err != nil {
    return Chunks{}, err
  }

  binary.BigEndian.PutUint64(data, uint64(len(gob_ser)))
  return NewChunks(data, gob_ser), nil
}

func DeserializeGob[T any, PT interface{gob.GobDecoder; *T}](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 8 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough bytes to deserialize gob %d/8", len(data))
  }

  size_bytes := data[:8]
  size := binary.BigEndian.Uint64(size_bytes)
  gob_data := data[8:8+size]
  data = data[8+size:]

  gob_ptr := reflect.New(reflect_type)
  err := gob_ptr.Interface().(gob.GobDecoder).GobDecode(gob_data)
  if err != nil {
    return reflect.Value{}, nil, err
  }

  return gob_ptr.Elem(), data, nil
}

func SerializeInt8(ctx *Context, value reflect.Value) (Chunks, error) {
  data := []byte{byte(value.Int())}

  return NewChunks(data), nil
}

func SerializeInt16(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 2)
  binary.BigEndian.PutUint16(data, uint16(value.Int()))

  return NewChunks(data), nil
}

func SerializeInt32(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 4)
  binary.BigEndian.PutUint32(data, uint32(value.Int()))

  return NewChunks(data), nil
}

func SerializeInt64(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 8)
  binary.BigEndian.PutUint64(data, uint64(value.Int()))

  return NewChunks(data), nil
}

func SerializeUint8(ctx *Context, value reflect.Value) (Chunks, error) {
  data := []byte{byte(value.Uint())}

  return NewChunks(data), nil
}

func SerializeUint16(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 2)
  binary.BigEndian.PutUint16(data, uint16(value.Uint()))

  return NewChunks(data), nil
}

func SerializeUint32(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 4)
  binary.BigEndian.PutUint32(data, uint32(value.Uint()))

  return NewChunks(data), nil
}

func SerializeUint64(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 8)
  binary.BigEndian.PutUint64(data, value.Uint())

  return NewChunks(data), nil
}

func DeserializeUint64[T ~uint64 | ~int64](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  uint_size := 8
  if len(data) < uint_size {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize uint %d/%d", len(data), uint_size)
  }

  uint_bytes := data[:uint_size]
  data = data[uint_size:]
  uint_value := reflect.New(reflect_type).Elem()

  typed_value := T(binary.BigEndian.Uint64(uint_bytes))
  uint_value.Set(reflect.ValueOf(typed_value))

  return uint_value, data, nil
}

func DeserializeUint32[T ~uint32 | ~uint | ~int32 | ~int](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  uint_size := 4
  if len(data) < uint_size {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize uint %d/%d", len(data), uint_size)
  }

  uint_bytes := data[:uint_size]
  data = data[uint_size:]
  uint_value := reflect.New(reflect_type).Elem()

  typed_value := T(binary.BigEndian.Uint32(uint_bytes))
  uint_value.Set(reflect.ValueOf(typed_value))

  return uint_value, data, nil
}

func DeserializeUint16[T ~uint16 | ~int16](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  uint_size := 2
  if len(data) < uint_size {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize uint %d/%d", len(data), uint_size)
  }

  uint_bytes := data[:uint_size]
  data = data[uint_size:]
  uint_value := reflect.New(reflect_type).Elem()

  typed_value := T(binary.BigEndian.Uint16(uint_bytes))
  uint_value.Set(reflect.ValueOf(typed_value))

  return uint_value, data, nil
}

func DeserializeUint8[T ~uint8 | ~int8](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  uint_size := 1
  if len(data) < uint_size {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize uint %d/%d", len(data), uint_size)
  }

  uint_bytes := data[:uint_size]
  data = data[uint_size:]
  uint_value := reflect.New(reflect_type).Elem()

  typed_value := T(uint_bytes[0])
  uint_value.Set(reflect.ValueOf(typed_value))

  return uint_value, data, nil
}

func SerializeFloat64(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 8)
  float_representation := math.Float64bits(value.Float())
  binary.BigEndian.PutUint64(data, float_representation)
  return NewChunks(data), nil
}

func DeserializeFloat64[T ~float64](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 8 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize float64 %d/8", len(data))
  }

  float_bytes := data[0:8]
  data = data[8:]

  float_representation := binary.BigEndian.Uint64(float_bytes)
  float := math.Float64frombits(float_representation)

  float_value := reflect.New(reflect_type).Elem()
  float_value.Set(reflect.ValueOf(T(float)))

  return float_value, data, nil
}

func SerializeFloat32(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 4)
  float_representation := math.Float32bits(float32(value.Float()))
  binary.BigEndian.PutUint32(data, float_representation)
  return NewChunks(data), nil
}

func DeserializeFloat32[T ~float32](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 4 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize float32 %d/4", len(data))
  }

  float_bytes := data[0:4]
  data = data[4:]

  float_representation := binary.BigEndian.Uint32(float_bytes)
  float := math.Float32frombits(float_representation)

  float_value := reflect.New(reflect_type).Elem()
  float_value.Set(reflect.ValueOf(T(float)))

  return float_value, data, nil
}

func SerializeString(ctx *Context, value reflect.Value) (Chunks, error) {
  data := make([]byte, 8)
  binary.BigEndian.PutUint64(data, uint64(value.Len()))

  return NewChunks(data, []byte(value.String())), nil
}

func DeserializeString[T ~string](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 8 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize string %d/8", len(data))
  }

  size_bytes := data[0:8]
  data = data[8:]

  size := binary.BigEndian.Uint64(size_bytes)
  if len(data) < int(size) {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize string of len %d, %d/%d", size, len(data), size)
  }

  string_value := reflect.New(reflect_type).Elem()
  string_value.Set(reflect.ValueOf(T(string(data[:size]))))
  data = data[size:]

  return string_value, data, nil
}

func SerializeBool(ctx *Context, value reflect.Value) (Chunks, error) {
  if value.Bool() == true {
    return NewChunks([]byte{0xFF}), nil
  } else {
    return NewChunks([]byte{0x00}), nil
  }
}

func DeserializeBool[T ~bool](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 1 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize bool %d/1", len(data))
  }
  byte := data[0]
  data = data[1:]

  bool_value := reflect.New(reflect_type).Elem()
  if byte == 0x00 {
    bool_value.Set(reflect.ValueOf(T(false)))
  } else {
    bool_value.Set(reflect.ValueOf(T(true)))
  }

  return bool_value, data, nil
}

func DeserializeTypePointer(ctx *Context, type_stack []SerializedType) (reflect.Type, []SerializedType, error) {
  elem_type, remaining, err := DeserializeType(ctx, type_stack)
  if err != nil {
    return nil, nil, err
  }

  return reflect.PointerTo(elem_type), remaining, nil
}

func SerializePointer(ctx *Context, value reflect.Value) (Chunks, error) {
  if value.IsZero() {
    return NewChunks([]byte{0x00}), nil
  } else {
    flags := NewChunks([]byte{0x01})

    elem_chunks, err := SerializeValue(ctx, value.Elem())
    if err != nil {
      return Chunks{}, err
    }

    return flags.AddChunksToEnd(elem_chunks), nil
  }
}

func DeserializePointer(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 1 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize pointer %d/1", len(data))
  }

  flags := data[0]
  data = data[1:]

  pointer_value := reflect.New(reflect_type).Elem()

  if flags != 0x00 {
    var element_value reflect.Value
    var err error
    element_value, data, err = DeserializeValue(ctx, reflect_type.Elem(), data)
    if err != nil {
      return reflect.Value{}, nil, err
    }

    pointer_value.Set(element_value.Addr())
  }

  return pointer_value, data, nil
}

func SerializeTypeStub(ctx *Context, reflect_type reflect.Type) ([]SerializedType, error) {
  return nil, nil
}

func DeserializeTypeStub[T any](ctx *Context, type_stack []SerializedType) (reflect.Type, []SerializedType, error) {
  var zero T
  return reflect.TypeOf(zero), type_stack, nil
}

func SerializeTypeElem(ctx *Context, reflect_type reflect.Type) ([]SerializedType, error) {
  return SerializeType(ctx, reflect_type.Elem())
}

func SerializeSlice(ctx *Context, value reflect.Value) (Chunks, error) {
  if value.IsZero() {
    return NewChunks([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}), nil
  } else if value.Len() == 0 {
    return NewChunks([]byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}), nil
  } else {
    slice_chunks := Chunks{}
    for i := 0; i < value.Len(); i += 1 {
      val := value.Index(i)
      element_chunks, err := SerializeValue(ctx, val)
      if err != nil {
        return Chunks{}, err
      }
      slice_chunks = slice_chunks.AddChunksToEnd(element_chunks)
    }

    size_data := make([]byte, 8)
    binary.BigEndian.PutUint64(size_data, uint64(value.Len()))

    return NewChunks(size_data).AddChunksToEnd(slice_chunks), nil
  }
}

func DeserializeTypeSlice(ctx *Context, type_stack []SerializedType) (reflect.Type, []SerializedType, error) {
  elem_type, remaining, err := DeserializeType(ctx, type_stack)
  if err != nil {
    return nil, nil, err
  }

  reflect_type := reflect.SliceOf(elem_type)
  return reflect_type, remaining, nil
}

func DeserializeSlice(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 8 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize slice %d/8", len(data))
  }

  slice_size := binary.BigEndian.Uint64(data[0:8])
  slice_value := reflect.New(reflect_type).Elem()
  data = data[8:]

  if slice_size != 0xFFFFFFFFFFFFFFFF {
    slice_unaddr := reflect.MakeSlice(reflect_type, int(slice_size), int(slice_size))
    slice_value.Set(slice_unaddr)
    for i := uint64(0); i < slice_size; i += 1 {
      var element_value reflect.Value
      var err error
      element_value, data, err = DeserializeValue(ctx, reflect_type.Elem(), data)
      if err != nil {
        return reflect.Value{}, nil, err
      }

      slice_elem := slice_value.Index(int(i))
      slice_elem.Set(element_value)
    }
  }

  return slice_value, data, nil
}

func SerializeTypeMap(ctx *Context, reflect_type reflect.Type) ([]SerializedType, error) {
  key_stack, err := SerializeType(ctx, reflect_type.Key())
  if err != nil {
    return nil, err
  }

  elem_stack, err := SerializeType(ctx, reflect_type.Elem())
  if err != nil {
    return nil, err
  }

  return append(key_stack, elem_stack...), nil
}

func DeserializeTypeMap(ctx *Context, type_stack []SerializedType) (reflect.Type, []SerializedType, error) {
  key_type, after_key, err := DeserializeType(ctx, type_stack)
  if err != nil {
    return nil, nil, err
  }

  elem_type, after_elem, err := DeserializeType(ctx, after_key)
  if err != nil {
    return nil, nil, err
  }

  map_type := reflect.MapOf(key_type, elem_type)
  return map_type, after_elem, nil
}

func SerializeMap(ctx *Context, value reflect.Value) (Chunks, error) {
  if value.IsZero() == true {
    return NewChunks([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}), nil
  }

  map_chunks := []Chunks{}
  map_size := uint64(0)
  map_iter := value.MapRange()
  for map_iter.Next() {
    map_size = map_size + 1
    key := map_iter.Key()
    val := map_iter.Value()

    key_chunks, err := SerializeValue(ctx, key)
    if err != nil {
      return Chunks{}, err
    }

    val_chunks, err := SerializeValue(ctx, val)
    if err != nil {
      return Chunks{}, err
    }

    chunks := key_chunks.AddChunksToEnd(val_chunks)
    map_chunks = append(map_chunks, chunks)
  }

  // Sort map_chunks
  sort.Slice(map_chunks, func(i, j int) bool {
    return bytes.Compare(map_chunks[i].First.Data, map_chunks[j].First.Data) < 0
  })
  chunks := Chunks{}
  for _, chunk := range(map_chunks) {
    chunks = chunks.AddChunksToEnd(chunk)
  }


  size_data := make([]byte, 8)
  binary.BigEndian.PutUint64(size_data, map_size)

  return NewChunks(size_data).AddChunksToEnd(chunks), nil
}

func DeserializeMap(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 8 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize map %d/8", len(data))
  }

  size_bytes := data[:8]
  data = data[8:]

  size := binary.BigEndian.Uint64(size_bytes)

  map_value := reflect.New(reflect_type).Elem()
  if size == 0xFFFFFFFFFFFFFFFF {
    return map_value, data, nil
  }

  map_unaddr := reflect.MakeMapWithSize(reflect_type, int(size))
  map_value.Set(map_unaddr)

  for i := uint64(0); i < size; i++ {
    var err error
    var key_value reflect.Value
    key_value, data, err = DeserializeValue(ctx, reflect_type.Key(), data)
    if err != nil {
      return reflect.Value{}, nil, err
    }

    var val_value reflect.Value
    val_value, data, err = DeserializeValue(ctx, reflect_type.Elem(), data)
    if err != nil {
      return reflect.Value{}, nil, err
    }

    map_value.SetMapIndex(key_value, val_value)
  }

  return map_value, data, nil
}

func SerializeTypeArray(ctx *Context, reflect_type reflect.Type) ([]SerializedType, error) {
  size := SerializedType(reflect_type.Len())
  elem_stack, err := SerializeType(ctx, reflect_type.Elem())
  if err != nil {
    return nil, err
  }

  return append([]SerializedType{size}, elem_stack...), nil
}

func SerializeUUID(ctx *Context, value reflect.Value) (Chunks, error) {
  uuid_ser, err := value.Interface().(encoding.BinaryMarshaler).MarshalBinary()
  if err != nil {
    return Chunks{}, err
  }

  if len(uuid_ser) != 16 {
    return Chunks{}, fmt.Errorf("Wrong length of uuid: %d/16", len(uuid_ser))
  }

  return NewChunks(uuid_ser), nil
}

func DeserializeUUID[T ~[16]byte](ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 16 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize UUID %d/16", len(data))
  }

  uuid_bytes := data[:16]
  data = data[16:]

  uuid_value := reflect.New(reflect_type).Elem()
  uuid_value.Set(reflect.ValueOf(T(uuid_bytes)))

  return uuid_value, data, nil
}

func SerializeArray(ctx *Context, value reflect.Value) (Chunks, error) {
  data := Chunks{}
  for i := 0; i < value.Len(); i += 1 {
    element := value.Index(i)
    element_chunks, err := SerializeValue(ctx, element)
    if err != nil {
      return Chunks{}, err
    }
    data = data.AddChunksToEnd(element_chunks)
  }

  return data, nil
}

func DeserializeTypeArray(ctx *Context, type_stack []SerializedType) (reflect.Type, []SerializedType, error) {
  if len(type_stack) < 1 {
    return nil, nil, fmt.Errorf("Not enough valued in type stack to deserialize array")
  }

  size := int(type_stack[0])
  element_type, remaining, err := DeserializeType(ctx, type_stack[1:])
  if err != nil {
    return nil, nil, err
  }

  array_type := reflect.ArrayOf(size, element_type)
  return array_type, remaining, nil
}

func DeserializeArray(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  array_value := reflect.New(reflect_type).Elem()
  for i := 0; i < array_value.Len(); i += 1 {
    var element_value reflect.Value
    var err error
    element_value, data, err = DeserializeValue(ctx, reflect_type.Elem(), data)
    if err != nil {
      return reflect.Value{}, nil, err
    }

    element := array_value.Index(i)
    element.Set(element_value)
  }

  return array_value, data, nil
}

func SerializeInterface(ctx *Context, value reflect.Value) (Chunks, error) {
  if value.IsZero() == true {
    return NewChunks([]byte{0xFF}), nil
  }

  type_stack, err := SerializeType(ctx, value.Elem().Type())
  if err != nil {
    return Chunks{}, err
  }

  elem_chunks, err := SerializeValue(ctx, value.Elem())
  if err != nil {
    return Chunks{}, err
  }

  data := elem_chunks.Slice()

  serialized_chunks, err := SerializedValue{type_stack, data}.Chunks()
  if err != nil {
    return Chunks{}, err
  }

  return NewChunks([]byte{0x00}).AddChunksToEnd(serialized_chunks), nil
}

func DeserializeInterface(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  if len(data) < 1 {
    return reflect.Value{}, nil, fmt.Errorf("Not enough data to deserialize interface %d/1", len(data))
  }

  flags := data[0]
  data = data[1:]
  if flags == 0xFF {
    return reflect.New(reflect_type).Elem(), data, nil
  }

  serialized_value, remaining, err := ParseSerializedValue(data)
  elem_type, types_remaining, err := DeserializeType(ctx, serialized_value.TypeStack)
  if err != nil {
    return reflect.Value{}, nil, err
  } else if len(types_remaining) > 0 {
    return reflect.Value{}, nil, fmt.Errorf("Types remaining in interface stack after deserializing")
  }

  elem_value, data_remaining, err := DeserializeValue(ctx, elem_type, serialized_value.Data)
  if err != nil {
    return reflect.Value{}, nil, err
  } else if len(data_remaining) > 0 {
    return reflect.Value{}, nil, fmt.Errorf("Data remaining in interface data after deserializing")
  }

  interface_value := reflect.New(reflect_type).Elem()
  interface_value.Set(elem_value)

  return interface_value, remaining, nil
}

type SerializedValue struct {
  TypeStack []SerializedType
  Data      []byte
}

func SerializeAny[T any](ctx *Context, value T) (SerializedValue, error) {
  reflect_value := reflect.ValueOf(value)
  type_stack, err := SerializeType(ctx, reflect_value.Type())
  if err != nil {
    return SerializedValue{}, err
  }
  data, err := SerializeValue(ctx, reflect_value)
  if err != nil {
    return SerializedValue{}, err
  }

  return SerializedValue{type_stack, data.Slice()}, nil
}

func SerializeType(ctx *Context, reflect_type reflect.Type) ([]SerializedType, error) {
  ctx.Log.Logf("serialize", "Serializing type %+v", reflect_type)

  type_info, type_exists := ctx.TypeReflects[reflect_type]
  var serialize_type TypeSerializeFn = nil
  var ctx_type SerializedType
  if type_exists == true {
    serialize_type = type_info.TypeSerialize
    ctx_type = type_info.Type
  }

  if serialize_type == nil {
    kind_info, handled := ctx.Kinds[reflect_type.Kind()]
    if handled == true {
      if type_exists == false {
        ctx_type = kind_info.Type
      }
      serialize_type = kind_info.TypeSerialize
    }
  }

  type_stack := []SerializedType{ctx_type}
  if serialize_type != nil {
    extra_types, err := serialize_type(ctx, reflect_type)
    if err != nil {
      return nil, err
    }
    return append(type_stack, extra_types...), nil
  } else {
    return type_stack, nil
  }
}

func SerializeValue(ctx *Context, value reflect.Value) (Chunks, error) {
  type_info, type_exists := ctx.TypeReflects[value.Type()]
  var serialize SerializeFn = nil
  if type_exists == true {
    if type_info.Serialize != nil {
      serialize = type_info.Serialize
    }
  }

  if serialize == nil {
    kind_info, handled := ctx.Kinds[value.Kind()]
    if handled {
      serialize = kind_info.Serialize
    } else {
      return Chunks{}, fmt.Errorf("Don't know how to serialize %+v", value.Type())
    }
  }

  return serialize(ctx, value)
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
  type_stack, err := SerializeType(ctx, field_value.Type())
  if err != nil {
    return SerializedValue{}, err
  }
  data, err := SerializeValue(ctx, field_value)
  if err != nil {
    return SerializedValue{}, err
  }
  return SerializedValue{type_stack, data.Slice()}, nil
}

func (value SerializedValue) Chunks() (Chunks, error) {
  header_data := make([]byte, 16)
  binary.BigEndian.PutUint64(header_data[0:8], uint64(len(value.TypeStack)))
  binary.BigEndian.PutUint64(header_data[8:16], uint64(len(value.Data)))

  type_stack_bytes := make([][]byte, len(value.TypeStack))
  for i, ctx_type := range(value.TypeStack) {
    type_stack_bytes[i] = make([]byte, 8)
    binary.BigEndian.PutUint64(type_stack_bytes[i], uint64(ctx_type))
  }

  return NewChunks(header_data).AddDataToEnd(type_stack_bytes...).AddDataToEnd(value.Data), nil
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

func DeserializeValue(ctx *Context, reflect_type reflect.Type, data []byte) (reflect.Value, []byte, error) {
  ctx.Log.Logf("serialize", "Deserializing %+v with %d bytes", reflect_type, len(data))
  var deserialize DeserializeFn = nil

  type_info, type_exists := ctx.TypeReflects[reflect_type]
  if type_exists == true {
    deserialize = type_info.Deserialize
  } else {
    kind_info, exists := ctx.Kinds[reflect_type.Kind()]
    if exists == false {
      return reflect.Value{}, nil, fmt.Errorf("Cannot deserialize %+v/%+v: unknown type/kind", reflect_type, reflect_type.Kind())
    }
    deserialize = kind_info.Deserialize
  }

  return deserialize(ctx, reflect_type, data)
}

func DeserializeType(ctx *Context, type_stack []SerializedType) (reflect.Type, []SerializedType, error) {
  ctx.Log.Logf("deserialize_types", "Deserializing type stack %+v", type_stack)
  var deserialize_type TypeDeserializeFn = nil
  var reflect_type reflect.Type = nil

  if len(type_stack) < 1 {
    return nil, nil, fmt.Errorf("No elements in type stack to deserialize(DeserializeType)")
  }

  ctx_type := type_stack[0]
  type_stack = type_stack[1:]

  type_info, type_exists := ctx.Types[SerializedType(ctx_type)]
  if type_exists == true {
    deserialize_type = type_info.TypeDeserialize
    reflect_type = type_info.Reflect
  } else {
    kind_info, exists := ctx.KindTypes[SerializedType(ctx_type)]
    if exists == false {
      return nil, nil, fmt.Errorf("Cannot deserialize 0x%x: unknown type/kind", ctx_type)
    }
    deserialize_type = kind_info.TypeDeserialize
    reflect_type = kind_info.Base
  }

  if deserialize_type == nil {
    return reflect_type, type_stack, nil
  } else {
    return deserialize_type(ctx, type_stack)
  }
}
