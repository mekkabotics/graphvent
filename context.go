package graphvent

import (
  "crypto/ecdh"
  "encoding/binary"
  "errors"
  "fmt"
  "math"
  "reflect"
  "runtime"
  "sync"

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
  Serialize TypeSerialize
  Deserialize TypeDeserialize
}

type KindInfo struct {
  Reflect reflect.Kind
  Type SerializedType
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

  info := KindInfo{
    kind,
    ctx_type,
    serialize,
    deserialize,
  }
  ctx.KindTypes[ctx_type] = &info
  ctx.Kinds[kind] = &info

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

  type_info := TypeInfo{
    Reflect: reflect_type,
    Type: ctx_type,
    Serialize: serialize,
    Deserialize: deserialize,
  }
  ctx.Types[ctx_type] = &type_info
  ctx.TypeReflects[reflect_type] = &type_info

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
  err = ctx.RegisterKind(reflect.Pointer, PointerType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value) (SerializedValue, error) {
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
    elem, err := SerializeValue(ctx, reflect_type.Elem(), elem_value)
    if err != nil {
      return SerializedValue{}, err
    }
    if elem.Data != nil {
      data = append(data, elem.Data...)
    }
    return SerializedValue{
      append([]SerializedType{ctx_type}, elem.TypeStack...),
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue) (reflect.Type, *reflect.Value, SerializedValue, error) {
    if value.Data == nil {
      var elem_type reflect.Type
      var err error
      elem_type, _, value, err = DeserializeValue(ctx, value)
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
        elem_type, elem_value, remaining_data, err := DeserializeValue(ctx, value)
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        pointer_type := reflect.PointerTo(elem_type)
        pointer_value := reflect.New(pointer_type).Elem()
        pointer_value.Set(elem_value.Addr())
        return pointer_type, &pointer_value, remaining_data, nil
      } else if pointer_flags == 0x01 {
        elem_type, _, remaining_data, err := DeserializeValue(ctx, value)
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

  // TODO: figure out why this doesn't break in the simple test, but breaks in TestGQLDB
  err = ctx.RegisterKind(reflect.Struct, StructType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    serialized_value := SerializedValue{
      []SerializedType{ctx_type},
      nil,
    }
    if value != nil {
      serialized_value.Data = make([]byte, 8)
    }

    num_fields := uint64(0)
    for _, field := range(reflect.VisibleFields(reflect_type)) {
      gv_tag, tagged_gv := field.Tag.Lookup("gv")
      if tagged_gv == false {
        continue
      } else if gv_tag == "" {
        continue
      } else {
        num_fields += 1
        field_hash := Hash(FieldNameBase, gv_tag)
        field_hash_bytes := make([]byte, 8)
        binary.BigEndian.PutUint64(field_hash_bytes, uint64(field_hash))
        if value == nil {
          field_ser, err := SerializeValue(ctx, field.Type, nil)
          if err != nil {
            return SerializedValue{}, err
          }
          serialized_value.TypeStack = append(serialized_value.TypeStack, field_ser.TypeStack...)
        } else {
          field_value := value.FieldByIndex(field.Index)
          field_ser, err := SerializeValue(ctx, field.Type, &field_value)
          if err != nil {
            return SerializedValue{}, err
          }
          serialized_value.TypeStack = append(serialized_value.TypeStack, field_ser.TypeStack...)
          serialized_value.Data = append(serialized_value.Data, field_hash_bytes...)
          serialized_value.Data = append(serialized_value.Data, field_ser.Data...)
        }
      }
    }
    if value != nil {
      binary.BigEndian.PutUint64(serialized_value.Data[0:8], num_fields)
    }

    return serialized_value, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(map[uint64]reflect.Value{}), nil, value, nil
    } else {
      var num_fields_data []byte
      var err error
      num_fields_data, value, err = value.PopData(8)
      if err != nil {
        return nil, nil, value, err
      }
      num_fields := int(binary.BigEndian.Uint64(num_fields_data))

      map_type := reflect.TypeOf(map[uint64]reflect.Value{})
      map_ptr := reflect.New(map_type)
      map_ptr.Elem().Set(reflect.MakeMap(map_type))
      map_value := map_ptr.Elem()
      if num_fields == 0 {
        return map_type, &map_value, value, nil
      } else {
        tmp_value := value
        for i := 0; i < num_fields; i += 1 {
          var field_hash_bytes []byte
          field_hash_bytes, tmp_value, err = tmp_value.PopData(8)
          if err != nil {
            return nil, nil, value, err
          }
          field_hash := binary.BigEndian.Uint64(field_hash_bytes)
          field_hash_value := reflect.ValueOf(field_hash)

          var elem_value *reflect.Value
          _, elem_value, tmp_value, err = DeserializeValue(ctx, tmp_value)
          if err != nil {
            return nil, nil, value, err
          }
          map_value.SetMapIndex(field_hash_value, reflect.ValueOf(*elem_value))
        }
        return map_type, &map_value, tmp_value, nil
      }
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int, IntType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Int()))
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Bool, BoolType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
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
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Float64, Float64Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      val := math.Float64bits(float64(value.Float()))
      binary.BigEndian.PutUint64(data, val) 
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Float32, Float32Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 4)
      val := math.Float32bits(float32(value.Float()))
      binary.BigEndian.PutUint32(data, val) 
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Uint32, UInt32Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    data := make([]byte, 4)
    if value != nil {
      binary.BigEndian.PutUint32(data, uint32(value.Uint()))
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.String, StringType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    if value == nil {
      return SerializedValue{
        []SerializedType{ctx_type},
        nil,
      }, nil
    }

    data := make([]byte, 8)
    str := value.String()
    binary.BigEndian.PutUint64(data, uint64(len(str)))
    return SerializedValue{
      []SerializedType{SerializedType(ctx_type)},
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


  err = ctx.RegisterKind(reflect.Array, ArrayType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    if value == nil {
      data = nil
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      data := make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Len()))

      var type_stack []SerializedType = nil
      for i := 0; i < value.Len(); i += 1 {
        val := value.Index(i)
        element, err := SerializeValue(ctx, reflect_type.Elem(), &val)
        if err != nil {
          return SerializedValue{}, err
        }
        if type_stack == nil {
          type_stack = append([]SerializedType{ctx_type}, element.TypeStack...)
        }
        data = append(data, element.Data...)
      }
    }

    elem, err := SerializeValue(ctx, reflect_type.Elem(), nil)
    if err != nil {
      return SerializedValue{}, err
    }

    return SerializedValue{
      append([]SerializedType{ctx_type}, elem.TypeStack...),
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return nil, nil, SerializedValue{}, fmt.Errorf("deserialize array unimplemented")
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Interface, InterfaceType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    type_stack := []SerializedType{ctx_type}
    if value == nil {
      data = nil
    } else if value.IsZero() {
    } else {
      elem_value := value.Elem()
      elem, err := SerializeValue(ctx, elem_value.Type(), &elem_value)
      if err != nil {
        return SerializedValue{}, err
      }
      data, err = elem.MarshalBinary()
      if err != nil {
        return SerializedValue{}, err
      }
    }
    return SerializedValue{
      type_stack,
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf((interface{})(nil)), nil, value, nil
    } else {
      var elem_value *reflect.Value
      var elem_ser SerializedValue
      var elem_type reflect.Type
      var err error
      elem_ser, value.Data, err = ParseSerializedValue(value.Data)
      elem_type, elem_value, _, err = DeserializeValue(ctx, elem_ser)
      if err != nil {
        return nil, nil, value, err
      }
      ptr_type := reflect.PointerTo(elem_type)
      ptr_value := reflect.New(ptr_type).Elem()
      ptr_value.Set(elem_value.Addr())
      return ptr_type, &ptr_value, value, nil
    }
  })
  if err != nil {
    return nil, err
  }


  err = ctx.RegisterKind(reflect.Map, MapType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    type_stack := []SerializedType{ctx_type}
    if value == nil {
      data = nil
    } else if value.IsZero() {
      data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      data = make([]byte, 8)
      map_size := 0
      var key_types, elem_types []SerializedType

      map_iter := value.MapRange()
      for map_iter.Next() {
        map_size += 1
        key_reflect := map_iter.Key()
        elem_reflect := map_iter.Value()

        key_value, err := SerializeValue(ctx, key_reflect.Type(), &key_reflect)
        if err != nil {
          return SerializedValue{}, err
        }
        elem_value, err := SerializeValue(ctx, elem_reflect.Type(), &elem_reflect)
        if err != nil {
          return SerializedValue{}, err
        }

        data = append(data, key_value.Data...)
        data = append(data, elem_value.Data...)

        if key_types == nil {
          key_types = key_value.TypeStack
          elem_types = elem_value.TypeStack
        }
      }

      binary.BigEndian.PutUint64(data[0:8], uint64(map_size))

      type_stack = append(type_stack, key_types...)
      type_stack = append(type_stack, elem_types...)
      return SerializedValue{
        type_stack,
        data,
      }, nil
    }
    key_value, err := SerializeValue(ctx, reflect_type.Key(), nil)
    if err != nil {
      return SerializedValue{}, nil
    }
    elem_value, err := SerializeValue(ctx, reflect_type.Elem(), nil)
    if err != nil {
      return SerializedValue{}, nil
    }

    type_stack = append(type_stack, key_value.TypeStack...)
    type_stack = append(type_stack, elem_value.TypeStack...)

    return SerializedValue{
      type_stack,
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      var key_type, elem_type reflect.Type
      var err error
      key_type, _, value, err = DeserializeValue(ctx, value)
      if err != nil {
        return nil, nil, value, err
      }
      elem_type, _, value, err = DeserializeValue(ctx, value)
      if err != nil {
        return nil, nil, value, err
      }
      reflect_type := reflect.MapOf(key_type, elem_type)
      return reflect_type, nil, value, nil
    } else if len(value.Data) < 8 {
      return nil, nil, value, fmt.Errorf("Not enough data to deserialize map")
    } else {
      var map_size_bytes []byte
      var err error
      map_size_bytes, value, err = value.PopData(8)
      if err != nil {
        return nil, nil, value, err
      }

      map_size := binary.BigEndian.Uint64(map_size_bytes)

      if map_size == 0xFFFFFFFFFFFFFFFF {
        var key_type, elem_type reflect.Type
        var err error
        tmp_value := SerializedValue{
          value.TypeStack,
          nil,
        }
        key_type, _, tmp_value, err = DeserializeValue(ctx, tmp_value)
        if err != nil {
          return nil, nil, value, err
        }
        elem_type, _, tmp_value, err = DeserializeValue(ctx, tmp_value)
        if err != nil {
          return nil, nil, value, err
        }
        new_value := SerializedValue{
          tmp_value.TypeStack,
          value.Data,
        }
        reflect_type := reflect.MapOf(key_type, elem_type)
        reflect_value := reflect.New(reflect_type).Elem()
        return reflect_type, &reflect_value, new_value, nil
      } else if map_size == 0x00 {
        var key_type, elem_type reflect.Type
        var err error
        tmp_value := SerializedValue{
          value.TypeStack,
          nil,
        }
        key_type, _, tmp_value, err = DeserializeValue(ctx, tmp_value)
        if err != nil {
          return nil, nil, value, err
        }
        elem_type, _, tmp_value, err = DeserializeValue(ctx, tmp_value)
        if err != nil {
          return nil, nil, value, err
        }
        new_value := SerializedValue{
          tmp_value.TypeStack,
          value.Data,
        }
        reflect_type := reflect.MapOf(key_type, elem_type)
        reflect_value := reflect.MakeMap(reflect_type)
        return reflect_type, &reflect_value, new_value, nil
      } else {
        tmp_value := value
        var map_value reflect.Value
        var map_type reflect.Type = nil
        for i := 0; i < int(map_size); i += 1 {
          tmp_value.TypeStack = value.TypeStack
          var key_type, elem_type reflect.Type
          var key_value, elem_value *reflect.Value
          var err error
          key_type, key_value, tmp_value, err = DeserializeValue(ctx, tmp_value)
          if err != nil {
            return nil, nil, value, err
          }
          elem_type, elem_value, tmp_value, err = DeserializeValue(ctx, tmp_value)
          if err != nil {
            return nil, nil, value, err
          }
          if map_type == nil {
            map_type = reflect.MapOf(key_type, elem_type)
            map_value = reflect.MakeMap(map_type)
          }
          map_value.SetMapIndex(*key_value, *elem_value)
        }
        return map_type, &map_value, tmp_value, nil
      }
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterKind(reflect.Int8, Int8Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = []byte{byte(value.Int())}
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Uint8, UInt8Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = []byte{uint8(value.Uint())}
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Uint16, UInt16Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 2)
      binary.BigEndian.PutUint16(data, uint16(value.Uint()))
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Int16, Int16Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 2)
      binary.BigEndian.PutUint16(data, uint16(value.Int()))
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Int32, Int32Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 4)
      binary.BigEndian.PutUint32(data, uint32(value.Int()))
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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

  err = ctx.RegisterKind(reflect.Uint, UIntType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, value.Uint())
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(uint(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize SerializedType")
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

  err = ctx.RegisterKind(reflect.Uint64, UInt64Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, value.Uint())
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(SerializedType(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize SerializedType")
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

  err = ctx.RegisterKind(reflect.Int64, Int64Type,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte = nil
    if value != nil {
      data = make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Int()))
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(int64(0)), nil, value, nil
    } else {
      if len(value.Data) < 8 {
        return nil, nil, SerializedValue{}, fmt.Errorf("Not enough data to deserialize SerializedType")
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

  err = ctx.RegisterKind(reflect.Slice, SliceType,
  func(ctx *Context, ctx_type SerializedType, reflect_type reflect.Type, value *reflect.Value)(SerializedValue, error){
    var data []byte
    type_stack := []SerializedType{ctx_type}
    if value == nil {
      data = nil
    } else if value.IsZero() {
      data = []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
    } else if value.Len() == 0 {
      data = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
    } else {
      data := make([]byte, 8)
      binary.BigEndian.PutUint64(data, uint64(value.Len()))
      var element SerializedValue
      var err error
      for i := 0; i < value.Len(); i += 1 {
        val := value.Index(i)
        element, err = SerializeValue(ctx, reflect_type.Elem(), &val)
        if err != nil {
          return SerializedValue{}, err
        }
        data = append(data, element.Data...)
      }
      return SerializedValue{
        append(type_stack, element.TypeStack...),
        data,
      }, nil
    }
    element, err := SerializeValue(ctx, reflect_type.Elem(), nil)
    if err != nil {
      return SerializedValue{}, err
    }
    return SerializedValue{
      append(type_stack, element.TypeStack...),
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      elem_type, _, _, err := DeserializeValue(ctx, value)
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
        })
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
        elem_type, _, remaining, err := DeserializeValue(ctx, SerializedValue{
          value.TypeStack,
          nil,
        })
        if err != nil {
          return nil, nil, SerializedValue{}, err
        }
        reflect_value := reflect.MakeSlice(reflect.SliceOf(elem_type), 0, 0)
        return reflect_value.Type(), &reflect_value, SerializedValue{
          remaining.TypeStack,
          value.Data,
        }, nil
      } else {
        var reflect_value *reflect.Value = nil
        var reflect_type reflect.Type = nil
        saved_type_stack := value.TypeStack
        for i := 0; i < int(slice_length); i += 1 {
          var element_type reflect.Type
          var element_value *reflect.Value
          element_type, element_value, value, err = DeserializeValue(ctx, value)
          if err != nil {
            return nil, nil, value, err
          }
          if reflect_value == nil {
            reflect_type = reflect.SliceOf(element_type)
            real_value := reflect.MakeSlice(reflect_type, int(slice_length), int(slice_length))
            reflect_value = &real_value
          }
          if i != (int(slice_length) - 1) {
            value.TypeStack = saved_type_stack
          }
          slice_index_ptr := reflect_value.Index(i)
          slice_index_ptr.Set(*element_value)
        }
        return reflect_type, reflect_value, value, nil
      }
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(StringError("")), ErrorType, nil, nil)
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(RandID()), NodeIDType,
  func(ctx *Context, ctx_type SerializedType, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var id_ser []byte = nil
    if value != nil {
      var err error = nil
      id_ser, err = value.Interface().(NodeID).MarshalBinary()
      if err != nil {
        return SerializedValue{}, err
      }
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      id_ser,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    if value.Data == nil {
      return reflect.TypeOf(ZeroID), nil, value, nil
    } else {
      var err error
      var id_bytes []byte
      id_bytes, value, err = value.PopData(16)
      if err != nil {
        return nil, nil, value, err
      }

      id, err := IDFromBytes(id_bytes)
      if err != nil {
        return nil, nil, value, err
      }

      id_value := reflect.ValueOf(id)
      return id_value.Type(), &id_value, value, nil
    }
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(Up), SignalDirectionType,
  func(ctx *Context, ctx_type SerializedType, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var data []byte = nil
    if value != nil {
      val := value.Interface().(SignalDirection)
      data = []byte{byte(val)}
    }
    return SerializedValue{
      []SerializedType{ctx_type},
      data,
    }, nil
  }, func(ctx *Context, value SerializedValue)(reflect.Type, *reflect.Value, SerializedValue, error){
    return reflect.TypeOf(Up), nil, SerializedValue{}, fmt.Errorf("unimplemented")
  })
  if err != nil {
    return nil, err
  }

  err = ctx.RegisterType(reflect.TypeOf(ReqState(0)), ReqStateType,
  func(ctx *Context, ctx_type SerializedType, t reflect.Type, value *reflect.Value) (SerializedValue, error) {
    var data []byte = nil
    if value != nil {
      val := value.Interface().(ReqState)
      data = []byte{byte(val)}
    }
    return SerializedValue{
      []SerializedType{ctx_type},
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
