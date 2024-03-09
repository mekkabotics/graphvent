package graphvent

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"reflect"
  "math"
	"slices"
)

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

type FieldTag SerializedType

func (t FieldTag) String() string {
  return fmt.Sprintf("0x%x", uint64(t))
}

func NodeTypeFor(extensions []ExtType) NodeType {
  digest := []byte("GRAPHVENT_NODE - ")

  slices.Sort(extensions)

  for _, ext := range(extensions) {
    digest = binary.BigEndian.AppendUint64(digest, uint64(ext))
  }

  hash := sha512.Sum512(digest)
  return NodeType(binary.BigEndian.Uint64(hash[0:8]))
}

func SerializeType(t fmt.Stringer) SerializedType {
  digest := []byte(t.String())
  hash := sha512.Sum512(digest)
  return SerializedType(binary.BigEndian.Uint64(hash[0:8]))
}

func SerializedTypeFor[T any]() SerializedType {
  return SerializeType(reflect.TypeFor[T]())
}

func ExtTypeFor[E any, T interface { *E; Extension}]() ExtType {
  return ExtType(SerializedTypeFor[E]())
}

func SignalTypeFor[S Signal]() SignalType {
  return SignalType(SerializedTypeFor[S]())
}

func Hash(base, data string) SerializedType { 
  digest := []byte(base + ":" + data)
  hash := sha512.Sum512(digest)
  return SerializedType(binary.BigEndian.Uint64(hash[0:8]))
}

func GetFieldTag(tag string) FieldTag {
  return FieldTag(Hash("GRAPHVENT_FIELD_TAG", tag))
}

func TypeStack(ctx *Context, t reflect.Type) ([]SerializedType, error) {
  info, registered := ctx.TypeTypes[t]
  if registered {
    return []SerializedType{info.Serialized}, nil
  } else {
    switch t.Kind() {
    case reflect.Map:
      key_stack, err := TypeStack(ctx, t.Key())
      if err != nil {
        return nil, err
      }

      elem_stack, err := TypeStack(ctx, t.Elem())
      if err != nil {
        return nil, err
      }

      return append([]SerializedType{SerializeType(reflect.Map)}, append(key_stack, elem_stack...)...), nil
    case reflect.Pointer:
      elem_stack, err := TypeStack(ctx, t.Elem())
      if err != nil {
        return nil, err
      }

      return append([]SerializedType{SerializeType(reflect.Pointer)}, elem_stack...), nil
    case reflect.Slice:
      elem_stack, err := TypeStack(ctx, t.Elem())
      if err != nil {
        return nil, err
      }

      return append([]SerializedType{SerializeType(reflect.Slice)}, elem_stack...), nil
    case reflect.Array:
      elem_stack, err := TypeStack(ctx, t.Elem())
      if err != nil {
        return nil, err
      }

      return append([]SerializedType{SerializeType(reflect.Array), SerializedType(t.Len())}, elem_stack...), nil
    default:
      return nil, fmt.Errorf("Hit %s, which is not a registered type", t.String())
    }
  }
}

func UnwrapStack(ctx *Context, stack []SerializedType) (reflect.Type, []SerializedType, error) {
  first := stack[0]
  stack = stack[1:]

  info, registered := ctx.TypeMap[first]
  if registered {
    return info.Reflect, stack, nil
  } else {
    switch first {
    case SerializeType(reflect.Map):
      key_type, after_key, err := UnwrapStack(ctx, stack)
      if err != nil {
        return nil, nil, err 
      }

      elem_type, after_elem, err := UnwrapStack(ctx, after_key)
      if err != nil {
        return nil, nil, err 
      }

      return reflect.MapOf(key_type, elem_type), after_elem, nil
    case SerializeType(reflect.Pointer):
      elem_type, rest, err := UnwrapStack(ctx, stack)
      if err != nil {
        return nil, nil, err 
      }
      return reflect.PointerTo(elem_type), rest, nil
    case SerializeType(reflect.Slice):
      elem_type, rest, err := UnwrapStack(ctx, stack)
      if err != nil {
        return nil, nil, err
      }
      return reflect.SliceOf(elem_type), rest, nil
    case SerializeType(reflect.Array):
      length := int(stack[0])

      stack = stack[1:]

      elem_type, rest, err := UnwrapStack(ctx, stack)
      if err != nil {
        return nil, nil, err
      }

      return reflect.ArrayOf(length, elem_type), rest, nil
    default:
      return nil, nil, fmt.Errorf("Type stack %+v not recognized", stack)
    }
  }
}

func Serialize[T any](ctx *Context, value T) ([]byte, error) {
  return serializeValue(ctx, reflect.ValueOf(value))
}

func Deserialize[T any](ctx *Context, data []byte) (T, error) {
  reflect_type := reflect.TypeFor[T]()
  var zero T
  value, left, err := deserializeValue(ctx, data, reflect_type)
  if err != nil {
    return zero, err
  } else if len(left) != 0 {
    return zero, fmt.Errorf("%d bytes left after deserializing %+v", len(left), value)
  } else if value.Type() != reflect_type {
    return zero, fmt.Errorf("Deserialized type %s does not match %s", value.Type(), reflect_type)
  }

  return value.Interface().(T), nil
}

func serializeValue(ctx *Context, value reflect.Value) ([]byte, error) {
  var serialize SerializeFn = nil

  info, registered := ctx.TypeTypes[value.Type()]
  if registered {
    serialize = info.Serialize 
  }

  if serialize == nil {
    switch value.Type().Kind() {
    case reflect.Bool:
      if value.Bool() {
        return []byte{0xFF}, nil
      } else {
        return []byte{0x00}, nil
      }

    case reflect.Int8:
      return []byte{byte(value.Int())}, nil
    case reflect.Int16:
      return binary.BigEndian.AppendUint16(nil, uint16(value.Int())), nil
    case reflect.Int32:
      return binary.BigEndian.AppendUint32(nil, uint32(value.Int())), nil
    case reflect.Int64:
      fallthrough
    case reflect.Int:
      return binary.BigEndian.AppendUint64(nil, uint64(value.Int())), nil

    case reflect.Uint8:
      return []byte{byte(value.Uint())}, nil
    case reflect.Uint16:
      return binary.BigEndian.AppendUint16(nil, uint16(value.Uint())), nil
    case reflect.Uint32:
      return binary.BigEndian.AppendUint32(nil, uint32(value.Uint())), nil
    case reflect.Uint64:
      fallthrough
    case reflect.Uint:
      return binary.BigEndian.AppendUint64(nil, value.Uint()), nil

    case reflect.Float32:
      return binary.BigEndian.AppendUint32(nil, math.Float32bits(float32(value.Float()))), nil
    case reflect.Float64:
      return binary.BigEndian.AppendUint64(nil, math.Float64bits(value.Float())), nil

    case reflect.String:
      len_bytes := make([]byte, 8)
      binary.BigEndian.PutUint64(len_bytes, uint64(value.Len()))
      return append(len_bytes, []byte(value.String())...), nil

    case reflect.Pointer:
      if value.IsNil() {
        return []byte{0x00}, nil
      } else {
        elem, err := serializeValue(ctx, value.Elem())
        if err != nil {
          return nil, err
        }

        return append([]byte{0x01}, elem...), nil
      }

    case reflect.Slice:
      if value.IsNil() {
        return []byte{0x00}, nil
      } else {
        len_bytes := make([]byte, 8)
        binary.BigEndian.PutUint64(len_bytes, uint64(value.Len()))

        data := []byte{}
        for i := 0; i < value.Len(); i++ {
          elem, err := serializeValue(ctx, value.Index(i))
          if err != nil {
            return nil, err
          }

          data = append(data, elem...)
        }

        return append(len_bytes, data...), nil
      }

    case reflect.Array:
      data := []byte{}
      for i := 0; i < value.Len(); i++ {
        elem, err := serializeValue(ctx, value.Index(i))
        if err != nil {
          return nil, err
        }

        data = append(data, elem...)
      }
      return data, nil

    case reflect.Map:
      len_bytes := make([]byte, 8)
      binary.BigEndian.PutUint64(len_bytes, uint64(value.Len()))

      data := []byte{}
      iter := value.MapRange()
      for iter.Next() {
        k, err := serializeValue(ctx, iter.Key())
        if err != nil {
          return nil, err
        }

        data = append(data, k...)

        v, err := serializeValue(ctx, iter.Value())
        if err != nil {
          return nil, err
        }

        data = append(data, v...)
      }
      return append(len_bytes, data...), nil
    
    case reflect.Struct:
      if registered == false {
        return nil, fmt.Errorf("Cannot serialize unregistered struct %s", value.Type())
      } else {
        data := binary.BigEndian.AppendUint64(nil, uint64(len(info.Fields)))

        for field_tag, field_info := range(info.Fields) {
          data = append(data, binary.BigEndian.AppendUint64(nil, uint64(field_tag))...)
          field_bytes, err := serializeValue(ctx, value.FieldByIndex(field_info.Index))
          if err != nil {
            return nil, err
          }

          data = append(data, field_bytes...)
        }
        return data, nil
      }

    default:
      return nil, fmt.Errorf("Don't know how to serialize %s", value.Type())
    }
  } else {
    return serialize(ctx, value)
  }
}

func split(data []byte, n int) ([]byte, []byte) {
  return data[:n], data[n:]
}

func deserializeValue(ctx *Context, data []byte, t reflect.Type) (reflect.Value, []byte, error) {
  var deserialize DeserializeFn = nil

  info, registered := ctx.TypeTypes[t]
  if registered {
    deserialize = info.Deserialize
  }

  if deserialize == nil {
    switch t.Kind() {
    case reflect.Bool:
      used, left := split(data, 1)
      value := reflect.New(t).Elem()
      value.SetBool(used[0] != 0x00)
      return value, left, nil

    case reflect.Int8:
      used, left := split(data, 1)
      value := reflect.New(t).Elem()
      value.SetInt(int64(used[0]))
      return value, left, nil
    case reflect.Int16:
      used, left := split(data, 2)
      value := reflect.New(t).Elem()
      value.SetInt(int64(binary.BigEndian.Uint16(used)))
      return value, left, nil
    case reflect.Int32:
      used, left := split(data, 4)
      value := reflect.New(t).Elem()
      value.SetInt(int64(binary.BigEndian.Uint32(used)))
      return value, left, nil
    case reflect.Int64:
      fallthrough
    case reflect.Int:
      used, left := split(data, 8)
      value := reflect.New(t).Elem()
      value.SetInt(int64(binary.BigEndian.Uint64(used)))
      return value, left, nil

    case reflect.Uint8:
      used, left := split(data, 1)
      value := reflect.New(t).Elem()
      value.SetUint(uint64(used[0]))
      return value, left, nil
    case reflect.Uint16:
      used, left := split(data, 2)
      value := reflect.New(t).Elem()
      value.SetUint(uint64(binary.BigEndian.Uint16(used)))
      return value, left, nil
    case reflect.Uint32:
      used, left := split(data, 4)
      value := reflect.New(t).Elem()
      value.SetUint(uint64(binary.BigEndian.Uint32(used)))
      return value, left, nil
    case reflect.Uint64:
      fallthrough
    case reflect.Uint:
      used, left := split(data, 8)
      value := reflect.New(t).Elem()
      value.SetUint(binary.BigEndian.Uint64(used))
      return value, left, nil

    case reflect.Float32:
      used, left := split(data, 4)
      value := reflect.New(t).Elem()
      value.SetFloat(float64(math.Float32frombits(binary.BigEndian.Uint32(used))))
      return value, left, nil
    case reflect.Float64:
      used, left := split(data, 8)
      value := reflect.New(t).Elem()
      value.SetFloat(math.Float64frombits(binary.BigEndian.Uint64(used)))
      return value, left, nil

    case reflect.String:
      length, after_len := split(data, 8)
      used, left := split(after_len, int(binary.BigEndian.Uint64(length)))
      value := reflect.New(t).Elem()
      value.SetString(string(used))
      return value, left, nil

    case reflect.Pointer:
      flags, after_flags := split(data, 1)
      value := reflect.New(t).Elem()
      if flags[0] == 0x00 {
        value.SetZero()
        return value, after_flags, nil
      } else {
        elem_value, after_elem, err := deserializeValue(ctx, after_flags, t.Elem())
        if err != nil {
          return reflect.Value{}, nil, err
        }
        value.Set(elem_value.Addr())
        return value, after_elem, nil
      }

    case reflect.Slice:
      len_bytes, left := split(data, 8)
      length := int(binary.BigEndian.Uint64(len_bytes))
      value := reflect.MakeSlice(t, length, length)
      for i := 0; i < length; i++ {
        var elem_value reflect.Value
        var err error
        elem_value, left, err = deserializeValue(ctx, left, t.Elem())
        if err != nil {
          return reflect.Value{}, nil, err
        }
        value.Index(i).Set(elem_value)
      }
      return value, left, nil

    case reflect.Array:
      value := reflect.New(t).Elem()
      left := data
      for i := 0; i < t.Len(); i++ {
        var elem_value reflect.Value
        var err error
        elem_value, left, err = deserializeValue(ctx, left, t.Elem())
        if err != nil {
          return reflect.Value{}, nil, err
        }
        value.Index(i).Set(elem_value)
      }
      return value, left, nil

    case reflect.Map:
      len_bytes, left := split(data, 8)
      length := int(binary.BigEndian.Uint64(len_bytes))

      value := reflect.MakeMapWithSize(t, length)

      for i := 0; i < length; i++ {
        var key_value reflect.Value
        var val_value reflect.Value
        var err error

        key_value, left, err = deserializeValue(ctx, left, t.Key())        
        if err != nil {
          return reflect.Value{}, nil, err
        }

        val_value, left, err = deserializeValue(ctx, left, t.Elem())
        if err != nil {
          return reflect.Value{}, nil, err
        }

        value.SetMapIndex(key_value, val_value)
      }

      return value, left, nil

    case reflect.Struct:
      info, mapped := ctx.TypeTypes[t]
      if mapped {
        value := reflect.New(t).Elem()

        num_field_bytes, left := split(data, 8)
        num_fields := int(binary.BigEndian.Uint64(num_field_bytes))

        for i := 0; i < num_fields; i++ {
          var tag_bytes []byte

          tag_bytes, left = split(left, 8)
          field_tag := FieldTag(binary.BigEndian.Uint64(tag_bytes))

          field_info, mapped := info.Fields[field_tag]
          if mapped {
            var field_val reflect.Value
            var err error
            field_val, left, err = deserializeValue(ctx, left, field_info.Type)
            if err != nil {
              return reflect.Value{}, nil, err
            }
            value.FieldByIndex(field_info.Index).Set(field_val)
          } else {
            return reflect.Value{}, nil, fmt.Errorf("Unknown field %s on struct %s", field_tag, t)
          }
        }
        if info.PostDeserializeIndex != -1 {
          post_deserialize_method := value.Addr().Method(info.PostDeserializeIndex)
          post_deserialize_method.Call([]reflect.Value{reflect.ValueOf(ctx)})
        }
        return value, left, nil
      } else {
        return reflect.Value{}, nil, fmt.Errorf("Cannot deserialize unregistered struct %s", t)
      }

    default:
      return reflect.Value{}, nil, fmt.Errorf("Don't know how to deserialize %s", t)
    }
  } else {
    return deserialize(ctx, data)
  }
}

