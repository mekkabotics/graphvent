package graphvent

import (
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"reflect"
  "math"
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

func NodeTypeFor(name string) NodeType {
  digest := []byte("GRAPHVENT_NODE - " + name)

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

func ExtTypeOf(t reflect.Type) ExtType {
  return ExtType(SerializeType(t.Elem()))
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

func TypeStack(ctx *Context, t reflect.Type, data []byte) (int, error) {
  info, registered := ctx.Types[t]
  if registered {
    binary.BigEndian.PutUint64(data, uint64(info.Serialized))
    return 8, nil
  } else {
    switch t.Kind() {
    case reflect.Map:
      binary.BigEndian.PutUint64(data, uint64(SerializeType(reflect.Map)))

      key_written, err := TypeStack(ctx, t.Key(), data[8:])
      if err != nil {
        return 0, err
      }

      elem_written, err := TypeStack(ctx, t.Elem(), data[8 + key_written:])
      if err != nil {
        return 0, err
      }

      return 8 + key_written + elem_written, nil

    case reflect.Pointer:
      binary.BigEndian.PutUint64(data, uint64(SerializeType(reflect.Pointer)))

      elem_written, err := TypeStack(ctx, t.Elem(), data[8:])
      if err != nil {
        return 0, err
      }

      return 8 + elem_written, nil
    case reflect.Slice:
      binary.BigEndian.PutUint64(data, uint64(SerializeType(reflect.Slice)))

      elem_written, err := TypeStack(ctx, t.Elem(), data[8:])
      if err != nil {
        return 0, err
      }

      return 8 + elem_written, nil
    case reflect.Array:
      binary.BigEndian.PutUint64(data, uint64(SerializeType(reflect.Array)))
      binary.BigEndian.PutUint64(data[8:], uint64(t.Len()))

      elem_written, err := TypeStack(ctx, t.Elem(), data[16:])
      if err != nil {
        return 0, err
      }

      return 16 + elem_written, nil

    default:
      return 0, fmt.Errorf("Hit %s, which is not a registered type", t.String())
    }
  }
}

func UnwrapStack(ctx *Context, stack []byte) (reflect.Type, []byte, error) {
  first_bytes, left := split(stack, 8)
  first := SerializedType(binary.BigEndian.Uint64(first_bytes))

  info, registered := ctx.TypesReverse[first]
  if registered {
    return info.Reflect, left, nil
  } else {
    switch first {
    case SerializeType(reflect.Map):
      key_type, after_key, err := UnwrapStack(ctx, left)
      if err != nil {
        return nil, nil, err 
      }

      elem_type, after_elem, err := UnwrapStack(ctx, after_key)
      if err != nil {
        return nil, nil, err 
      }

      return reflect.MapOf(key_type, elem_type), after_elem, nil
    case SerializeType(reflect.Pointer):
      elem_type, rest, err := UnwrapStack(ctx, left)
      if err != nil {
        return nil, nil, err 
      }
      return reflect.PointerTo(elem_type), rest, nil
    case SerializeType(reflect.Slice):
      elem_type, rest, err := UnwrapStack(ctx, left)
      if err != nil {
        return nil, nil, err
      }
      return reflect.SliceOf(elem_type), rest, nil
    case SerializeType(reflect.Array):
      length_bytes, left := split(left, 8)
      length := int(binary.BigEndian.Uint64(length_bytes))

      elem_type, rest, err := UnwrapStack(ctx, left)
      if err != nil {
        return nil, nil, err
      }

      return reflect.ArrayOf(length, elem_type), rest, nil
    default:
      return nil, nil, fmt.Errorf("Type stack %+v not recognized", stack)
    }
  }
}

func Serialize[T any](ctx *Context, value T, data []byte) (int, error) {
  return SerializeValue(ctx, reflect.ValueOf(&value).Elem(), data)
}

func Deserialize[T any](ctx *Context, data []byte) (T, error) {
  reflect_type := reflect.TypeFor[T]()
  var zero T
  value, left, err := DeserializeValue(ctx, data, reflect_type)
  if err != nil {
    return zero, err
  } else if len(left) != 0 {
    return zero, fmt.Errorf("%d bytes left after deserializing %+v", len(left), value)
  } else if value.Type() != reflect_type {
    return zero, fmt.Errorf("Deserialized type %s does not match %s", value.Type(), reflect_type)
  }

  return value.Interface().(T), nil
}

func SerializeValue(ctx *Context, value reflect.Value, data []byte) (int, error) {
  var serialize SerializeFn = nil

  info, registered := ctx.Types[value.Type()]
  if registered {
    serialize = info.Serialize 
  }

  if serialize == nil {
    switch value.Type().Kind() {
    case reflect.Bool:
      if value.Bool() {
        data[0] = 0xFF
      } else {
        data[0] = 0x00
      }
      return 1, nil

    case reflect.Int8:
      data[0] = byte(value.Int())
      return 1, nil
    case reflect.Int16:
      binary.BigEndian.PutUint16(data, uint16(value.Int()))
      return 2, nil
    case reflect.Int32:
      binary.BigEndian.PutUint32(data, uint32(value.Int()))
      return 4, nil
    case reflect.Int64:
      fallthrough
    case reflect.Int:
      binary.BigEndian.PutUint64(data, uint64(value.Int()))
      return 8, nil

    case reflect.Uint8:
      data[0] = byte(value.Uint())
      return 1, nil
    case reflect.Uint16:
      binary.BigEndian.PutUint16(data, uint16(value.Uint()))
      return 2, nil
    case reflect.Uint32:
      binary.BigEndian.PutUint32(data, uint32(value.Uint()))
      return 4, nil
    case reflect.Uint64:
      fallthrough
    case reflect.Uint:
      binary.BigEndian.PutUint64(data, value.Uint())
      return 8, nil

    case reflect.Float32:
      binary.BigEndian.PutUint32(data, math.Float32bits(float32(value.Float())))
      return 4, nil
    case reflect.Float64:
      binary.BigEndian.PutUint64(data, math.Float64bits(value.Float()))
      return 8, nil

    case reflect.String:
      binary.BigEndian.PutUint64(data, uint64(value.Len()))
      copy(data[8:], []byte(value.String()))
      return 8 + value.Len(), nil

    case reflect.Pointer:
      if value.IsNil() {
        data[0] = 0x00
        return 1, nil
      } else {
        data[0] = 0x01
        written, err := SerializeValue(ctx, value.Elem(), data[1:])
        if err != nil {
          return 0, err
        }
        return 1 + written, nil
      }

    case reflect.Slice:
      if value.IsNil() {
        data[0] = 0x00
        return 8, nil
      } else {
        data[0] = 0x01
        binary.BigEndian.PutUint64(data[1:], uint64(value.Len()))
        total_written := 0
        for i := 0; i < value.Len(); i++ {
          written, err := SerializeValue(ctx, value.Index(i), data[9+total_written:])
          if err != nil {
            return 0, err
          }
          total_written += written
        }
        return 9 + total_written, nil
      }

    case reflect.Array:
      data := []byte{}
      total_written := 0
      for i := 0; i < value.Len(); i++ {
        written, err := SerializeValue(ctx, value.Index(i), data[total_written:])
        if err != nil {
          return 0, err
        }
        total_written += written
      }
      return total_written, nil

    case reflect.Map:
      binary.BigEndian.PutUint64(data, uint64(value.Len()))

      key := reflect.New(value.Type().Key()).Elem()
      val := reflect.New(value.Type().Elem()).Elem()
      iter := value.MapRange()
      total_written := 0
      for iter.Next() {
        key.SetIterKey(iter)
        val.SetIterValue(iter)

        k, err := SerializeValue(ctx, key, data[8+total_written:])
        if err != nil {
          return 0, err
        }
        total_written += k

        v, err := SerializeValue(ctx, val, data[8+total_written:]) 
        if err != nil {
          return 0, err
        }
        total_written += v
      }
      return 8 + total_written, nil
    
    case reflect.Struct:
      if registered == false {
        return 0, fmt.Errorf("Cannot serialize unregistered struct %s", value.Type())
      } else {
        binary.BigEndian.PutUint64(data, uint64(len(info.Fields)))

        total_written := 0
        for field_tag, field_info := range(info.Fields) {
          binary.BigEndian.PutUint64(data[8+total_written:], uint64(field_tag))
          total_written += 8
          written, err := SerializeValue(ctx, value.FieldByIndex(field_info.Index), data[8+total_written:])
          if err != nil {
            return 0, err
          }
          total_written += written
        }
        return 8 + total_written, nil
      }

    case reflect.Interface:
      type_written, err := TypeStack(ctx, value.Elem().Type(), data)

      elem_written, err := SerializeValue(ctx, value.Elem(), data[type_written:])
      if err != nil {
        return 0, err
      }

      return type_written + elem_written, nil
    default:
      return 0, fmt.Errorf("Don't know how to serialize %s", value.Type())
    }
  } else {
    return serialize(ctx, value, data)
  }
}

func split(data []byte, n int) ([]byte, []byte) {
  return data[:n], data[n:]
}

func DeserializeValue(ctx *Context, data []byte, t reflect.Type) (reflect.Value, []byte, error) {
  var deserialize DeserializeFn = nil

  info, registered := ctx.Types[t]
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
        elem_value, after_elem, err := DeserializeValue(ctx, after_flags, t.Elem())
        if err != nil {
          return reflect.Value{}, nil, err
        }
        value.Set(elem_value.Addr())
        return value, after_elem, nil
      }

    case reflect.Slice:
      nil_byte := data[0]
      data = data[1:]
      if nil_byte == 0x00 {
        return reflect.New(t).Elem(), data, nil
      } else {
        len_bytes, left := split(data, 8)
        length := int(binary.BigEndian.Uint64(len_bytes))
        value := reflect.MakeSlice(t, length, length)
        for i := 0; i < length; i++ {
          var elem_value reflect.Value
          var err error
          elem_value, left, err = DeserializeValue(ctx, left, t.Elem())
          if err != nil {
            return reflect.Value{}, nil, err
          }
          value.Index(i).Set(elem_value)
        }
        return value, left, nil
      }

    case reflect.Array:
      value := reflect.New(t).Elem()
      left := data
      for i := 0; i < t.Len(); i++ {
        var elem_value reflect.Value
        var err error
        elem_value, left, err = DeserializeValue(ctx, left, t.Elem())
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

        key_value, left, err = DeserializeValue(ctx, left, t.Key())        
        if err != nil {
          return reflect.Value{}, nil, err
        }

        val_value, left, err = DeserializeValue(ctx, left, t.Elem())
        if err != nil {
          return reflect.Value{}, nil, err
        }

        value.SetMapIndex(key_value, val_value)
      }

      return value, left, nil

    case reflect.Struct:
      info, mapped := ctx.Types[t]
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
            field_val, left, err = DeserializeValue(ctx, left, field_info.Type)
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

    case reflect.Interface:
      elem_type, rest, err := UnwrapStack(ctx, data)
      if err != nil {
        return reflect.Value{}, nil, err
      }

      elem_val, left, err := DeserializeValue(ctx, rest, elem_type)
      if err != nil {
        return reflect.Value{}, nil, err
      }

      val := reflect.New(t).Elem()
      val.Set(elem_val)

      return val, left, nil

    default:
      return reflect.Value{}, nil, fmt.Errorf("Don't know how to deserialize %s", t)
    }
  } else {
    return deserialize(ctx, data)
  }
}

