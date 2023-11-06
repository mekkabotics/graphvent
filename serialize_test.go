package graphvent

import (
  "testing"
  "reflect"
  "fmt"
  "time"
)

func TestSerializeTest(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "serialize", "deserialize_types"})
  testSerialize(t, ctx, map[string][]NodeID{"test_group": {RandID(), RandID(), RandID()}})
  testSerialize(t, ctx, map[NodeID]ReqState{
    RandID(): Locked,
    RandID(): Unlocked,
  })
}

func TestSerializeBasic(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "serialize"})
  testSerializeComparable[bool](t, ctx, true)

  type bool_wrapped bool
  err := ctx.RegisterType(reflect.TypeOf(bool_wrapped(true)), NewSerializedType("BOOL_WRAPPED"), nil, nil, nil, DeserializeBool[bool_wrapped])
  fatalErr(t, err)
  testSerializeComparable[bool_wrapped](t, ctx, true)

  testSerializeSlice[[]bool](t, ctx, []bool{false, false, true, false})
  testSerializeComparable[string](t, ctx, "test")
  testSerializeComparable[float32](t, ctx, 0.05)
  testSerializeComparable[float64](t, ctx, 0.05)
  testSerializeComparable[uint](t, ctx, uint(1234))
  testSerializeComparable[uint8] (t, ctx, uint8(123))
  testSerializeComparable[uint16](t, ctx, uint16(1234))
  testSerializeComparable[uint32](t, ctx, uint32(12345))
  testSerializeComparable[uint64](t, ctx, uint64(123456))
  testSerializeComparable[int](t, ctx, 1234)
  testSerializeComparable[int8] (t, ctx, int8(-123))
  testSerializeComparable[int16](t, ctx, int16(-1234))
  testSerializeComparable[int32](t, ctx, int32(-12345))
  testSerializeComparable[int64](t, ctx, int64(-123456))
  testSerializeComparable[time.Duration](t, ctx, time.Duration(100))
  testSerializeComparable[time.Time](t, ctx, time.Now().Truncate(0))
  testSerializeSlice[[]int](t, ctx, []int{123, 456, 789, 101112})
  testSerializeSlice[[]int](t, ctx, ([]int)(nil))
  testSerializeSliceSlice[[][]int](t, ctx, [][]int{{123, 456, 789, 101112}, {3253, 2341, 735, 212}, {123, 51}, nil})
  testSerializeSliceSlice[[][]string](t, ctx, [][]string{{"123", "456", "789", "101112"}, {"3253", "2341", "735", "212"}, {"123", "51"}, nil})

  testSerialize(t, ctx, map[int8]map[*int8]string{})
  testSerialize(t, ctx, map[int8]time.Time{
    1: time.Now(),
    3: time.Now().Add(time.Second),
    0: time.Now().Add(time.Second*2),
    4: time.Now().Add(time.Second*3),
  })

  testSerialize(t, ctx, Tree{
    NodeTypeSerialized: nil,
    SerializedTypeSerialized: Tree{
      NodeTypeSerialized: Tree{},
    },
  })

  var i interface{} = nil
  testSerialize(t, ctx, i)

  testSerializeMap(t, ctx, map[int8]interface{}{
    0: "abcd",
    1: uint32(12345678),
    2: i,
    3: 123,
  })

  testSerializeMap(t, ctx, map[int8]int32{
    0: 1234,
    2: 5678,
    4: 9101,
    6: 1121,
  })

  type test_struct struct {
    Int int `gv:"int"`
    String string `gv:"string"`
  }

  test_struct_type := reflect.TypeOf(test_struct{})
  test_struct_info, err := GetStructInfo(ctx, test_struct_type)
  fatalErr(t, err)

  err = ctx.RegisterType(test_struct_type, NewSerializedType("TEST_STRUCT"), nil, SerializeStruct(test_struct_info), nil, DeserializeStruct(test_struct_info))
  fatalErr(t, err)

  testSerialize(t, ctx, test_struct{
    12345,
    "test_string",
  })

  testSerialize(t, ctx, Tree{
    MapType: nil,
    StringType: nil,
  })

  testSerialize(t, ctx, Tree{
    TreeType: nil,
  })

  testSerialize(t, ctx, Tree{
    TreeType: {
      ErrorType: Tree{},
      MapType: nil,
    },
    StringType: nil,
  })

  type test_slice []string
  test_slice_type := reflect.TypeOf(test_slice{})
  err = ctx.RegisterType(test_slice_type, NewSerializedType("TEST_SLICE"), SerializeTypeStub, SerializeSlice, DeserializeTypeStub[test_slice], DeserializeSlice)
  fatalErr(t, err)

  testSerialize[[]string](t, ctx, []string{"test_1", "test_2", "test_3"})
  testSerialize[test_slice](t, ctx, test_slice{"test_1", "test_2", "test_3"})
}

type test struct {
  Int int `gv:"int"`
  Str string `gv:"string"`
}

func (s test) String() string {
  return fmt.Sprintf("%d:%s", s.Int, s.Str)
}

func TestSerializeStructTags(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})

  test_type := NewSerializedType("TEST_STRUCT")
  test_struct_type := reflect.TypeOf(test{})
  ctx.Log.Logf("test", "TEST_TYPE: %+v", test_type)
  test_struct_info, err := GetStructInfo(ctx, test_struct_type)
  fatalErr(t, err)
  ctx.RegisterType(test_struct_type, test_type, nil, SerializeStruct(test_struct_info), nil, DeserializeStruct(test_struct_info))

  test_int := 10
  test_string := "test"

  ret := testSerialize(t, ctx, test{
    test_int,
    test_string,
  })
  if ret.Int != test_int {
    t.Fatalf("Deserialized int %d does not equal test %d", ret.Int, test_int)
  } else if ret.Str != test_string {
    t.Fatalf("Deserialized string %s does not equal test %s", ret.Str, test_string)
  }

  testSerialize(t, ctx, []test{
    {
      test_int,
      test_string,
    },
    {
      test_int * 2,
      fmt.Sprintf("%s%s", test_string, test_string),
    },
    {
      test_int * 4,
      fmt.Sprintf("%s%s%s", test_string, test_string, test_string),
    },
  })
}

func testSerializeMap[M map[T]R, T, R comparable](t *testing.T, ctx *Context, val M) {
  v := testSerialize(t, ctx, val)
  for key, value := range(val) {
    recreated, exists := v[key]
    if exists == false {
      t.Fatalf("DeserializeValue returned wrong value %+v != %+v", v, val)
    } else if recreated != value {
      t.Fatalf("DeserializeValue returned wrong value %+v != %+v", v, val)
    }
  }
  if len(v) != len(val) {
    t.Fatalf("DeserializeValue returned wrong value %+v != %+v", v, val)
  }
}

func testSerializeSliceSlice[S [][]T, T comparable](t *testing.T, ctx *Context, val S) {
  v := testSerialize(t, ctx, val)
  for i, original := range(val) {
    if (original == nil && v[i] != nil) || (original != nil && v[i] == nil) {
      t.Fatalf("DeserializeValue returned wrong value %+v != %+v", v, val)
    }
    for j, o := range(original) {
      if v[i][j] != o {
        t.Fatalf("DeserializeValue returned wrong value %+v != %+v", v, val)
      }
    }
  }
}

func testSerializeSlice[S []T, T comparable](t *testing.T, ctx *Context, val S) {
  v := testSerialize(t, ctx, val)
  for i, original := range(val) {
    if v[i] != original {
      t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
    }
  }
}

func testSerializeComparable[T comparable](t *testing.T, ctx *Context, val T) {
  v := testSerialize(t, ctx, val)
  if v != val {
    t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
  }
}

func testSerialize[T any](t *testing.T, ctx *Context, val T) T {
  value := reflect.ValueOf(&val).Elem()
  type_stack, err := SerializeType(ctx, value.Type())
  chunks, err := SerializeValue(ctx, value)
  value_serialized := SerializedValue{type_stack, chunks.Slice()}
  fatalErr(t, err)
  ctx.Log.Logf("test", "Serialized %+v to %+v(%d)", val, value_serialized, len(value_serialized.Data))

  value_chunks, err := value_serialized.Chunks()
  fatalErr(t, err)
  ctx.Log.Logf("test", "Binary: %+v", value_chunks.Slice())

  val_parsed, remaining_parse, err := ParseSerializedValue(value_chunks.Slice())
  fatalErr(t, err)
  ctx.Log.Logf("test", "Parsed: %+v", val_parsed)

  if len(remaining_parse) != 0 {
    t.Fatal("Data remaining after deserializing value")
  }

  val_type, remaining_types, err := DeserializeType(ctx, val_parsed.TypeStack)
  deserialized_value, remaining_deserialize, err := DeserializeValue(ctx, val_type, val_parsed.Data)
  fatalErr(t, err)

  if len(remaining_deserialize) != 0 {
    t.Fatal("Data remaining after deserializing value")
  } else if len(remaining_types) != 0 {
    t.Fatal("TypeStack remaining after deserializing value")
  } else if val_type != value.Type() {
    t.Fatal(fmt.Sprintf("DeserializeValue returned wrong reflect.Type %+v - %+v", val_type, reflect.TypeOf(val)))
  } else if deserialized_value.CanConvert(val_type) == false {
    t.Fatal("DeserializeValue returned value that can't convert to original value")
  }
  ctx.Log.Logf("test", "Value: %+v", deserialized_value.Interface())
  if val_type.Kind() == reflect.Interface && deserialized_value.Interface() == nil {
    var zero T
    return zero
  }
  return deserialized_value.Interface().(T)
}
