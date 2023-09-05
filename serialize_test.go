package graphvent

import (
  "testing"
  "reflect"
  "fmt"
)

func TestSerializeBasic(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "serialize"})
  testSerializeComparable[string](t, ctx, "test")
  testSerializeComparable[bool](t, ctx, true)
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
  testSerializeSlice[[]int](t, ctx, []int{123, 456, 789, 101112})
  testSerializeSlice[[]int](t, ctx, ([]int)(nil))
  testSerializeSliceSlice[[][]int](t, ctx, [][]int{{123, 456, 789, 101112}, {3253, 2341, 735, 212}, {123, 51}, nil})
  testSerializeSliceSlice[[][]string](t, ctx, [][]string{{"123", "456", "789", "101112"}, {"3253", "2341", "735", "212"}, {"123", "51"}, nil})

  testSerialize(t, ctx, map[int8]map[*int8]string{})

  testSerializeMap(t, ctx, map[int8]int32{
    0: 1234,
    2: 5678,
    4: 9101,
    6: 1121,
  })

  testSerialize(t, ctx, struct{
    int `gv:"0"`
    string `gv:"1"`
  }{
    12345,
    "test_string",
  })
}

func testSerializeMap[M map[T]R, T, R comparable](t *testing.T, ctx *Context, val M) {
  v := testSerialize(t, ctx, val)
  for key, value := range(val) {
    recreated, exists := v[key]
    if exists == false {
      t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
    } else if recreated != value {
      t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
    }
  }
  if len(v) != len(val) {
    t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
  }
}

func testSerializeSliceSlice[S [][]T, T comparable](t *testing.T, ctx *Context, val S) {
  v := testSerialize(t, ctx, val)
  for i, original := range(val) {
    if (original == nil && v[i] != nil) || (original != nil && v[i] == nil) {
      t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
    }
    for j, o := range(original) {
      if v[i][j] != o {
        t.Fatal(fmt.Sprintf("DeserializeValue returned wrong value %+v != %+v", v, val))
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
  value, err := SerializeAny(ctx, val)
  fatalErr(t, err)
  ctx.Log.Logf("test", "Serialized %+v to %+v", val, value)

  ser, err := value.MarshalBinary()
  fatalErr(t, err)
  ctx.Log.Logf("test", "Binary: %+v", ser)

  val_parsed, remaining_parse, err := ParseSerializedValue(ser)
  fatalErr(t, err)
  ctx.Log.Logf("test", "Parsed: %+v", val_parsed)

  if len(remaining_parse) != 0 {
    t.Fatal("Data remaining after deserializing value")
  }

  val_type, deserialized_values, remaining_deserialize, err := DeserializeValue(ctx, val_parsed)
  fatalErr(t, err)

  if len(remaining_deserialize.Data) != 0 {
    t.Fatal("Data remaining after deserializing value")
  } else if len(remaining_deserialize.TypeStack) != 0 {
    t.Fatal("TypeStack remaining after deserializing value")
  } else if val_type != reflect.TypeOf(val) {
    t.Fatal(fmt.Sprintf("DeserializeValue returned wrong reflect.Type %+v - %+v", val_type, reflect.TypeOf(val)))
  } else if deserialized_values == nil {
    t.Fatal("DeserializeValue returned no []reflect.Value")
  } else if deserialized_values == nil {
    t.Fatal("DeserializeValue returned nil *reflect.Value")
  } else if deserialized_values.CanConvert(val_type) == false {
    t.Fatal("DeserializeValue returned value that can't convert to original value")
  }
  return deserialized_values.Interface().(T)
}
