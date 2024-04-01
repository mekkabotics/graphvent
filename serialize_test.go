package graphvent

import (
  "testing"
  "reflect"
  "github.com/google/uuid"
)

func testTypeStack[T any](t *testing.T, ctx *Context) {
  buffer := [1024]byte{}
  reflect_type := reflect.TypeFor[T]()
  written, err := TypeStack(ctx, reflect_type, buffer[:])
  fatalErr(t, err)

  stack := buffer[:written]

  ctx.Log.Logf("test", "TypeStack[%s]: %+v", reflect_type, stack)

  unwrapped_type, rest, err := UnwrapStack(ctx, stack)
  fatalErr(t, err)

  if len(rest) != 0 {
    t.Errorf("Types remaining after unwrapping %s: %+v", unwrapped_type, stack)
  }

  if unwrapped_type != reflect_type {
    t.Errorf("Unwrapped type[%+v] doesn't match original[%+v]", unwrapped_type, reflect_type)
  }

  ctx.Log.Logf("test", "Unwrapped type[%s]: %s", reflect_type, reflect_type)
}

func TestSerializeTypes(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})

  testTypeStack[int](t, ctx)
  testTypeStack[map[int]string](t, ctx)
  testTypeStack[string](t, ctx)
  testTypeStack[*string](t, ctx)
  testTypeStack[*map[string]*map[*string]int](t, ctx)
  testTypeStack[[5]int](t, ctx)
  testTypeStack[uuid.UUID](t, ctx)
  testTypeStack[NodeID](t, ctx)
}

func testSerializeCompare[T comparable](t *testing.T, ctx *Context, value T) {
  buffer := [1024]byte{}
  written, err := Serialize(ctx, value, buffer[:]) 
  fatalErr(t, err)

  serialized := buffer[:written]

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[T](), value, serialized)

  deserialized, err := Deserialize[T](ctx, serialized)
  fatalErr(t, err)

  if value != deserialized {
    t.Fatalf("Deserialized value[%+v] doesn't match original[%+v]", value, deserialized)
  }

  ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", value, deserialized)
}

func testSerializeList[L []T, T comparable](t *testing.T, ctx *Context, value L) {
  buffer := [1024]byte{}
  written, err := Serialize(ctx, value, buffer[:])
  fatalErr(t, err)

  serialized := buffer[:written]

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[L](), value, serialized)

  deserialized, err := Deserialize[L](ctx, serialized)
  fatalErr(t, err)

  for i, item := range(value) {
    if item != deserialized[i] {
      t.Fatalf("Deserialized list %+v does not match original %+v", value, deserialized)
    }
  }

  ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", value, deserialized)
}

func testSerializePointer[P interface {*T}, T comparable](t *testing.T, ctx *Context, value P) {
  buffer := [1024]byte{}

  written, err := Serialize(ctx, value, buffer[:])
  fatalErr(t, err)

  serialized := buffer[:written]

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[P](), value, serialized)

  deserialized, err := Deserialize[P](ctx, serialized)
  fatalErr(t, err)

  if value == nil && deserialized == nil {
    ctx.Log.Logf("test", "Deserialized nil")
  } else if value == nil {
    t.Fatalf("Non-nil value[%+v] returned for nil value", deserialized)
  } else if deserialized == nil {
    t.Fatalf("Nil value returned for non-nil value[%+v]", value)
  } else if *deserialized != *value {
    t.Fatalf("Deserialized value[%+v] doesn't match original[%+v]", value, deserialized)
  } else {
    ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", *value, *deserialized)
  }
}

func testSerialize[T any](t *testing.T, ctx *Context, value T) {
  buffer := [1024]byte{}
  written, err := Serialize(ctx, value, buffer[:])
  fatalErr(t, err)

  serialized := buffer[:written]

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[T](), value, serialized)

  deserialized, err := Deserialize[T](ctx, serialized)
  fatalErr(t, err)

  ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", value, deserialized)
}

func TestSerializeValues(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})

  testSerialize(t, ctx, Extension(NewLockableExt(nil)))

  testSerializeCompare[int8](t, ctx, -64)
  testSerializeCompare[int16](t, ctx, -64)
  testSerializeCompare[int32](t, ctx, -64)
  testSerializeCompare[int64](t, ctx, -64)
  testSerializeCompare[int](t, ctx, -64)

  testSerializeCompare[uint8](t, ctx, 64)
  testSerializeCompare[uint16](t, ctx, 64)
  testSerializeCompare[uint32](t, ctx, 64)
  testSerializeCompare[uint64](t, ctx, 64)
  testSerializeCompare[uint](t, ctx, 64)

  testSerializeCompare[string](t, ctx, "test")

  a := 12
  testSerializePointer[*int](t, ctx, &a)

  b := "test"
  testSerializePointer[*string](t, ctx, nil)
  testSerializePointer[*string](t, ctx, &b)

  testSerializeList(t, ctx, []int{1, 2, 3, 4, 5})

  testSerializeCompare[bool](t, ctx, true)
  testSerializeCompare[bool](t, ctx, false)
  testSerializeCompare[int](t, ctx, -1)
  testSerializeCompare[uint](t, ctx, 1)
  testSerializeCompare[NodeID](t, ctx, RandID())
  testSerializeCompare[*int](t, ctx, nil)
  testSerializeCompare(t, ctx, "string")

  testSerialize(t, ctx, map[string]string{
    "Test": "Test",
    "key": "String",
    "": "",
  })

  testSerialize[map[string]string](t, ctx, nil)

  testSerialize(t, ctx, NewListenerExt(10))

  node, err := ctx.NewNode(nil, "Node")
  fatalErr(t, err)
  testSerialize(t, ctx, node)
}
