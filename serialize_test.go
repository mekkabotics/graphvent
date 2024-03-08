package graphvent

import (
  "testing"
  "reflect"
  "github.com/google/uuid"
)

func testTypeStack[T any](t *testing.T, ctx *Context) {
  reflect_type := reflect.TypeFor[T]()
  stack, err := TypeStack(ctx, reflect_type)
  fatalErr(t, err)

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
  serialized, err := SerializeValue(ctx, reflect.ValueOf(value))
  fatalErr(t, err)

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[T](), value, serialized)

  deserialized, left, err := DeserializeValue(ctx, serialized, reflect.TypeFor[T]())
  fatalErr(t, err)

  if len(left) != 0 {
    t.Fatalf("Data left after deserialize[%+v]: %+v", deserialized, left)
  }

  if reflect.TypeFor[T]() != deserialized.Type() {
    t.Fatalf("Type mismatch after deserialize %s != %s", reflect.TypeFor[T](), deserialized.Type())
  }

  val, ok := deserialized.Interface().(T)
  if ok == false {
    t.Fatalf("Deserialized type[%s] can't cast to type %s", deserialized.Type(), reflect.TypeFor[T]())
  }

  if value != val {
    t.Fatalf("Deserialized value[%+v] doesn't match original[%+v]", value, deserialized)
  }

  ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", value, val)
}

func testSerializeList[L []T, T comparable](t *testing.T, ctx *Context, value L) {
  serialized, err := SerializeValue(ctx, reflect.ValueOf(value))
  fatalErr(t, err)

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[L](), value, serialized)

  deserialized, left, err := DeserializeValue(ctx, serialized, reflect.TypeFor[L]())
  fatalErr(t, err)

  if len(left) != 0 {
    t.Fatalf("Data left after deserialize[%+v]: %+v", deserialized, left)
  }

  if reflect.TypeFor[L]() != deserialized.Type() {
    t.Fatalf("Type mismatch after deserialize %s != %s", reflect.TypeFor[L](), deserialized.Type())
  }

  val, ok := deserialized.Interface().(L)
  if ok == false {
    t.Fatalf("Deserialized type[%s] can't cast to type %s", deserialized.Type(), reflect.TypeFor[L]())
  }

  for i, item := range(value) {
    if item != val[i] {
      t.Fatalf("Deserialized list %+v does not match original %+v", value, val)
    }
  }

  ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", value, val)
}

func testSerializePointer[P interface {*T}, T comparable](t *testing.T, ctx *Context, value P) {
  serialized, err := SerializeValue(ctx, reflect.ValueOf(value))
  fatalErr(t, err)

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[P](), value, serialized)

  deserialized, left, err := DeserializeValue(ctx, serialized, reflect.TypeFor[P]())
  fatalErr(t, err)

  if len(left) != 0 {
    t.Fatalf("Data left after deserialize[%+v]: %+v", deserialized, left)
  }

  if reflect.TypeFor[P]() != deserialized.Type() {
    t.Fatalf("Type mismatch after deserialize %s != %s", reflect.TypeFor[P](), deserialized.Type())
  }

  val, ok := deserialized.Interface().(P)
  if ok == false {
    t.Fatalf("Deserialized type[%s] can't cast to type %s", deserialized.Type(), reflect.TypeFor[P]())
  }

  if value == nil && val == nil {
    ctx.Log.Logf("test", "Deserialized nil")
  } else if value == nil {
    t.Fatalf("Non-nil value[%+v] returned for nil value", val)
  } else if val == nil {
    t.Fatalf("Nil value returned for non-nil value[%+v]", value)
  } else if *val != *value {
    t.Fatalf("Deserialized value[%+v] doesn't match original[%+v]", value, deserialized)
  } else {
    ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", *value, *val)
  }
}

func testSerialize[T any](t *testing.T, ctx *Context, value T) {
  serialized, err := SerializeValue(ctx, reflect.ValueOf(value))
  fatalErr(t, err)

  ctx.Log.Logf("test", "Serialized Value[%s : %+v]: %+v", reflect.TypeFor[T](), value, serialized)

  deserialized, left, err := DeserializeValue(ctx, serialized, reflect.TypeFor[T]())
  fatalErr(t, err)

  if len(left) != 0 {
    t.Fatalf("Data left after deserialize[%+v]: %+v", deserialized, left)
  }

  if reflect.TypeFor[T]() != deserialized.Type() {
    t.Fatalf("Type mismatch after deserialize %s != %s", reflect.TypeFor[T](), deserialized.Type())
  }

  val, ok := deserialized.Interface().(T)
  if ok == false {
    t.Fatalf("Deserialized type[%s] can't cast to type %s", deserialized.Type(), reflect.TypeFor[T]())
  }

  ctx.Log.Logf("test", "Deserialized Value[%+v]: %+v", value, val)
}

func TestSerializeValues(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})

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

  node, err := NewNode(ctx, nil, "Base", 100)
  fatalErr(t, err)
  testSerialize(t, ctx, node)
}
