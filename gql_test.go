package graphvent

import (
  "testing"
)

func TestGQLThread(t * testing.T) {
  ctx := testContext(t)
  gql_thread, err := NewGQLThread(ctx, ":8080", []Lockable{}, ObjTypeMap{}, FieldMap{}, FieldMap{}, FieldMap{})
  fatalErr(t, err)

  test_thread, err := NewSimpleBaseThread(ctx, "Test thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  err = LinkThreads(ctx, gql_thread, test_thread, nil)
  fatalErr(t, err)

  err = RunThread(ctx, gql_thread)
  fatalErr(t, err)
}
