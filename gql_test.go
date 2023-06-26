package graphvent

import (
  "testing"
  "time"
)

func TestGQLThread(t * testing.T) {
  ctx := logTestContext(t, []string{"gqlws", "gql", "thread", "update"})
  gql_thread, err := NewGQLThread(ctx, ":8080", []Lockable{}, ObjTypeMap{}, FieldMap{}, FieldMap{}, FieldMap{})
  fatalErr(t, err)

  test_thread, err := NewSimpleBaseThread(ctx, "Test thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  err = LinkThreads(ctx, gql_thread, test_thread, nil)
  fatalErr(t, err)

  go func(thread Thread){
    time.Sleep(10*time.Millisecond)
    SendUpdate(ctx, thread, CancelSignal(nil))
  }(gql_thread)

  err = RunThread(ctx, gql_thread)
  fatalErr(t, err)
}
