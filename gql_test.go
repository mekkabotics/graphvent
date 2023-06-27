package graphvent

import (
  "testing"
  "time"
)

func TestGQLThread(t * testing.T) {
  println("TEST_GQL")
  ctx := logTestContext(t, []string{"gqlws", "gql", "thread", "update"})
  gql_thread, err := NewGQLThread(ctx, ":8080", []Lockable{}, ObjTypeMap{}, FieldMap{}, FieldMap{}, FieldMap{})
  fatalErr(t, err)

  test_thread_1, err := NewSimpleBaseThread(ctx, "Test thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  test_thread_2, err := NewSimpleBaseThread(ctx, "Test thread 2", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  i1 := NewGQLThreadInfo(true)
  err = LinkThreads(ctx, gql_thread, test_thread_1, &i1)
  fatalErr(t, err)

  i2 := NewGQLThreadInfo(false)
  err = LinkThreads(ctx, gql_thread, test_thread_2, &i2)
  fatalErr(t, err)

  go func(thread Thread){
    time.Sleep(10*time.Millisecond)
    // Check that test_thread_1 is running and test_thread_2 is not
    SendUpdate(ctx, thread, CancelSignal(nil))
  }(gql_thread)

  err = RunThread(ctx, gql_thread)
  fatalErr(t, err)
}
