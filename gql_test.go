package graphvent

import (
  "testing"
  "time"
  "fmt"
  "encoding/json"
)

func TestGQLThread(t * testing.T) {
  ctx := testContext(t)
  gql_thread, err := NewGQLThread(ctx, ":8080", []Lockable{})
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
    SendUpdate(ctx, thread, CancelSignal(nil))
  }(gql_thread)

  err = RunThread(ctx, gql_thread)
  fatalErr(t, err)
}

func TestGQLDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{})
  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  t1, err := NewGQLThread(ctx, ":8080", []Lockable{l1})
  fatalErr(t, err)

  SendUpdate(ctx, t1, CancelSignal(nil))
  err = RunThread(ctx, t1)
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{t1}, func(states NodeStateMap) error {
    ser, err := json.MarshalIndent(states[t1.ID()], "", "  ")
    fmt.Printf("\n%s\n\n", ser)
    return err
  })

  t1_loaded, err := LoadNode(ctx, t1.ID())
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{t1_loaded}, func(states NodeStateMap) error {
    ser, err := json.MarshalIndent(states[t1_loaded.ID()], "", "  ")
    fmt.Printf("\n%s\n\n", ser)
    return err
  })
}
