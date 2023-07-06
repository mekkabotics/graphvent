package graphvent

import (
  "testing"
  "time"
  "fmt"
  "encoding/json"
  "errors"
)

func TestGQLThread(t * testing.T) {
  ctx := logTestContext(t, []string{})
  gql_thread, err := NewGQLThread(ctx, ":0", []Lockable{})
  fatalErr(t, err)

  test_thread_1, err := NewSimpleThread(ctx, "Test thread 1", []Lockable{}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)

  test_thread_2, err := NewSimpleThread(ctx, "Test thread 2", []Lockable{}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)

  i1 := NewGQLThreadInfo(true, "start", "restore")
  err = LinkThreads(ctx, gql_thread, test_thread_1, &i1)
  fatalErr(t, err)

  i2 := NewGQLThreadInfo(false, "start", "restore")
  err = LinkThreads(ctx, gql_thread, test_thread_2, &i2)
  fatalErr(t, err)

  go func(thread Thread){
    time.Sleep(10*time.Millisecond)
    err := UseStates(ctx, []GraphNode{thread}, func(states NodeStateMap) error {
      SendUpdate(ctx, thread, CancelSignal(nil), states)
      return nil
    })
    fatalErr(t, err)
  }(gql_thread)

  err = ThreadLoop(ctx, gql_thread, "start")
  fatalErr(t, err)
}

func TestGQLDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{"thread", "update", "gql"})
  l1, err := NewSimpleLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  t1, err := NewSimpleThread(ctx, "Test Thread 1", []Lockable{}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)
  update_channel := t1.UpdateChannel(10)

  gql, err := NewGQLThread(ctx, ":0", []Lockable{l1})
  fatalErr(t, err)

  info := NewGQLThreadInfo(true, "start", "restore")
  err = UpdateStates(ctx, []GraphNode{gql, t1}, func(nodes NodeMap) error {
    return LinkThreads(ctx, gql, t1, &info)
  })
  fatalErr(t, err)
  err = UseStates(ctx, []GraphNode{gql}, func(states NodeStateMap) error {
    SendUpdate(ctx, gql, NewSignal(t1, "child_added"), states)
    SendUpdate(ctx, gql, AbortSignal(nil), states)
    return nil
  })
  err = ThreadLoop(ctx, gql, "start")
  if errors.Is(err, NewThreadAbortedError("")) {
    ctx.Log.Logf("test", "Main thread aborted by signal: %s", err)
  } else {
    fatalErr(t, err)
  }

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "thread_aborted", t1, 100*time.Millisecond, "Didn't receive thread_abort from t1 on t1")

  err = UseStates(ctx, []GraphNode{gql, t1}, func(states NodeStateMap) error {
    ser1, err := json.MarshalIndent(states[gql.ID()], "", "  ")
    ser2, err := json.MarshalIndent(states[t1.ID()], "", "  ")
    fmt.Printf("\n%s\n\n", ser1)
    fmt.Printf("\n%s\n\n", ser2)
    return err
  })

  gql_loaded, err := LoadNode(ctx, gql.ID())
  fatalErr(t, err)
  var t1_loaded *BaseThread = nil

  err = UseStates(ctx, []GraphNode{gql_loaded}, func(states NodeStateMap) error {
    ser, err := json.MarshalIndent(states[gql_loaded.ID()], "", "  ")
    fmt.Printf("\n%s\n\n", ser)
    child := states[gql_loaded.ID()].(ThreadState).Children()[0]
    t1_loaded = child.(*BaseThread)
    update_channel = t1_loaded.UpdateChannel(10)
    err = UseMoreStates(ctx, []GraphNode{child}, states, func(states NodeStateMap) error {
      ser, err := json.MarshalIndent(states[child.ID()], "", "  ")
      fmt.Printf("\n%s\n\n", ser)
      return err
    })
    SendUpdate(ctx, gql_loaded, AbortSignal(nil), states)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded.(Thread), "restore")
  if errors.Is(err, NewThreadAbortedError("")) {
    ctx.Log.Logf("test", "Main thread aborted by signal: %s", err)
  } else {
    fatalErr(t, err)
  }
  (*GraphTester)(t).WaitForValue(ctx, update_channel, "thread_aborted", t1_loaded, 100*time.Millisecond, "Dicn't received thread_aborted on t1_loaded from t1_loaded")

}
