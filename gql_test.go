package graphvent

import (
  "testing"
  "time"
  "fmt"
  "errors"
)

func TestGQLThread(t * testing.T) {
  ctx := logTestContext(t, []string{})
  gql_t_r := NewGQLThread(RandID(), "GQL Thread", "init", ":0")
  gql_t := &gql_t_r

  t1_r := NewSimpleThread(RandID(), "Test thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r
  t2_r := NewSimpleThread(RandID(), "Test thread 2", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t2 := &t2_r

  err := UpdateStates(ctx, []Node{gql_t, t1, t2}, func(nodes NodeMap) error {
    i1 := NewParentThreadInfo(true, "start", "restore")
    err := LinkThreads(ctx, gql_t, t1, &i1, nodes)
    if err != nil {
      return err
    }

    i2 := NewParentThreadInfo(false, "start", "restore")
    return LinkThreads(ctx, gql_t, t2, &i2, nodes)
  })
  fatalErr(t, err)

  go func(thread Thread){
    time.Sleep(10*time.Millisecond)
    err := UseStates(ctx, []Node{thread}, func(nodes NodeMap) error {
      return thread.Signal(ctx, CancelSignal(nil), nodes)
    })
    fatalErr(t, err)
  }(gql_t)

  err = ThreadLoop(ctx, gql_t, "start")
  fatalErr(t, err)
}

func TestGQLDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{"thread", "signal", "gql", "test"})
  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r

  t1_r := NewSimpleThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r
  update_channel := UpdateChannel(t1, 10, "test")

  gql_r := NewGQLThread(RandID(), "GQL Thread", "init", ":8080")
  gql := &gql_r

  info := NewParentThreadInfo(true, "start", "restore")
  err := UpdateStates(ctx, []Node{gql, t1, l1}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, gql, []Lockable{l1}, nodes)
    if err != nil {
      return err
    }
    return LinkThreads(ctx, gql, t1, &info, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{gql}, func(nodes NodeMap) error {
    err := gql.Signal(ctx, NewSignal(t1, "child_added"), nodes)
    if err != nil {
      return nil
    }
    return gql.Signal(ctx, AbortSignal(nil), nodes)
  })
  fatalErr(t, err)

  err = ThreadLoop(ctx, gql, "start")
  if errors.Is(err, NewThreadAbortedError("")) {
    ctx.Log.Logf("test", "Main thread aborted by signal: %s", err)
  } else {
    fatalErr(t, err)
  }

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "thread_aborted", t1, 100*time.Millisecond, "Didn't receive thread_abort from t1 on t1")

  err = UseStates(ctx, []Node{gql, t1}, func(nodes NodeMap) error {
    ser1, err := gql.Serialize()
    ser2, err := t1.Serialize()
    fmt.Printf("\n%s\n\n", ser1)
    fmt.Printf("\n%s\n\n", ser2)
    return err
  })

  gql_loaded, err := LoadNode(ctx, gql.ID())
  fatalErr(t, err)
  var t1_loaded *SimpleThread = nil

  var update_channel_2 chan GraphSignal
  err = UseStates(ctx, []Node{gql_loaded}, func(nodes NodeMap) error {
    ser, err := gql_loaded.Serialize()
    fmt.Printf("\n%s\n\n", ser)
    child := gql_loaded.(Thread).Children()[0].(*SimpleThread)
    t1_loaded = child
    update_channel_2 = UpdateChannel(t1_loaded, 10, "test")
    err = UseMoreStates(ctx, []Node{child}, nodes, func(nodes NodeMap) error {
      ser, err := child.Serialize()
      fmt.Printf("\n%s\n\n", ser)
      return err
    })
    gql_loaded.Signal(ctx, AbortSignal(nil), nodes)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded.(Thread), "restore")
  if errors.Is(err, NewThreadAbortedError("")) {
    ctx.Log.Logf("test", "Main thread aborted by signal: %s", err)
  } else {
    fatalErr(t, err)
  }
  (*GraphTester)(t).WaitForValue(ctx, update_channel_2, "thread_aborted", t1_loaded, 100*time.Millisecond, "Didn't received thread_aborted on t1_loaded from t1_loaded")

}
