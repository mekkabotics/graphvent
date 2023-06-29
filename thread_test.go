package graphvent

import (
  "testing"
  "time"
  "fmt"
)

func TestNewThread(t * testing.T) {
  ctx := testContext(t)

  t1, err := NewSimpleBaseThread(ctx, "Test thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  go func(thread Thread) {
    time.Sleep(10*time.Millisecond)
    SendUpdate(ctx, t1, CancelSignal(nil))
  }(t1)

  err = RunThread(ctx, t1)
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{t1}, func(states NodeStateMap) (error) {
    owner := states[t1.ID()].(ThreadState).Owner()
    if owner != nil {
      return fmt.Errorf("Wrong owner %+v", owner)
    }
    return nil
  })
}

func TestThreadWithRequirement(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  t1, err := NewSimpleBaseThread(ctx, "Test Thread 1", []Lockable{l1}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  go func (thread Thread) {
    time.Sleep(10*time.Millisecond)
    SendUpdate(ctx, t1, CancelSignal(nil))
  }(t1)
  fatalErr(t, err)

  err = RunThread(ctx, t1)
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{l1}, func(states NodeStateMap) (error) {
    owner := states[l1.ID()].(LockableState).Owner()
    if owner != nil {
      return fmt.Errorf("Wrong owner %+v", owner)
    }
    return nil
  })
  fatalErr(t, err)
}

func TestCustomThreadState(t * testing.T ) {
  ctx := testContext(t)

  t1, err := NewSimpleBaseThread(ctx, "Test Thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)
  println(t1)
}
