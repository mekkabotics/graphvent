package graphvent

import (
  "testing"
  "time"
  "fmt"
  "encoding/json"
)

func TestNewThread(t * testing.T) {
  ctx := testContext(t)

  t1, err := NewSimpleThread(ctx, "Test thread 1", []Lockable{}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)

  go func(thread Thread) {
    time.Sleep(10*time.Millisecond)
    UseStates(ctx, []GraphNode{t1}, func(states NodeStateMap) error {
      SendUpdate(ctx, t1, CancelSignal(nil), states)
      return nil
    })
  }(t1)

  err = RunThread(ctx, t1, "start")
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

  l1, err := NewSimpleLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  t1, err := NewSimpleThread(ctx, "Test Thread 1", []Lockable{l1}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)

  go func (thread Thread) {
    time.Sleep(10*time.Millisecond)
    UseStates(ctx, []GraphNode{t1}, func(states NodeStateMap) error {
      SendUpdate(ctx, t1, CancelSignal(nil), states)
      return nil
    })
  }(t1)
  fatalErr(t, err)

  err = RunThread(ctx, t1, "start")
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

func TestThreadDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{})
  l1, err := NewSimpleLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  t1, err := NewSimpleThread(ctx, "Test Thread 1", []Lockable{l1}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)


  UseStates(ctx, []GraphNode{t1}, func(states NodeStateMap) error {
    SendUpdate(ctx, t1, CancelSignal(nil), states)
    return nil
  })
  err = RunThread(ctx, t1, "start")
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

func TestThreadUnlink(t * testing.T) {
  ctx := logTestContext(t, []string{})
  t1, err := NewSimpleThread(ctx, "Test Thread 1", []Lockable{}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)

  t2, err := NewSimpleThread(ctx, "Test Thread 2", []Lockable{}, BaseThreadActions, BaseThreadHandlers)
  fatalErr(t, err)

  err = LinkThreads(ctx, t1, t2, nil)
  fatalErr(t, err)

  err = UnlinkThreads(ctx, t1, t2)
  fatalErr(t, err)
}
