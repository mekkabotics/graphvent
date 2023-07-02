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
    SendUpdate(ctx, t1, CancelSignal(nil))
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
    SendUpdate(ctx, t1, CancelSignal(nil))
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


  SendUpdate(ctx, t1, CancelSignal(nil))
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
