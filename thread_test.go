package graphvent

import (
  "testing"
  "time"
  "fmt"
)

func TestNewThread(t * testing.T) {
  ctx := testContext(t)

  t1_r := NewSimpleThread(RandID(), "Test thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r

  go func(thread Thread) {
    time.Sleep(10*time.Millisecond)
    UseStates(ctx, []Node{t1}, func(nodes NodeMap) error {
      return t1.Signal(ctx, CancelSignal(nil), nodes)
    })
  }(t1)

  err := ThreadLoop(ctx, t1, "start")
  fatalErr(t, err)

  err = UseStates(ctx, []Node{t1}, func(nodes NodeMap) (error) {
    owner := t1.owner
    if owner != nil {
      return fmt.Errorf("Wrong owner %+v", owner)
    }
    return nil
  })
}

func TestThreadWithRequirement(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r

  t1_r := NewSimpleThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r

  err := UpdateStates(ctx, []Node{l1, t1}, func(nodes NodeMap) error {
    return LinkLockables(ctx, t1, []Lockable{l1}, nodes)
  })
  fatalErr(t, err)

  go func (thread Thread) {
    time.Sleep(10*time.Millisecond)
    UseStates(ctx, []Node{t1}, func(nodes NodeMap) error {
      return t1.Signal(ctx, CancelSignal(nil), nodes)
    })
  }(t1)
  fatalErr(t, err)

  err = ThreadLoop(ctx, t1, "start")
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    owner := l1.owner
    if owner != nil {
      return fmt.Errorf("Wrong owner %+v", owner)
    }
    return nil
  })
  fatalErr(t, err)
}

func TestThreadDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{})
  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  t1_r := NewSimpleThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r

  err := UpdateStates(ctx, []Node{t1, l1}, func(nodes NodeMap) error {
    return LinkLockables(ctx, t1, []Lockable{l1}, nodes)
  })

  err = UseStates(ctx, []Node{t1}, func(nodes NodeMap) error {
    return t1.Signal(ctx, CancelSignal(nil), nodes)
  })
  fatalErr(t, err)

  err = ThreadLoop(ctx, t1, "start")
  fatalErr(t, err)

  err = UseStates(ctx, []Node{t1}, func(nodes NodeMap) error {
    ser, err := t1.Serialize()
    fmt.Printf("\n%s\n\n", ser)
    return err
  })

  t1_loaded, err := LoadNode(ctx, t1.ID())
  fatalErr(t, err)

  err = UseStates(ctx, []Node{t1_loaded}, func(nodes NodeMap) error {
    ser, err := t1_loaded.Serialize()
    fmt.Printf("\n%s\n\n", ser)
    return err
  })
}

func TestThreadUnlink(t * testing.T) {
  ctx := logTestContext(t, []string{})
  t1_r := NewSimpleThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r
  t2_r := NewSimpleThread(RandID(), "Test Thread 2", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t2 := &t2_r


  err := UpdateStates(ctx, []Node{t1, t2}, func(nodes NodeMap) error {
    err := LinkThreads(ctx, t1, t2, nil, nodes)
    if err != nil {
      return err
    }

    return UnlinkThreads(ctx, t1, t2)
  })
  fatalErr(t, err)
}

