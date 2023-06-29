package graphvent

import (
  "testing"
  "fmt"
  "time"
)

func TestNewSimpleBaseLockable(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  _, err = NewSimpleBaseLockable(ctx, "Test lockable 2", []Lockable{r1})
  fatalErr(t, err)
}

func TestRepeatedChildLockable(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  _, err = NewSimpleBaseLockable(ctx, "Test lockable 2", []Lockable{r1, r1})
  if err == nil {
    t.Fatal("Added the same lockable as a requirement twice to the same lockable")
  }
}

func TestLockableSelfLock(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r1}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{r1}, r1, nil, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1}, func(states NodeStateMap) (error) {
    owner_id := states[r1.ID()].(LockableState).Owner().ID()
    if owner_id != r1.ID() {
      return fmt.Errorf("r1 is owned by %s instead of self", owner_id)
    }
    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r1}, func(nodes NodeMap) error {
    return UnlockLockables(ctx, []Lockable{r1}, r1, nil, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1}, func(states NodeStateMap) (error) {
    owner := states[r1.ID()].(LockableState).Owner()
    if owner != nil {
      return fmt.Errorf("r1 is not unowned after unlock: %s", owner.ID())
    }
    return nil
  })

  fatalErr(t, err)
}

func TestLockableSelfLockTiered(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewSimpleBaseLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  r3, err := NewSimpleBaseLockable(ctx, "Test lockable 3", []Lockable{r1, r2})
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r3}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{r3}, r3, nil, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1, r2, r3}, func(states NodeStateMap) (error) {
    owner_1_id := states[r1.ID()].(LockableState).Owner().ID()
    if owner_1_id != r3.ID() {
      return fmt.Errorf("r1 is owned by %s instead of r3", owner_1_id)
    }

    owner_2_id := states[r2.ID()].(LockableState).Owner().ID()
    if owner_2_id != r3.ID() {
      return fmt.Errorf("r2 is owned by %s instead of r3", owner_2_id)
    }
    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r3}, func(nodes NodeMap) error {
    return UnlockLockables(ctx, []Lockable{r3}, r3, nil, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1, r2, r3}, func(states NodeStateMap) (error) {
    owner_1 := states[r1.ID()].(LockableState).Owner()
    if owner_1 != nil {
      return fmt.Errorf("r1 is not unowned after unlocking: %s", owner_1.ID())
    }

    owner_2 := states[r2.ID()].(LockableState).Owner()
    if owner_2 != nil {
      return fmt.Errorf("r2 is not unowned after unlocking: %s", owner_2.ID())
    }

    owner_3 := states[r3.ID()].(LockableState).Owner()
    if owner_3 != nil {
      return fmt.Errorf("r3 is not unowned after unlocking: %s", owner_3.ID())
    }
    return nil
  })

  fatalErr(t, err)
}

func TestLockableLockOther(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewSimpleBaseLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r1, r2}, func(nodes NodeMap) (error) {
    node_state := r2.State().(LockableState)
    err := LockLockables(ctx, []Lockable{r1}, r2, node_state, nodes)
    fatalErr(t, err)
    return nil
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1}, func(states NodeStateMap) (error) {
    owner_id := states[r1.ID()].(LockableState).Owner().ID()
    if owner_id != r2.ID() {
      return fmt.Errorf("r1 is owned by %s instead of r2", owner_id)
    }

    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r2}, func(nodes NodeMap) (error) {
    node_state := r2.State().(LockableState)
    err := UnlockLockables(ctx, []Lockable{r1}, r2, node_state, nodes)
    fatalErr(t, err)
    return nil
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1}, func(states NodeStateMap) (error) {
    owner := states[r1.ID()].(LockableState).Owner()
    if owner != nil {
      return fmt.Errorf("r1 is owned by %s instead of r2", owner.ID())
    }

    return nil
  })
  fatalErr(t, err)

}

func TestLockableLockSimpleConflict(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewSimpleBaseLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r1}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{r1}, r1, nil, nodes)
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r2}, func(nodes NodeMap) (error) {
    node_state := r2.State().(LockableState)
    err := LockLockables(ctx, []Lockable{r1}, r2, node_state, nodes)
    if err == nil {
      t.Fatal("r2 took r1's lock from itself")
    }

    return nil
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1}, func(states NodeStateMap) (error) {
    owner_id := states[r1.ID()].(LockableState).Owner().ID()
    if owner_id != r1.ID() {
      return fmt.Errorf("r1 is owned by %s instead of r1", owner_id)
    }

    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r1}, func(nodes NodeMap) error {
    return UnlockLockables(ctx, []Lockable{r1}, r1, nil, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []GraphNode{r1}, func(states NodeStateMap) (error) {
    owner := states[r1.ID()].(LockableState).Owner()
    if owner != nil {
      return fmt.Errorf("r1 is owned by %s instead of r1", owner.ID())
    }

    return nil
  })
  fatalErr(t, err)

}

func TestLockableLockTieredConflict(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewSimpleBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewSimpleBaseLockable(ctx, "Test lockable 2", []Lockable{r1})
  fatalErr(t, err)

  r3, err := NewSimpleBaseLockable(ctx, "Test lockable 3", []Lockable{r1})
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r2}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{r2}, r2, nil, nodes)
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []GraphNode{r3}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{r3}, r3, nil, nodes)
  })
  if err == nil {
    t.Fatal("Locked r3 which depends on r1 while r2 which depends on r1 is already locked")
  }
}

func TestLockableSimpleUpdate(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  update_channel := l1.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l1, NewDirectSignal(l1, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l1, 100*time.Millisecond, "Didn't receive test_update sent to l1")
}

func TestLockableDownUpdate(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  l2, err := NewSimpleBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)

  _, err = NewSimpleBaseLockable(ctx, "Test Lockable 3", []Lockable{l2})
  fatalErr(t, err)

  update_channel := l1.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l2, NewDownSignal(l2, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l2, 100*time.Millisecond, "Didn't receive test_update on l3 sent on l2")
}

func TestLockableUpUpdate(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  l2, err := NewSimpleBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)

  l3, err := NewSimpleBaseLockable(ctx, "Test Lockable 3", []Lockable{l2})
  fatalErr(t, err)

  update_channel := l3.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l2, NewSignal(l2, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l2, 100*time.Millisecond, "Didn't receive test_update on l3 sent on l2")
}

func TestOwnerNotUpdatedTwice(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  l2, err := NewSimpleBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)

  update_channel := l2.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l1, NewSignal(l1, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l1, 100*time.Millisecond, "Dicn't received test_update on l2 from l1")
  (*GraphTester)(t).CheckForNone(update_channel, "Second update received on dependency")
}

func TestLockableDependencyOverlap(t * testing.T) {
  ctx := testContext(t)
  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)
  l2, err := NewSimpleBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)
  _, err = NewSimpleBaseLockable(ctx, "Test Lockable 3", []Lockable{l1, l2})
  if err == nil {
    t.Fatal("Should have thrown an error because of dependency overlap")
  }
}

func TestLockableDBLoad(t * testing.T){
  ctx := testContext(t)
  l1, err := NewSimpleBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)
  l2, err := NewSimpleBaseLockable(ctx, "Test Lockable 2", []Lockable{})
  fatalErr(t, err)
  l3, err := NewSimpleBaseLockable(ctx, "Test Lockable 3", []Lockable{l1, l2})
  fatalErr(t, err)
  l4, err := NewSimpleBaseLockable(ctx, "Test Lockable 4", []Lockable{l3})
  fatalErr(t, err)
  _, err = NewSimpleBaseLockable(ctx, "Test Lockable 5", []Lockable{l4})
  fatalErr(t, err)
  l6, err := NewSimpleBaseLockable(ctx, "Test Lockable 6", []Lockable{})
  err = UpdateStates(ctx, []GraphNode{l6, l3}, func(nodes NodeMap) error {
    l6_state := l6.State().(LockableState)
    err := LockLockables(ctx, []Lockable{l3}, l6, l6_state, nodes)
    return err
  })
  fatalErr(t, err)

  _, err = LoadNode(ctx, l3.ID())
  fatalErr(t, err)
}
