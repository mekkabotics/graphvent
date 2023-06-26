package graphvent

import (
  "testing"
  "fmt"
  "encoding/json"
  "time"
)

func TestNewBaseLockable(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  _, err = NewBaseLockable(ctx, "Test lockable 2", []Lockable{r1})
  fatalErr(t, err)
}

func TestRepeatedChildLockable(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  _, err = NewBaseLockable(ctx, "Test lockable 2", []Lockable{r1, r1})
  if err == nil {
    t.Fatal("Added the same lockable as a child twice to the same lockable")
  }
}

func TestLockableSelfLock(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  err = LockLockable(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner_id := states[0].(LockableState).Owner().ID()
    if owner_id != r1.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of self", owner_id)
    }
    return nil, nil
  })
  fatalErr(t, err)

  err = UnlockLockable(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner := states[0].(LockableState).Owner()
    if owner != nil {
      return nil, fmt.Errorf("r1 is not unowned after unlock: %s", owner.ID())
    }
    return nil, nil
  })

  fatalErr(t, err)
}

func TestLockableSelfLockTiered(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewBaseLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  r3, err := NewBaseLockable(ctx, "Test lockable 3", []Lockable{r1, r2})
  fatalErr(t, err)

  err = LockLockable(ctx, r3, r3, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1, r2, r3}, func(states []NodeState) (interface{}, error) {
    owner_1_id := states[0].(LockableState).Owner().ID()
    if owner_1_id != r3.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of r3", owner_1_id)
    }

    owner_2_id := states[1].(LockableState).Owner().ID()
    if owner_2_id != r3.ID() {
      return nil, fmt.Errorf("r2 is owned by %s instead of r3", owner_2_id)
    }
    ser, _ := json.MarshalIndent(states, "", "  ")
    fmt.Printf("\n\n%s\n\n", ser)

    return nil, nil
  })
  fatalErr(t, err)

  err = UnlockLockable(ctx, r3, r3, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1, r2, r3}, func(states []NodeState) (interface{}, error) {
    owner_1 := states[0].(LockableState).Owner()
    if owner_1 != nil {
      return nil, fmt.Errorf("r1 is not unowned after unlocking: %s", owner_1.ID())
    }

    owner_2 := states[1].(LockableState).Owner()
    if owner_2 != nil {
      return nil, fmt.Errorf("r2 is not unowned after unlocking: %s", owner_2.ID())
    }

    owner_3 := states[2].(LockableState).Owner()
    if owner_3 != nil {
      return nil, fmt.Errorf("r3 is not unowned after unlocking: %s", owner_3.ID())
    }
    return nil, nil
  })

  fatalErr(t, err)
}

func TestLockableLockOther(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewBaseLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  _, err = UpdateStates(ctx, []GraphNode{r2}, func(states []NodeState) ([]NodeState, interface{}, error) {
    node_state := states[0].(LockHolderState)
    err := LockLockable(ctx, r1, r2, node_state)
    fatalErr(t, err)
    return []NodeState{node_state}, nil, nil
  })
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner_id := states[0].(LockableState).Owner().ID()
    if owner_id != r2.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of r2", owner_id)
    }

    return nil, nil
  })
  fatalErr(t, err)

  _, err = UpdateStates(ctx, []GraphNode{r2}, func(states []NodeState) ([]NodeState, interface{}, error) {
    node_state := states[0].(LockHolderState)
    err := UnlockLockable(ctx, r1, r2, node_state)
    fatalErr(t, err)
    return []NodeState{node_state}, nil, nil
  })
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner := states[0].(LockableState).Owner()
    if owner != nil {
      return nil, fmt.Errorf("r1 is owned by %s instead of r2", owner.ID())
    }

    return nil, nil
  })
  fatalErr(t, err)

}

func TestLockableLockSimpleConflict(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewBaseLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  err = LockLockable(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UpdateStates(ctx, []GraphNode{r2}, func(states []NodeState) ([]NodeState, interface{}, error) {
    node_state := states[0].(LockHolderState)
    err := LockLockable(ctx, r1, r2, node_state)
    if err == nil {
      t.Fatal("r2 took r1's lock from itself")
    }

    return []NodeState{node_state}, nil, nil
  })
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner_id := states[0].(LockableState).Owner().ID()
    if owner_id != r1.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of r1", owner_id)
    }

    return nil, nil
  })
  fatalErr(t, err)

  err = UnlockLockable(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner := states[0].(LockableState).Owner()
    if owner != nil {
      return nil, fmt.Errorf("r1 is owned by %s instead of r1", owner.ID())
    }

    return nil, nil
  })
  fatalErr(t, err)

}

func TestLockableLockTieredConflict(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewBaseLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewBaseLockable(ctx, "Test lockable 2", []Lockable{r1})
  fatalErr(t, err)

  r3, err := NewBaseLockable(ctx, "Test lockable 3", []Lockable{r1})
  fatalErr(t, err)

  err = LockLockable(ctx, r2, r2, nil)
  fatalErr(t, err)

  err = LockLockable(ctx, r3, r3, nil)
  if err == nil {
    t.Fatal("Locked r3 which depends on r1 while r2 which depends on r1 is already locked")
  }
}

func TestLockableSimpleUpdate(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  update_channel := l1.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l1, NewDirectSignal(l1, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l1, 100*time.Millisecond, "Didn't receive test_update sent to l1")
}

func TestLockableDownUpdate(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  l2, err := NewBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)

  _, err = NewBaseLockable(ctx, "Test Lockable 3", []Lockable{l2})
  fatalErr(t, err)

  update_channel := l1.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l2, NewDownSignal(l2, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l2, 100*time.Millisecond, "Didn't receive test_update on l3 sent on l2")
}

func TestLockableUpUpdate(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  l2, err := NewBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)

  l3, err := NewBaseLockable(ctx, "Test Lockable 3", []Lockable{l2})
  fatalErr(t, err)

  update_channel := l3.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l2, NewSignal(l2, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l2, 100*time.Millisecond, "Didn't receive test_update on l3 sent on l2")
}

func TestOwnerNotUpdatedTwice(t * testing.T) {
  ctx := testContext(t)

  l1, err := NewBaseLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  l2, err := NewBaseLockable(ctx, "Test Lockable 2", []Lockable{l1})
  fatalErr(t, err)

  update_channel := l2.UpdateChannel(0)

  go func() {
    SendUpdate(ctx, l1, NewSignal(l1, "test_update"))
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l1, 100*time.Millisecond, "Dicn't received test_update on l2 from l1")
  (*GraphTester)(t).CheckForNone(update_channel, "Second update received on dependency")
}
