package graphvent

import (
  "testing"
  "fmt"
  "encoding/json"
)

func TestNewLockable(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  _, err = NewLockable(ctx, "Test lockable 2", []Lockable{r1})
  fatalErr(t, err)
}

func TestRepeatedChildLockable(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  _, err = NewLockable(ctx, "Test lockable 2", []Lockable{r1, r1})
  if err == nil {
    t.Fatal("Added the same lockable as a child twice to the same lockable")
  }
}

func TestLockableSelfLock(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
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

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewLockable(ctx, "Test lockable 2", []Lockable{})
  fatalErr(t, err)

  r3, err := NewLockable(ctx, "Test lockable 3", []Lockable{r1, r2})
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

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewLockable(ctx, "Test lockable 2", []Lockable{})
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

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewLockable(ctx, "Test lockable 2", []Lockable{})
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

  r1, err := NewLockable(ctx, "Test lockable 1", []Lockable{})
  fatalErr(t, err)

  r2, err := NewLockable(ctx, "Test lockable 2", []Lockable{r1})
  fatalErr(t, err)

  r3, err := NewLockable(ctx, "Test lockable 3", []Lockable{r1})
  fatalErr(t, err)

  err = LockLockable(ctx, r2, r2, nil)
  fatalErr(t, err)

  err = LockLockable(ctx, r3, r3, nil)
  if err == nil {
    t.Fatal("Locked r3 which depends on r1 while r2 which depends on r1 is already locked")
  }
}
