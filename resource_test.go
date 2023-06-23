package graphvent

import (
  "testing"
  "fmt"
)

func TestNewResource(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  _, err = NewResource(ctx, "Test resource 2", []Resource{r1})
  fatalErr(t, err)
}

func TestRepeatedChildResource(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  _, err = NewResource(ctx, "Test resource 2", []Resource{r1, r1})
  if err == nil {
    t.Fatal("Added the same resource as a child twice to the same resource")
  }
}

func TestResourceSelfLock(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  _, err = LockResource(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner_id := states[0].(ResourceState).owner.ID()
    if owner_id != r1.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of self", owner_id)
    }
    return nil, nil
  })
  fatalErr(t, err)

  _, err = UnlockResource(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner := states[0].(ResourceState).owner
    if owner != nil {
      return nil, fmt.Errorf("r1 is not unowned after unlock: %s", owner.ID())
    }
    return nil, nil
  })

  fatalErr(t, err)
}

func TestResourceSelfLockTiered(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  r2, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  r3, err := NewResource(ctx, "Test resource 3", []Resource{r1, r2})
  fatalErr(t, err)

  _, err = LockResource(ctx, r3, r3, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1, r2}, func(states []NodeState) (interface{}, error) {
    owner_1_id := states[0].(ResourceState).owner.ID()
    if owner_1_id != r3.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of r3", owner_1_id)
    }

    owner_2_id := states[1].(ResourceState).owner.ID()
    if owner_2_id != r3.ID() {
      return nil, fmt.Errorf("r2 is owned by %s instead of r3", owner_2_id)
    }
    return nil, nil
  })
  fatalErr(t, err)

  _, err = UnlockResource(ctx, r3, r3, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1, r2, r3}, func(states []NodeState) (interface{}, error) {
    owner_1 := states[0].(ResourceState).owner
    if owner_1 != nil {
      return nil, fmt.Errorf("r1 is not unowned after unlocking: %s", owner_1.ID())
    }

    owner_2 := states[1].(ResourceState).owner
    if owner_2 != nil {
      return nil, fmt.Errorf("r2 is not unowned after unlocking: %s", owner_2.ID())
    }

    owner_3 := states[2].(ResourceState).owner
    if owner_3 != nil {
      return nil, fmt.Errorf("r3 is not unowned after unlocking: %s", owner_3.ID())
    }
    return nil, nil
  })

  fatalErr(t, err)
}

func TestResourceLockOther(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  r2, err := NewResource(ctx, "Test resource 2", []Resource{})
  fatalErr(t, err)

  _, err = UpdateStates(ctx, []GraphNode{r2}, func(states []NodeState) ([]NodeState, interface{}, error) {
    new_state, err := LockResource(ctx, r1, r2, states[0])
    fatalErr(t, err)
    return []NodeState{new_state}, nil, nil
  })
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner_id := states[0].(ResourceState).owner.ID()
    if owner_id != r2.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of r2", owner_id)
    }

    return nil, nil
  })
  fatalErr(t, err)

  _, err = UpdateStates(ctx, []GraphNode{r2}, func(states []NodeState) ([]NodeState, interface{}, error) {
    new_state, err := UnlockResource(ctx, r1, r2, states[0])
    fatalErr(t, err)
    return []NodeState{new_state}, nil, nil
  })
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner := states[0].(ResourceState).owner
    if owner != nil {
      return nil, fmt.Errorf("r1 is owned by %s instead of r2", owner.ID())
    }

    return nil, nil
  })
  fatalErr(t, err)

}

func TestResourceLockSimpleConflict(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  r2, err := NewResource(ctx, "Test resource 2", []Resource{})
  fatalErr(t, err)

  _, err = LockResource(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UpdateStates(ctx, []GraphNode{r2}, func(states []NodeState) ([]NodeState, interface{}, error) {
    new_state, err := LockResource(ctx, r1, r2, states[0])
    if err == nil {
      t.Fatal("r2 took r1's lock from itself")
    }

    return []NodeState{new_state}, nil, nil
  })
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner_id := states[0].(ResourceState).owner.ID()
    if owner_id != r1.ID() {
      return nil, fmt.Errorf("r1 is owned by %s instead of r1", owner_id)
    }

    return nil, nil
  })
  fatalErr(t, err)

  _, err = UnlockResource(ctx, r1, r1, nil)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{r1}, func(states []NodeState) (interface{}, error) {
    owner := states[0].(ResourceState).owner
    if owner != nil {
      return nil, fmt.Errorf("r1 is owned by %s instead of r1", owner.ID())
    }

    return nil, nil
  })
  fatalErr(t, err)

}

func TestResourceLockTieredConflict(t * testing.T) {
  ctx := testContext(t)

  r1, err := NewResource(ctx, "Test resource 1", []Resource{})
  fatalErr(t, err)

  r2, err := NewResource(ctx, "Test resource 2", []Resource{r1})
  fatalErr(t, err)

  r3, err := NewResource(ctx, "Test resource 3", []Resource{r1})
  fatalErr(t, err)

  _, err = LockResource(ctx, r2, r2, nil)
  fatalErr(t, err)

  _, err = LockResource(ctx, r3, r3, nil)
  if err == nil {
    t.Fatal("Locked r3 which depends on r1 while r2 which depends on r1 is already locked")
  }
}
