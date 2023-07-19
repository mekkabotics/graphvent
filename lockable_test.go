package graphvent

import (
  "testing"
  "fmt"
  "time"
)

func TestNewSimpleLockable(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test lockable 2")
  l2 := &l2_r

  err := UpdateStates(ctx, []Node{l1, l2}, func(nodes NodeMap) error {
    return LinkLockables(ctx, l2, []Lockable{l1}, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1, l2}, func(nodes NodeMap) error {
    l1_deps := len(l1.Dependencies())
    if l1_deps != 1 {
      return fmt.Errorf("l1 has wront amount of dependencies %d/1", l1_deps)
    }

    l1_dep1 := l1.Dependencies()[0]
    if l1_dep1.ID() != l2.ID() {
      return fmt.Errorf("Wrong dependency for l1, %s instead of %s", l1_dep1.ID(), l2.ID())
    }

    l2_reqs := len(l2.Requirements())
    if l2_reqs != 1 {
      return fmt.Errorf("l2 has wrong amount of requirements %d/1", l2_reqs)
    }

    l2_req1 := l2.Requirements()[0]
    if l2_req1.ID() != l1.ID() {
      return fmt.Errorf("Wrong requirement for l2, %s instead of %s", l2_req1.ID(), l1.ID())
    }
    return nil
  })
  fatalErr(t, err)
}

func TestRepeatedChildLockable(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r

  l2_r := NewSimpleLockable(RandID(), "Test lockable 2")
  l2 := &l2_r

  err := UpdateStates(ctx, []Node{l1, l2}, func(nodes NodeMap) error {
    return LinkLockables(ctx, l2, []Lockable{l1, l1}, nodes)
  })

  if err == nil {
    t.Fatal("Added the same lockable as a requirement twice to the same lockable")
  }
}

func TestLockableSelfLock(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r

  err := UpdateStates(ctx, []Node{l1}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{l1}, l1, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    owner_id := NodeID("")
    if l1.owner != nil {
      owner_id = l1.owner.ID()
    }
    if owner_id != l1.ID() {
      return fmt.Errorf("l1 is owned by %s instead of self", owner_id)
    }
    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l1}, func(nodes NodeMap) error {
    return UnlockLockables(ctx, []Lockable{l1}, l1, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    if l1.owner != nil {
      return fmt.Errorf("l1 is not unowned after unlock: %s", l1.owner.ID())
    }
    return nil
  })

  fatalErr(t, err)
}

func TestLockableSelfLockTiered(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test lockable 2")
  l2 := &l2_r
  l3_r := NewSimpleLockable(RandID(), "Test lockable 3")
  l3 := &l3_r

  err := UpdateStates(ctx, []Node{l3}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l3, []Lockable{l1, l2}, nodes)
    if err != nil {
      return err
    }
    return LockLockables(ctx, []Lockable{l3}, l3, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1, l2, l3}, func(nodes NodeMap) (error) {
    owner_1 := NodeID("")
    if l1.owner != nil {
      owner_1 = l1.owner.ID()
    }
    if owner_1 != l3.ID() {
      return fmt.Errorf("l1 is owned by %s instead of l3", owner_1)
    }

    owner_2 := NodeID("")
    if l2.owner != nil {
      owner_2 = l2.owner.ID()
    }
    if owner_2 != l3.ID() {
      return fmt.Errorf("l2 is owned by %s instead of l3", owner_2)
    }
    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l3}, func(nodes NodeMap) error {
    return UnlockLockables(ctx, []Lockable{l3}, l3, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1, l2, l3}, func(nodes NodeMap) (error) {
    owner_1 := l1.owner
    if owner_1 != nil {
      return fmt.Errorf("l1 is not unowned after unlocking: %s", owner_1.ID())
    }

    owner_2 := l2.owner
    if owner_2 != nil {
      return fmt.Errorf("l2 is not unowned after unlocking: %s", owner_2.ID())
    }

    owner_3 := l3.owner
    if owner_3 != nil {
      return fmt.Errorf("l3 is not unowned after unlocking: %s", owner_3.ID())
    }
    return nil
  })

  fatalErr(t, err)
}

func TestLockableLockOther(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test lockable 2")
  l2 := &l2_r

  err := UpdateStates(ctx, []Node{l1, l2}, func(nodes NodeMap) (error) {
    err := LockLockables(ctx, []Lockable{l1}, l2, nodes)
    fatalErr(t, err)
    return nil
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    owner_id := NodeID("")
    if l1.owner != nil {
      owner_id = l1.owner.ID()
    }
    if owner_id != l2.ID() {
      return fmt.Errorf("l1 is owned by %s instead of l2", owner_id)
    }

    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l2}, func(nodes NodeMap) (error) {
    err := UnlockLockables(ctx, []Lockable{l1}, l2, nodes)
    fatalErr(t, err)
    return nil
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    owner := l1.owner
    if owner != nil {
      return fmt.Errorf("l1 is owned by %s instead of l2", owner.ID())
    }

    return nil
  })
  fatalErr(t, err)

}

func TestLockableLockSimpleConflict(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test lockable 2")
  l2 := &l2_r

  err := UpdateStates(ctx, []Node{l1}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{l1}, l1, nodes)
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l2}, func(nodes NodeMap) (error) {
    err := LockLockables(ctx, []Lockable{l1}, l2, nodes)
    if err == nil {
      t.Fatal("l2 took l1's lock from itself")
    }

    return nil
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    owner_id := NodeID("")
    if l1.owner != nil {
      owner_id = l1.owner.ID()
    }
    if owner_id != l1.ID() {
      return fmt.Errorf("l1 is owned by %s instead of l1", owner_id)
    }

    return nil
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l1}, func(nodes NodeMap) error {
    return UnlockLockables(ctx, []Lockable{l1}, l1, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l1}, func(nodes NodeMap) (error) {
    owner := l1.owner
    if owner != nil {
      return fmt.Errorf("l1 is owned by %s instead of l1", owner.ID())
    }

    return nil
  })
  fatalErr(t, err)

}

func TestLockableLockTieredConflict(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test lockable 2")
  l2 := &l2_r
  l3_r := NewSimpleLockable(RandID(), "Test lockable 3")
  l3 := &l3_r

  err := UpdateStates(ctx, []Node{l1, l2, l3}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l2, []Lockable{l1}, nodes)
    if err != nil {
      return err
    }
    return LinkLockables(ctx, l3, []Lockable{l1}, nodes)
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l2}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{l2}, l2, nodes)
  })
  fatalErr(t, err)

  err = UpdateStates(ctx, []Node{l3}, func(nodes NodeMap) error {
    return LockLockables(ctx, []Lockable{l3}, l3, nodes)
  })
  if err == nil {
    t.Fatal("Locked l3 which depends on l1 while l2 which depends on l1 is already locked")
  }
}

func TestLockableSimpleUpdate(t * testing.T) {
  ctx := logTestContext(t, []string{})

  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r


  update_channel := UpdateChannel(l1, 1, "test")

  go func() {
    UseStates(ctx, []Node{l1}, func(nodes NodeMap) error {
      return l1.Signal(ctx, NewDirectSignal(l1, "test_update"), nodes)
    })
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l1, 100*time.Millisecond, "Didn't receive test_update sent to l1")
}

func TestLockableDownUpdate(t * testing.T) {
  ctx := logTestContext(t, []string{})

  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test Lockable 2")
  l2 := &l2_r
  l3_r := NewSimpleLockable(RandID(), "Test Lockable 3")
  l3 := &l3_r
  err := UpdateStates(ctx, []Node{l1, l2, l3}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l2, []Lockable{l1}, nodes)
    if err != nil {
      return err
    }
    return LinkLockables(ctx, l3, []Lockable{l2}, nodes)
  })
  fatalErr(t, err)

  update_channel := UpdateChannel(l1, 1, "test")

  go func() {
    UseStates(ctx, []Node{l2}, func(nodes NodeMap) error {
      return l2.Signal(ctx, NewDownSignal(l2, "test_update"), nodes)
    })
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l2, 100*time.Millisecond, "Didn't receive test_update on l3 sent on l2")
}

func TestLockableUpUpdate(t * testing.T) {
  ctx := logTestContext(t, []string{})

  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test Lockable 2")
  l2 := &l2_r
  l3_r := NewSimpleLockable(RandID(), "Test Lockable 3")
  l3 := &l3_r
  err := UpdateStates(ctx, []Node{l1, l2, l3}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l2, []Lockable{l1}, nodes)
    if err != nil {
      return err
    }
    return LinkLockables(ctx, l3, []Lockable{l2}, nodes)
  })
  fatalErr(t, err)

  update_channel := UpdateChannel(l3, 1, "test")

  go func() {
    UseStates(ctx, []Node{l2}, func(nodes NodeMap) error {
      return l2.Signal(ctx, NewSignal(l2, "test_update"), nodes)
    })
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l2, 100*time.Millisecond, "Didn't receive test_update on l3 sent on l2")
}

func TestOwnerNotUpdatedTwice(t * testing.T) {
  ctx := logTestContext(t, []string{})

  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test Lockable 2")
  l2 := &l2_r

  err := UpdateStates(ctx, []Node{l1, l2}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l2, []Lockable{l1}, nodes)
    if err != nil {
      return err
    }
    return LockLockables(ctx, []Lockable{l2}, l2, nodes)
  })
  fatalErr(t, err)

  update_channel := UpdateChannel(l2, 1, "test")

  go func() {
    err := UseStates(ctx, []Node{l1}, func(nodes NodeMap) error {
      return l1.Signal(ctx, NewSignal(l1, "test_update"), nodes)
    })
    fatalErr(t, err)
  }()

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "test_update", l1, 100*time.Millisecond, "Dicn't received test_update on l2 from l1")
  (*GraphTester)(t).CheckForNone(update_channel, "Second update received on dependency")
}

func TestLockableDependencyOverlap(t * testing.T) {
  ctx := testContext(t)

  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test Lockable 2")
  l2 := &l2_r
  l3_r := NewSimpleLockable(RandID(), "Test Lockable 3")
  l3 := &l3_r

  err := UpdateStates(ctx, []Node{l1, l2, l3}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l2, []Lockable{l1}, nodes)
    if err != nil {
      return err
    }

    return LinkLockables(ctx, l3, []Lockable{l2, l1}, nodes)
  })
  if err == nil {
    t.Fatal("Should have thrown an error because of dependency overlap")
  }
}

func TestLockableDBLoad(t * testing.T){
  ctx := logTestContext(t, []string{})
  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test Lockable 2")
  l2 := &l2_r
  l3_r := NewSimpleLockable(RandID(), "Test Lockable 3")
  l3 := &l3_r
  l4_r := NewSimpleLockable(RandID(), "Test Lockable 4")
  l4 := &l4_r
  l5_r := NewSimpleLockable(RandID(), "Test Lockable 5")
  l5 := &l5_r
  l6_r := NewSimpleLockable(RandID(), "Test Lockable 6")
  l6 := &l6_r
  err := UpdateStates(ctx, []Node{l1, l2, l3, l4, l5, l6}, func(nodes NodeMap) error {
    err := LinkLockables(ctx, l3, []Lockable{l1, l2}, nodes)
    if err != nil {
      return err
    }

    err = LinkLockables(ctx, l4, []Lockable{l3}, nodes)
    if err != nil {
      return err
    }

    err = LinkLockables(ctx, l5, []Lockable{l4}, nodes)
    if err != nil {
      return err
    }
    return LockLockables(ctx, []Lockable{l3}, l6, nodes)
  })
  fatalErr(t, err)

  err = UseStates(ctx, []Node{l3}, func(nodes NodeMap) error {
    ser, err := l3.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    return err
  })
  fatalErr(t, err)

  l3_loaded, err := LoadNode(ctx, l3.ID())
  fatalErr(t, err)

  // TODO: add more equivalence checks
  err = UseStates(ctx, []Node{l3_loaded}, func(nodes NodeMap) error {
    ser, err := l3_loaded.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    return err
  })
  fatalErr(t, err)
}

func TestLockableUnlink(t * testing.T){
  ctx := logTestContext(t, []string{})
  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  l2_r := NewSimpleLockable(RandID(), "Test Lockable 2")
  l2 := &l2_r

  err := UpdateStates(ctx, []Node{l1, l2}, func(nodes NodeMap) error {
    return LinkLockables(ctx, l2, []Lockable{l1}, nodes)
  })
  fatalErr(t, err)

  err = UnlinkLockables(ctx, l2, l1)
  fatalErr(t, err)
}
