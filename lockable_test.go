package graphvent

import (
  "testing"
  "time"
)

func TestLink(t *testing.T) {
  ctx := logTestContext(t, []string{"lockable", "listener"})


  l2_listener := NewListenerExt(10)
  l2, err := NewNode(ctx, nil, "LockableNode", 10, l2_listener, NewLockableExt(nil))
  fatalErr(t, err)

  l1_lockable := NewLockableExt(nil)
  l1_listener := NewListenerExt(10)
  l1, err := NewNode(ctx, nil, "LockableNode", 10, l1_listener, l1_lockable)
  fatalErr(t, err)

  link_signal := NewLinkSignal("add", l2.ID)
  msgs := []SendMsg{{l1.ID, link_signal}}
  err = ctx.Send(l1, msgs)
  fatalErr(t, err)

  _, _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, link_signal.ID())
  fatalErr(t, err)

  state, exists := l1_lockable.Requirements[l2.ID]
  if exists == false {
    t.Fatal("l2 not in l1 requirements")
  } else if state != Unlocked {
    t.Fatalf("l2 in bad requirement state in l1: %+v", state)
  }

  unlink_signal := NewLinkSignal("remove", l2.ID)
  msgs = []SendMsg{{l1.ID, unlink_signal}}
  err = ctx.Send(l1, msgs)
  fatalErr(t, err)

  _, _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, unlink_signal.ID())
  fatalErr(t, err)
}

func Test10Lock(t *testing.T) {
  testLockN(t, 10)
}

func Test100Lock(t *testing.T) {
  testLockN(t, 100)
}

func Test1000Lock(t *testing.T) {
  testLockN(t, 1000)
}

func Test10000Lock(t *testing.T) {
  testLockN(t, 10000)
}

func testLockN(t *testing.T, n int) {
  ctx := logTestContext(t, []string{"test"})

  NewLockable := func()(*Node) {
    l, err := NewNode(ctx, nil, "LockableNode", 10, NewLockableExt(nil))
    fatalErr(t, err)
    return l
  }

  reqs := make([]NodeID, n)
  for i := range(reqs) {
    new_lockable := NewLockable()
    reqs[i] = new_lockable.ID
  }
  ctx.Log.Logf("test", "CREATED_%d", n)

  listener := NewListenerExt(50000)
  node, err := NewNode(ctx, nil, "LockableNode", 500000, listener, NewLockableExt(reqs))
  fatalErr(t, err)
  ctx.Log.Logf("test", "CREATED_LISTENER")

  lock_id, err := LockLockable(ctx, node)
  fatalErr(t, err)

  response, _, err := WaitForResponse(listener.Chan, time.Second*60, lock_id)
  fatalErr(t, err)

  switch resp := response.(type) {
  case *SuccessSignal:
  default:
    t.Fatalf("Unexpected response to lock - %s", resp)
  }

  ctx.Log.Logf("test", "LOCKED_%d", n)
}

func TestLock(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "lockable"})

  NewLockable := func(reqs []NodeID)(*Node, *ListenerExt) {
    listener := NewListenerExt(10000)
    l, err := NewNode(ctx, nil, "LockableNode", 10, listener, NewLockableExt(reqs))
    fatalErr(t, err)
    return l, listener
  }

  l2, _ := NewLockable(nil)
  l3, _ := NewLockable(nil)
  l4, _ := NewLockable(nil)
  l5, _ := NewLockable(nil)
  l0, l0_listener := NewLockable([]NodeID{l5.ID})
  l1, l1_listener := NewLockable([]NodeID{l2.ID, l3.ID, l4.ID, l5.ID})

  ctx.Log.Logf("test", "l0: %s", l0.ID)
  ctx.Log.Logf("test", "l1: %s", l1.ID)
  ctx.Log.Logf("test", "l2: %s", l2.ID)
  ctx.Log.Logf("test", "l3: %s", l3.ID)
  ctx.Log.Logf("test", "l4: %s", l4.ID)
  ctx.Log.Logf("test", "l5: %s", l5.ID)

  ctx.Log.Logf("test", "locking l0")
  id_1, err := LockLockable(ctx, l0)
  fatalErr(t, err)
  response, _, err := WaitForResponse(l0_listener.Chan, time.Millisecond*10, id_1)
  fatalErr(t, err)
  ctx.Log.Logf("test", "l0 lock: %+v", response)

  ctx.Log.Logf("test", "locking l1")
  id_2, err := LockLockable(ctx, l1)
  fatalErr(t, err)
  response, _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10000, id_2)
  fatalErr(t, err)
  ctx.Log.Logf("test", "l1 lock: %+v", response)

  ctx.Log.Logf("test", "unlocking l0")
  id_3, err := UnlockLockable(ctx, l0)
  fatalErr(t, err)
  response, _, err = WaitForResponse(l0_listener.Chan, time.Millisecond*10, id_3)
  fatalErr(t, err)
  ctx.Log.Logf("test", "l0 unlock: %+v", response)

  ctx.Log.Logf("test", "locking l1")
  id_4, err := LockLockable(ctx, l1)
  fatalErr(t, err)
  response, _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, id_4)
  fatalErr(t, err)
  ctx.Log.Logf("test", "l1 lock: %+v", response)
}
