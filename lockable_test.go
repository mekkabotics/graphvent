package graphvent

import (
  "testing"
  "time"
)

const TestLockableType = NodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T, logs []string) *Context {
  ctx := logTestContext(t, logs)

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{LockableExtType})
  fatalErr(t, err)

  return ctx
}

func TestLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{"lockable"})

  l2_listener := NewListenerExt(10)
  l2 := NewNode(ctx, nil, TestLockableType, 10, nil,
                 l2_listener,
                 NewLockableExt(nil),
               )
  l1_listener := NewListenerExt(10)
  NewNode(ctx, nil, TestLockableType, 10, nil,
                 l1_listener,
                 NewLockableExt([]NodeID{l2.ID}),
               )

  msgs := Messages{}
  msgs = msgs.Add(l2.ID, l2.Key, NewStatusSignal("TEST", l2.ID), l2.ID)
  err := ctx.Send(msgs)
  fatalErr(t, err)

  _, err = WaitForSignal(ctx, l1_listener.Chan, time.Millisecond*10, StatusSignalType, func(sig *IDStringSignal) bool {
    return sig.Str == "TEST"
  })
  fatalErr(t, err)

  _, err = WaitForSignal(ctx, l2_listener.Chan, time.Millisecond*10, StatusSignalType, func(sig *IDStringSignal) bool {
    return sig.Str == "TEST"
  })
  fatalErr(t, err)
}

func TestLink10K(t *testing.T) {
  ctx := lockableTestContext(t, []string{})

  NewLockable := func()(*Node) {
    l := NewNode(ctx, nil, TestLockableType, 10, nil,
                  NewLockableExt(nil),
                )
    return l
  }

  reqs := make([]NodeID, 10000)
  for i, _ := range(reqs) {
    new_lockable := NewLockable()
    reqs[i] = new_lockable.ID
  }
  ctx.Log.Logf("test", "CREATED_10K")

  NewListener := func()(*ListenerExt) {
    listener := NewListenerExt(100000)
    NewNode(ctx, nil, TestLockableType, 256, nil,
                  listener,
                  NewLockableExt(reqs),
                )
    return listener
  }
  NewListener()
  ctx.Log.Logf("test", "CREATED_LISTENER")

  // TODO: Lock listener and wait for all the lock signals
  //ctx.Log.Logf("test", "LOCKED_10K")
}

func TestLock(t *testing.T) {
  ctx := lockableTestContext(t, []string{})

  NewLockable := func(reqs []NodeID)(*Node, *ListenerExt) {
    listener := NewListenerExt(100)
    l := NewNode(ctx, nil, TestLockableType, 10, nil,
                  listener,
                  NewLockableExt(reqs),
                )
    return l, listener
  }

  l2, _ := NewLockable(nil)
  l3, _ := NewLockable(nil)
  l4, _ := NewLockable(nil)
  l5, _ := NewLockable(nil)
  NewLockable([]NodeID{l2.ID, l3.ID, l4.ID, l5.ID})
  l1, l1_listener := NewLockable([]NodeID{l2.ID, l3.ID, l4.ID, l5.ID})

  locked := func(sig *StringSignal) bool {
    return sig.Str == "locked"
  }

  err := LockLockable(ctx, l1)
  fatalErr(t, err)
  _, err = WaitForSignal(ctx, l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(ctx, l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(ctx, l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(ctx, l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(ctx, l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)

  err = UnlockLockable(ctx, l1)
  fatalErr(t, err)
}
