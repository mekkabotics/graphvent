package graphvent

import (
  "testing"
  "time"
)

const TestLockableType = NodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T, logs []string) *Context {
  ctx := logTestContext(t, logs)

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{ACLExtType, LockableExtType, ListenerExtType})
  fatalErr(t, err)

  return ctx
}


var link_policy = NewAllNodesPolicy([]SignalType{LinkSignalType, StatusSignalType})
var lock_policy = NewAllNodesPolicy([]SignalType{LinkSignalType, LockSignalType, StatusSignalType})

func TestLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{})

  l1_listener := NewListenerExt(10)
  l1 := NewNode(ctx, RandID(), TestLockableType, nil,
                 l1_listener,
                 NewACLExt(&link_policy),
                 NewLockableExt(),
               )
  l2_listener := NewListenerExt(10)
  l2 := NewNode(ctx, RandID(), TestLockableType, nil,
                 l2_listener,
                 NewACLExt(&link_policy),
                 NewLockableExt(),
               )

  // Link l2 as a requirement of l1
  err := LinkRequirement(ctx, l1, l2.ID)
  fatalErr(t, err)

  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "dep_linked", time.Millisecond*10, "No dep_link")
  (*GraphTester)(t).WaitForState(ctx, l2_listener, LinkSignalType, "req_linked", time.Millisecond*10, "No req_linked")

  err = ctx.Send(l2.ID, l2.ID, NewStatusSignal("TEST", l2.ID))
  fatalErr(t, err)

  (*GraphTester)(t).WaitForStatus(ctx, l1_listener, "TEST", time.Millisecond*10, "No TEST on l1")
  (*GraphTester)(t).WaitForStatus(ctx, l2_listener, "TEST", time.Millisecond*10, "No TEST on l2")
}

func TestLock(t *testing.T) {
  ctx := lockableTestContext(t, []string{"test", "lockable"})

  l1_listener := NewListenerExt(10)
  l1 := NewNode(ctx, RandID(), TestLockableType, nil,
                 l1_listener,
                 NewACLExt(&link_policy),
                 NewLockableExt(),
               )
  l2_listener := NewListenerExt(10)
  l2 := NewNode(ctx, RandID(), TestLockableType, nil,
                 l2_listener,
                 NewACLExt(&link_policy),
                 NewLockableExt(),
               )

  err := LinkRequirement(ctx, l1, l2.ID)
  fatalErr(t, err)
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "dep_linked", time.Millisecond*10, "No dep_link")
  (*GraphTester)(t).WaitForState(ctx, l2_listener, LinkSignalType, "req_linked", time.Millisecond*10, "No req_linked")

  err = LockLockable(ctx, l1)
  fatalErr(t, err)
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LockSignalType, "locked", time.Millisecond*10, "No locked")
}
