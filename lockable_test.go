package graphvent

import (
  "testing"
  "time"
)

const TestLockableType = NodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T) *Context {
  ctx := logTestContext(t, []string{"lockable", "test"})

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{ACLExtType, LockableExtType, ListenerExtType})
  fatalErr(t, err)

  return ctx
}


var link_policy = NewAllNodesPolicy([]SignalType{LinkSignalType})

func TestLinkStatus(t *testing.T) {
  ctx := lockableTestContext(t)

  l1_listener := NewListenerExt(10)
  l1 := NewNode(ctx, RandID(), TestLockableType, nil,
                 l1_listener,
                 NewACLExt(&link_policy),
                 NewLockableExt(nil, nil, nil, nil),
               )
  l2_listener := NewListenerExt(10)
  l2 := NewNode(ctx, RandID(), TestLockableType, nil,
                 l2_listener,
                 NewACLExt(&link_policy),
                 NewLockableExt(nil, nil, nil, nil),
               )

  // Link l2 as a requirement of l1
  err := LinkRequirement(ctx, l1, l2.ID)
  fatalErr(t, err)

  (*GraphTester)(t).WaitForLinkState(ctx, l1_listener, "dep_link", time.Millisecond*100, "No dep_link")
  (*GraphTester)(t).WaitForLinkState(ctx, l2_listener, "req_linked", time.Millisecond*100, "No req_linked")
}
