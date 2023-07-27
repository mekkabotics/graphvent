package graphvent

import (
  "testing"
)

const TestLockableType = NodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T) *Context {
  ctx := logTestContext(t, []string{"lockable", "signal"})

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{ACLExtType, LockableExtType, ListenerExtType})
  fatalErr(t, err)

  return ctx
}


var link_policy = NewAllNodesPolicy([]string{"link", "status"})

func Test(t *testing.T) {
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

  ctx.Send(l1.ID, l2.ID, NewLinkSignal("start", l1.ID))
}

