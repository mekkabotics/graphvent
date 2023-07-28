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


var link_policy = NewAllNodesPolicy([]Action{Action(LinkSignalType), Action(StatusSignalType)})
var lock_policy = NewAllNodesPolicy([]Action{Action(LockSignalType)})

func TestLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{"lockable"})

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

  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l2_listener, LinkSignalType, "linked_as_dep", time.Millisecond*10, "No req_linked")

  err = ctx.Send(l2.ID, l2.ID, NewStatusSignal("TEST", l2.ID))
  fatalErr(t, err)

  (*GraphTester)(t).WaitForStatus(ctx, l1_listener, "TEST", time.Millisecond*10, "No TEST on l1")
  (*GraphTester)(t).WaitForStatus(ctx, l2_listener, "TEST", time.Millisecond*10, "No TEST on l2")
}

func TestLock(t *testing.T) {
  ctx := lockableTestContext(t, []string{"policy"})

  NewLockable := func()(*Node, *ListenerExt) {
    listener := NewListenerExt(10)
    l := NewNode(ctx, RandID(), TestLockableType, nil,
                  listener,
                  NewACLExt(&lock_policy, &link_policy),
                  NewLockableExt(),
                )
    return l, listener
  }

  l0, l0_listener := NewLockable()
  l1, l1_listener := NewLockable()
  l2, _ := NewLockable()
  l3, _ := NewLockable()
  l4, _ := NewLockable()
  l5, _ := NewLockable()


  var err error
  err = LinkRequirement(ctx, l1, l2.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l1, l3.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l1, l4.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l1, l5.ID)
  fatalErr(t, err)

  err = LinkRequirement(ctx, l0, l2.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l0, l3.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l0, l4.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l0, l5.ID)
  fatalErr(t, err)

  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")

  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")

  err = LockLockable(ctx, l1)
  fatalErr(t, err)
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LockSignalType, "locked", time.Millisecond*10, "No locked")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LockSignalType, "locked", time.Millisecond*10, "No locked")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LockSignalType, "locked", time.Millisecond*10, "No locked")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LockSignalType, "locked", time.Millisecond*10, "No locked")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LockSignalType, "locked", time.Millisecond*10, "No locked")

  err = UnlockLockable(ctx, l1)
  fatalErr(t, err)
}
