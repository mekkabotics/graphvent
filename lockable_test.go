package graphvent

import (
  "testing"
  "time"
  "fmt"
)

const TestLockableType = NodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T, logs []string) *Context {
  ctx := logTestContext(t, logs)

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{ACLExtType, LockableExtType})
  fatalErr(t, err)

  return ctx
}


//TODO: add new finer grained signals, and probably add wildcards to not have to deal with annoying acl policies
var link_policy = NewAllNodesPolicy(Actions{MakeAction(LinkSignalType, "*"), MakeAction(StatusSignalType, "+")})
var lock_policy = NewAllNodesPolicy(Actions{MakeAction(LockSignalType, "*")})

func TestLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{})

  l1_listener := NewListenerExt(10)
  l1 := NewNode(ctx, RandID(), TestLockableType, 10, nil,
                 l1_listener,
                 NewACLExt(link_policy),
                 NewLockableExt(),
               )
  l2_listener := NewListenerExt(10)
  l2 := NewNode(ctx, RandID(), TestLockableType, 10, nil,
                 l2_listener,
                 NewACLExt(link_policy),
                 NewLockableExt(),
               )

  // Link l2 as a requirement of l1
  err := LinkRequirement(ctx, l1.ID, l2.ID)
  fatalErr(t, err)

  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*10, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l2_listener, LinkSignalType, "linked_as_dep", time.Millisecond*10, "No req_linked")

  err = ctx.Send(l2.ID, l2.ID, NewStatusSignal("TEST", l2.ID))
  fatalErr(t, err)

  (*GraphTester)(t).WaitForStatus(ctx, l1_listener, "TEST", time.Millisecond*10, "No TEST on l1")
  (*GraphTester)(t).WaitForStatus(ctx, l2_listener, "TEST", time.Millisecond*10, "No TEST on l2")
}

func TestLink10K(t *testing.T) {
  ctx := lockableTestContext(t, []string{"test"})

  NewLockable := func()(*Node) {
    l := NewNode(ctx, RandID(), TestLockableType, 10, nil,
                  NewACLExt(lock_policy, link_policy),
                  NewLockableExt(),
                )
    return l
  }

  NewListener := func()(*Node, *ListenerExt) {
    listener := NewListenerExt(100000)
    l := NewNode(ctx, RandID(), TestLockableType, 256, nil,
                  listener,
                  NewACLExt(lock_policy, link_policy),
                  NewLockableExt(),
                )
    return l, listener
  }

  l0, l0_listener := NewListener()
  lockables := make([]*Node, 10000)
  for i, _ := range(lockables) {
    lockables[i] = NewLockable()
    LinkRequirement(ctx, l0.ID, lockables[i].ID)
  }

  ctx.Log.Logf("test", "CREATED_10K")


  for i, _ := range(lockables) {
    (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*1000, fmt.Sprintf("No linked_as_req for %d", i))
  }

  ctx.Log.Logf("test", "LINKED_10K")
}

func TestLock(t *testing.T) {
  ctx := lockableTestContext(t, []string{})

  NewLockable := func()(*Node, *ListenerExt) {
    listener := NewListenerExt(100)
    l := NewNode(ctx, RandID(), TestLockableType, 10, nil,
                  listener,
                  NewACLExt(lock_policy, link_policy),
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
  err = LinkRequirement(ctx, l1.ID, l2.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l1.ID, l3.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l1.ID, l4.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l1.ID, l5.ID)
  fatalErr(t, err)

  err = LinkRequirement(ctx, l0.ID, l2.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l0.ID, l3.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l0.ID, l4.ID)
  fatalErr(t, err)
  err = LinkRequirement(ctx, l0.ID, l5.ID)
  fatalErr(t, err)

  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l1_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")

  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")
  (*GraphTester)(t).WaitForState(ctx, l0_listener, LinkSignalType, "linked_as_req", time.Millisecond*100, "No linked_as_req")

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
