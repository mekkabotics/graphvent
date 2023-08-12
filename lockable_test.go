package graphvent

import (
  "testing"
  "time"
  "crypto/ed25519"
  "crypto/rand"
)

const TestLockableType = NodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T, logs []string) *Context {
  ctx := logTestContext(t, logs)

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{LockableExtType})
  fatalErr(t, err)

  return ctx
}

func TestLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{"listener"})

  l1_pub, l1_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)
  l1_id := KeyID(l1_pub)
  policy := NewPerNodePolicy(map[NodeID]Tree{
    l1_id: nil,
  })

  l2_listener := NewListenerExt(10)
  l2 := NewNode(ctx, nil, TestLockableType, 10,
                map[PolicyType]Policy{
                  PerNodePolicyType: &policy,
                },
                  l2_listener,
                  NewLockableExt(nil),
                )

  l1_listener := NewListenerExt(10)
  l1 := NewNode(ctx, l1_key, TestLockableType, 10, nil,
                 l1_listener,
                 NewLockableExt([]NodeID{l2.ID}),
               )

  msgs := Messages{}
  s := NewBaseSignal("TEST", Down)
  msgs = msgs.Add(l1.ID, l1.Key, &s, l1.ID)
  err = ctx.Send(msgs)
  fatalErr(t, err)

  _, err = WaitForSignal(l1_listener.Chan, time.Millisecond*10, "TEST", func(sig *BaseSignal) bool {
    return sig.ID() == s.ID()
  })
  fatalErr(t, err)

  _, err = WaitForSignal(l2_listener.Chan, time.Millisecond*10, "TEST", func(sig *BaseSignal) bool {
    return sig.ID() == s.ID()
  })
  fatalErr(t, err)
}

func TestLink10K(t *testing.T) {
  ctx := lockableTestContext(t, []string{"test"})

  l_pub, listener_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)
  listener_id := KeyID(l_pub)
  child_policy := NewPerNodePolicy(map[NodeID]Tree{
    listener_id: Tree{
      LockSignalType.String(): nil,
    },
  })
  NewLockable := func()(*Node) {
    l := NewNode(ctx, nil, TestLockableType, 10,
                  map[PolicyType]Policy{
                    PerNodePolicyType: &child_policy,
                  },
                  NewLockableExt(nil),
                )
    return l
  }

  reqs := make([]NodeID, 1000)
  for i, _ := range(reqs) {
    new_lockable := NewLockable()
    reqs[i] = new_lockable.ID
  }
  ctx.Log.Logf("test", "CREATED_10K")

  l_policy := NewAllNodesPolicy(Tree{
    LockSignalType.String(): nil,
  })
  listener := NewListenerExt(100000)
  node := NewNode(ctx, listener_key, TestLockableType, 10000,
                map[PolicyType]Policy{
                  AllNodesPolicyType: &l_policy,
                },
                listener,
                NewLockableExt(reqs),
              )
  ctx.Log.Logf("test", "CREATED_LISTENER")

  err = LockLockable(ctx, node)
  fatalErr(t, err)

  _, err = WaitForSignal(listener.Chan, time.Millisecond*1000, LockSignalType, func(sig *StringSignal) bool {
    return sig.Str == "locked"
  })
  fatalErr(t, err)

  for _, _ = range(reqs) {
    _, err := WaitForSignal(listener.Chan, time.Millisecond*100, LockSignalType, func(sig *StringSignal) bool {
      return sig.Str == "locked"
    })
    fatalErr(t, err)
  }
  ctx.Log.Logf("test", "LOCKED_10K")
}

func TestLock(t *testing.T) {
  ctx := lockableTestContext(t, []string{"lockable", "policy"})

  policy := NewAllNodesPolicy(nil)

  NewLockable := func(reqs []NodeID)(*Node, *ListenerExt) {
    listener := NewListenerExt(100)
    l := NewNode(ctx, nil, TestLockableType, 10,
                  map[PolicyType]Policy{
                    AllNodesPolicyType: &policy,
                  },
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
  _, err = WaitForSignal(l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)
  _, err = WaitForSignal(l1_listener.Chan, time.Millisecond*10, LockSignalType, locked)
  fatalErr(t, err)

  err = UnlockLockable(ctx, l1)
  fatalErr(t, err)
}
