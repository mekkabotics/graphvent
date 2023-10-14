package graphvent

import (
  "testing"
  "time"
  "crypto/ed25519"
  "crypto/rand"
)

var TestLockableType = NewNodeType("TEST_LOCKABLE")
func lockableTestContext(t *testing.T, logs []string) *Context {
  ctx := logTestContext(t, logs)

  err := ctx.RegisterNodeType(TestLockableType, []ExtType{LockableExtType})
  fatalErr(t, err)

  return ctx
}

func TestLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{"lockable", "listener"})

  l1_pub, l1_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)
  l1_id := KeyID(l1_pub)
  policy := NewPerNodePolicy(map[NodeID]Tree{
    l1_id: nil,
  })

  l2_listener := NewListenerExt(10)
  l2, err := NewNode(ctx, nil, TestLockableType, 10, []Policy{policy},
                  l2_listener,
                  NewLockableExt(nil),
                )
  fatalErr(t, err)

  l1_lockable := NewLockableExt(nil)
  l1_listener := NewListenerExt(10)
  l1, err := NewNode(ctx, l1_key, TestLockableType, 10, nil,
                 l1_listener,
                 l1_lockable,
               )
  fatalErr(t, err)

  msgs := Messages{}
  link_signal := NewLinkSignal("add", l2.ID)
  msgs = msgs.Add(ctx, l1.ID, l1, nil, link_signal)
  err = ctx.Send(msgs)
  fatalErr(t, err)

  _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, link_signal.ID())
  fatalErr(t, err)

  info, exists := l1_lockable.Requirements[l2.ID]
  if exists == false {
    t.Fatal("l2 not in l1 requirements")
  } else if info.State != Unlocked {
    t.Fatalf("l2 in bad requirement state in l1: %+v", info.State)
  }

  msgs = Messages{}
  unlink_signal := NewLinkSignal("remove", l2.ID)
  msgs = msgs.Add(ctx, l1.ID, l1, nil, unlink_signal)
  err = ctx.Send(msgs)
  fatalErr(t, err)

  _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, unlink_signal.ID())
  fatalErr(t, err)
}

func Test1KLink(t *testing.T) {
  ctx := lockableTestContext(t, []string{"test"})

  l_pub, listener_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)
  listener_id := KeyID(l_pub)
  child_policy := NewPerNodePolicy(map[NodeID]Tree{
    listener_id: Tree{
      SerializedType(LockSignalType): nil,
    },
  })
  NewLockable := func()(*Node) {
    l, err := NewNode(ctx, nil, TestLockableType, 10, []Policy{child_policy},
                  NewLockableExt(nil),
                )
    fatalErr(t, err)
    return l
  }

  reqs := make([]NodeID, 1000)
  for i, _ := range(reqs) {
    new_lockable := NewLockable()
    reqs[i] = new_lockable.ID
  }
  ctx.Log.Logf("test", "CREATED_10K")

  l_policy := NewAllNodesPolicy(Tree{
    SerializedType(LockSignalType): nil,
  })

  listener := NewListenerExt(100000)
  node, err := NewNode(ctx, listener_key, TestLockableType, 10000, []Policy{l_policy},
                listener,
                NewLockableExt(reqs),
              )
  fatalErr(t, err)
  ctx.Log.Logf("test", "CREATED_LISTENER")

  lock_id, err := LockLockable(ctx, node)
  fatalErr(t, err)

  _, err = WaitForResponse(listener.Chan, time.Second*20, lock_id)
  fatalErr(t, err)

  ctx.Log.Logf("test", "LOCKED_10K")
}

func TestLock(t *testing.T) {
  ctx := lockableTestContext(t, []string{"test", "lockable"})

  policy := NewAllNodesPolicy(nil)

  NewLockable := func(reqs []NodeID)(*Node, *ListenerExt) {
    listener := NewListenerExt(100)
    l, err := NewNode(ctx, nil, TestLockableType, 10, []Policy{policy},
                  listener,
                  NewLockableExt(reqs),
                )
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

  id_1, err := LockLockable(ctx, l0)
  ctx.Log.Logf("test", "ID_1: %s", id_1)
  fatalErr(t, err)
  _, err = WaitForResponse(l0_listener.Chan, time.Millisecond*10, id_1)
  fatalErr(t, err)

  id_2, err := LockLockable(ctx, l1)
  fatalErr(t, err)
  _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, id_2)
  fatalErr(t, err)

  id_3, err := UnlockLockable(ctx, l0)
  fatalErr(t, err)
  _, err = WaitForResponse(l0_listener.Chan, time.Millisecond*10, id_3)
  fatalErr(t, err)

  id_4, err := LockLockable(ctx, l1)
  fatalErr(t, err)

  _, err = WaitForResponse(l1_listener.Chan, time.Millisecond*10, id_4)
  fatalErr(t, err)
}
