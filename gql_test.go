package graphvent

import (
  "testing"
  "time"
  "errors"
  "crypto/rand"
  "crypto/ecdh"
  "crypto/ecdsa"
  "crypto/elliptic"
)

func TestGQLDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{"test", "db", "policy", "signal"})

  TestUserNodeType := NodeType("TEST_USER")
  err := ctx.RegisterNodeType(TestUserNodeType, []ExtType{ACLExtType, ACLPolicyExtType})
  fatalErr(t, err)

  u1 := NewNode(RandID(), TestUserNodeType)
  ctx.Nodes[u1.ID] = &u1
  u1.Extensions[ACLExtType] = NewACLExt(nil)
  u1.Extensions[ACLPolicyExtType] = NewACLPolicyExt(map[PolicyType]Policy{
    PerNodePolicyType: NewPerNodePolicy(map[NodeID][]string{
      u1.ID: []string{"users.write", "children.write", "parent.write", "dependencies.write", "requirements.write"},
    }, nil),
  })

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  ListenerNodeType := NodeType("LISTENER")
  err =  ctx.RegisterNodeType(ListenerNodeType, []ExtType{ACLExtType, ListenerExtType, LockableExtType})
  fatalErr(t, err)

  l1 := NewNode(RandID(), ListenerNodeType)
  ctx.Nodes[l1.ID] = &l1
  l1.Extensions[ACLExtType] = NewACLExt(NodeList(&u1))
  listener_ext := NewListenerExt(10)
  l1.Extensions[ListenerExtType] = listener_ext
  l1.Extensions[LockableExtType] = NewLockableExt(nil, nil, nil, nil)

  ctx.Log.Logf("test", "L1_ID: %s", l1.ID)

  TestThreadNodeType := NodeType("TEST_THREAD")
  err = ctx.RegisterNodeType(TestThreadNodeType, []ExtType{ACLExtType, ThreadExtType, LockableExtType})
  fatalErr(t, err)

  t1 := NewNode(RandID(), TestThreadNodeType)
  ctx.Nodes[t1.ID] = &t1
  t1.Extensions[ACLExtType] = NewACLExt(NodeList(&u1))
  t1.Extensions[ACLPolicyExtType] = NewACLPolicyExt(map[PolicyType]Policy{
    ParentOfPolicyType: NewParentOfPolicy(map[NodeID][]string{
      t1.ID: []string{"signal.abort", "state.write"},
    }),
  })
  t1.Extensions[ThreadExtType], err = NewThreadExt(ctx, BaseThreadType, nil, nil, "init", nil)
  fatalErr(t, err)
  t1.Extensions[LockableExtType] = NewLockableExt(nil, nil, nil, nil)

  ctx.Log.Logf("test", "T1_ID: %s", t1.ID)

  TestGQLNodeType := NodeType("TEST_GQL")
  err = ctx.RegisterNodeType(TestGQLNodeType, []ExtType{ACLExtType, GroupExtType, GQLExtType, ThreadExtType, LockableExtType})
  fatalErr(t, err)

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  gql := NewNode(RandID(), TestGQLNodeType)
  ctx.Nodes[gql.ID] = &gql
  gql.Extensions[ACLExtType] = NewACLExt(NodeList(&u1))
  gql.Extensions[ACLPolicyExtType] = NewACLPolicyExt(map[PolicyType]Policy{
    ChildOfPolicyType: NewChildOfPolicy(map[NodeID][]string{
      gql.ID: []string{"signal.status"},
    }),
  })
  gql.Extensions[GroupExtType] = NewGroupExt(nil)
  gql.Extensions[GQLExtType] = NewGQLExt(":0", ecdh.P256(), key, nil, nil)
  gql.Extensions[ThreadExtType], err = NewThreadExt(ctx, GQLThreadType, nil, nil, "ini", nil)
  fatalErr(t, err)
  gql.Extensions[LockableExtType] = NewLockableExt(nil, nil, nil, nil)

  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)
  info := ParentInfo{true, "start", "restore"}
  context := NewWriteContext(ctx)
  err = UpdateStates(context, &u1, NewACLInfo(&gql, []string{"users"}), func(context *StateContext) error {
    err := LinkThreads(context, &u1, &gql, ChildInfo{&t1, map[InfoType]Info{
      ParentInfoType: &info,
    }})
    if err != nil {
      return err
    }
    return LinkLockables(context, &u1, &l1, []*Node{&gql})
  })
  fatalErr(t, err)

  context = NewReadContext(ctx)
  err = Signal(context, &gql, &gql, NewStatusSignal("child_linked", t1.ID))
  fatalErr(t, err)
  context = NewReadContext(ctx)
  err = Signal(context, &gql, &gql, AbortSignal)
  fatalErr(t, err)

  err = ThreadLoop(ctx, &gql, "start")
  if errors.Is(err, ThreadAbortedError) == false {
    fatalErr(t, err)
  }

  (*GraphTester)(t).WaitForStatus(ctx, listener_ext.Chan, "aborted", 100*time.Millisecond, "Didn't receive aborted on listener")

  context = NewReadContext(ctx)
  err = UseStates(context, &gql, ACLList([]*Node{&gql, &u1}, nil), func(context *StateContext) error {
    ser1, err := gql.Serialize()
    ser2, err := u1.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser1)
    ctx.Log.Logf("test", "\n%s\n\n", ser2)
    return err
  })

  // Clear all loaded nodes from the context so it loads them from the database
  ctx.Nodes = NodeMap{}
  gql_loaded, err := LoadNode(ctx, gql.ID)
  fatalErr(t, err)
  context = NewReadContext(ctx)
  err = UseStates(context, gql_loaded, NewACLInfo(gql_loaded, []string{"users", "children", "requirements"}), func(context *StateContext) error {
    ser, err := gql_loaded.Serialize()
    lockable_ext, err := GetExt[*LockableExt](gql_loaded)
    if err != nil {
      return err
    }
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    dependency := lockable_ext.Dependencies[l1.ID]
    listener_ext, err = GetExt[*ListenerExt](dependency)
    if err != nil {
      return err
    }
    Signal(context, gql_loaded, gql_loaded, StopSignal)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded, "start")
  fatalErr(t, err)
  (*GraphTester)(t).WaitForStatus(ctx, listener_ext.Chan, "stopped", 100*time.Millisecond, "Didn't receive stopped on update_channel_2")

}

