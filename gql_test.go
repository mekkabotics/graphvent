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

func TestGQL(t *testing.T) {


}

func TestGQLDB(t * testing.T) {
  ctx := logTestContext(t, []string{"test", "signal", "policy"})

  TestUserNodeType := NodeType("TEST_USER")
  err := ctx.RegisterNodeType(TestUserNodeType, []ExtType{ACLExtType})
  fatalErr(t, err)

  u1 := NewNode(ctx, RandID(), TestUserNodeType)
  u1_policy := NewPerNodePolicy(NodeActions{
    u1.ID: Actions{"users.write", "children.write", "parent.write", "dependencies.write", "requirements.write"},
  })
  u1.Extensions[ACLExtType] = NewACLExt(map[PolicyType]Policy{
    PerNodePolicyType: &u1_policy,
  })

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  l1, listener_ext := NewSimpleListener(ctx, 10)
  ctx.Log.Logf("test", "L1_ID: %s", l1.ID)

  TestThreadNodeType := NodeType("TEST_THREAD")
  err = ctx.RegisterNodeType(TestThreadNodeType, []ExtType{ACLExtType, ThreadExtType, LockableExtType})
  fatalErr(t, err)

  t1 := NewNode(ctx, RandID(), TestThreadNodeType)
  t1_policy_1 := NewParentOfPolicy(Actions{"signal.abort", "state.write"})
  t1_policy_2 := NewPerNodePolicy(NodeActions{
    u1.ID: Actions{"parent.write"},
  })
  t1.Extensions[ACLExtType] = NewACLExt(map[PolicyType]Policy{
    ParentOfPolicyType: &t1_policy_1,
    PerNodePolicyType: &t1_policy_2,
  })
  t1.Extensions[ThreadExtType], err = NewThreadExt(ctx, BaseThreadType, nil, nil, "init", nil)
  fatalErr(t, err)
  t1.Extensions[LockableExtType] = NewLockableExt(nil, nil, nil, nil)

  ctx.Log.Logf("test", "T1_ID: %s", t1.ID)

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  gql, err := NewGQLNode(ctx, NewGQLExt(":0", ecdh.P256(), key, nil, nil))
  fatalErr(t, err)
  gql_policy_1 := NewChildOfPolicy(Actions{"signal.status"})
  gql_policy_2 := NewPerNodePolicy(NodeActions{
    u1.ID: Actions{"parent.write", "children.write", "dependencies.write"},
  })
  gql.Extensions[ACLExtType] = NewACLExt(map[PolicyType]Policy{
    ChildOfPolicyType: &gql_policy_1,
    PerNodePolicyType: &gql_policy_2,
  })

  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)

  info := ParentInfo{true, "start", "restore"}
  context := NewWriteContext(ctx)
  err = UpdateStates(context, u1, ACLMap{}, func(context *StateContext) error {
    err := LinkThreads(context, u1, gql, ChildInfo{t1, map[InfoType]Info{
      ParentInfoType: &info,
    }})
    if err != nil {
      return err
    }
    return LinkLockables(context, u1, l1, []*Node{gql})
  })
  fatalErr(t, err)

  context = NewReadContext(ctx)
  err = gql.Process(context, gql.ID, NewStatusSignal("child_linked", t1.ID))
  fatalErr(t, err)
  context = NewReadContext(ctx)
  err = gql.Process(context, gql.ID, AbortSignal)
  fatalErr(t, err)

  err = ThreadLoop(ctx, gql, "start")
  if errors.Is(err, ThreadAbortedError) == false {
    fatalErr(t, err)
  }

  (*GraphTester)(t).WaitForStatus(ctx, listener_ext.Chan, "aborted", 100*time.Millisecond, "Didn't receive aborted on listener")

  context = NewReadContext(ctx)
  err = UseStates(context, gql, ACLList([]*Node{gql, u1}, nil), func(context *StateContext) error {
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
    gql_loaded.Process(context, gql_loaded.ID, StopSignal)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded, "start")
  fatalErr(t, err)
  (*GraphTester)(t).WaitForStatus(ctx, listener_ext.Chan, "stopped", 100*time.Millisecond, "Didn't receive stopped on update_channel_2")

}

