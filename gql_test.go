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
  ctx := logTestContext(t, []string{"test", "signal", "policy", "db"})

  TestUserNodeType := NodeType("TEST_USER")
  err := ctx.RegisterNodeType(TestUserNodeType, []ExtType{})
  fatalErr(t, err)
  u1 := NewNode(ctx, RandID(), TestUserNodeType)

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  TestThreadNodeType := NodeType("TEST_THREAD")
  err = ctx.RegisterNodeType(TestThreadNodeType, []ExtType{ACLExtType, ThreadExtType})
  fatalErr(t, err)

  t1_policy_1 := NewParentOfPolicy(Actions{"signal.abort", "state.write"})
  t1_policy_2 := NewPerNodePolicy(NodeActions{
    u1.ID: Actions{"parent.write"},
  })
  t1_thread, err := NewThreadExt(ctx, BaseThreadType, nil,nil, "init", nil)
  fatalErr(t, err)
  t1 := NewNode(ctx,
                RandID(),
                TestThreadNodeType,
                NewACLExt(&t1_policy_1, &t1_policy_2),
                t1_thread)
  ctx.Log.Logf("test", "T1_ID: %s", t1.ID)

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  gql_p1 := NewChildOfPolicy(Actions{"signal.status"})
  gql_p2 := NewPerNodePolicy(NodeActions{
    u1.ID: Actions{"parent.write", "children.write", "dependencies.write"},
  })

  gql_thread, err := NewThreadExt(ctx, GQLThreadType, nil, nil, "init", nil)
  fatalErr(t, err)

  gql_ext := NewGQLExt(":0", ecdh.P256(), key, nil, nil)
  listener_ext := NewListenerExt(10)
  gql := NewNode(ctx, RandID(), GQLNodeType,
                 gql_thread,
                 gql_ext,
                 listener_ext,
                 NewACLExt(&gql_p1, &gql_p2),
                 NewGroupExt(nil))
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)

  info := ParentInfo{true, "start", "restore"}
  context := NewWriteContext(ctx)
  err = UpdateStates(context, u1, ACLMap{}, func(context *StateContext) error {
    return LinkThreads(context, u1, gql, ChildInfo{t1, map[InfoType]Info{
      ParentInfoType: &info,
    }})
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
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    listener_ext, err = GetExt[*ListenerExt](gql_loaded)
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

