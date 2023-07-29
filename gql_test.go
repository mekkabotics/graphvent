package graphvent

import (
  "testing"
  "time"
)

func TestGQL(t *testing.T) {
  ctx := logTestContext(t, []string{})

  TestNodeType := NodeType("TEST")
  err := ctx.RegisterNodeType(TestNodeType, []ExtType{LockableExtType, ACLExtType})
  fatalErr(t, err)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)
  policy := NewAllNodesPolicy(Actions{MakeAction("+")})
  gql := NewNode(ctx, nil, TestNodeType, 10, nil, NewLockableExt(), NewACLExt(policy), gql_ext, listener_ext)
  n1 := NewNode(ctx, nil, TestNodeType, 10, nil, NewLockableExt(), NewACLExt(policy))

  LinkRequirement(ctx, gql.ID, n1.ID)
  _, err = WaitForSignal(ctx, listener_ext, time.Millisecond*10, LinkSignalType, func(sig StateSignal) bool {
    return sig.State == "linked_as_req"
  })
  fatalErr(t, err)
}

func TestGQLDB(t *testing.T) {
  ctx := logTestContext(t, []string{})

  TestUserNodeType := NodeType("TEST_USER")
  err := ctx.RegisterNodeType(TestUserNodeType, []ExtType{})
  fatalErr(t, err)
  u1 := NewNode(ctx, nil, TestUserNodeType, 10, nil)

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)
  gql := NewNode(ctx, nil, GQLNodeType, 10, nil,
                 gql_ext,
                 listener_ext,
                 NewACLExt(),
                 NewGroupExt(nil))
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)

  err = ctx.Send(gql.ID, gql.ID, StopSignal)
  fatalErr(t, err)

  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, StatusSignalType, func(sig IDStateSignal) bool {
    return sig.State == "stopped" && sig.ID == gql.ID
  })
  fatalErr(t, err)

  ser1, err := gql.Serialize()
  ser2, err := u1.Serialize()
  ctx.Log.Logf("test", "\n%s\n\n", ser1)
  ctx.Log.Logf("test", "\n%s\n\n", ser2)

  // Clear all loaded nodes from the context so it loads them from the database
  ctx.Nodes = NodeMap{}
  gql_loaded, err := LoadNode(ctx, gql.ID)
  fatalErr(t, err)
  listener_ext, err = GetExt[*ListenerExt](gql_loaded)
  fatalErr(t, err)
  err = ctx.Send(gql_loaded.ID, gql_loaded.ID, StopSignal)
  fatalErr(t, err)
  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, StatusSignalType, func(sig IDStateSignal) bool {
    return sig.State == "stopped" && sig.ID == gql_loaded.ID
  })
  fatalErr(t, err)

}

