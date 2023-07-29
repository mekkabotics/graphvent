package graphvent

import (
  "testing"
  "time"
  "crypto/rand"
  "crypto/ecdh"
  "crypto/ecdsa"
  "crypto/elliptic"
)

func TestGQLDB(t * testing.T) {
  ctx := logTestContext(t, []string{})

  TestUserNodeType := NodeType("TEST_USER")
  err := ctx.RegisterNodeType(TestUserNodeType, []ExtType{})
  fatalErr(t, err)
  u1 := NewNode(ctx, nil, TestUserNodeType, 10, nil)

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  gql_ext := NewGQLExt(":0", ecdh.P256(), key, nil, nil)
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

