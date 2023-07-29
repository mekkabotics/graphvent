package graphvent

import (
  "testing"
  "time"
  "fmt"
  "encoding/json"
  "io"
  "net/http"
  "net"
  "crypto/tls"
  "bytes"
)

func TestGQL(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "gql", "policy"})

  TestNodeType := NodeType("TEST")
  err := ctx.RegisterNodeType(TestNodeType, []ExtType{LockableExtType, ACLExtType})
  fatalErr(t, err)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)
  policy := NewAllNodesPolicy(Actions{MakeAction("+")})
  gql := NewNode(ctx, nil, TestNodeType, 10, nil, NewLockableExt(), NewACLExt(policy), gql_ext)
  n1 := NewNode(ctx, nil, TestNodeType, 10, nil, NewLockableExt(), NewACLExt(policy), listener_ext)

  ctx.Send(n1.ID, gql.ID, StateSignal{NewDirectSignal(GQLStateSignalType), "start_server"})
  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, GQLStateSignalType, func(sig StateSignal) bool {
    return sig.State == "server_started"
  })
  fatalErr(t, err)

  skipVerifyTransport := &http.Transport{
    TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
  }
  client := &http.Client{Transport: skipVerifyTransport}
  port := gql_ext.tcp_listener.Addr().(*net.TCPAddr).Port
  url := fmt.Sprintf("https://localhost:%d/gql", port)

  ser, err := json.MarshalIndent(&GQLPayload{
    Query: "query { Self { ID } }",
  }, "", "  ")
  fatalErr(t, err)

  req_data := bytes.NewBuffer(ser)

  req, err := http.NewRequest("GET", url, req_data)
  req.SetBasicAuth(n1.ID.String(), "BAD_PASSWORD")
  fatalErr(t, err)

  resp, err := client.Do(req)
  fatalErr(t, err)

  body, err := io.ReadAll(resp.Body)
  fatalErr(t, err)

  resp.Body.Close()

  ctx.Log.Logf("test", "TEST_RESP: %s", body)

  ctx.Send(n1.ID, gql.ID, StateSignal{NewDirectSignal(GQLStateSignalType), "stop_server"})
  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, GQLStateSignalType, func(sig StateSignal) bool {
    return sig.State == "server_stopped"
  })
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

