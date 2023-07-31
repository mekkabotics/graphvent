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
  "github.com/google/uuid"
)

func TestGQL(t *testing.T) {
  ctx := logTestContext(t, []string{"node", "test", "gql", "policy"})

  TestNodeType := NodeType("TEST")
  err := ctx.RegisterNodeType(TestNodeType, []ExtType{LockableExtType, ACLExtType})
  fatalErr(t, err)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil, "stopped")
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)
  policy := NewAllNodesPolicy(Actions{MakeAction("+")})
  gql := NewNode(ctx, nil, GQLNodeType, 10, []QueuedSignal{
    QueuedSignal{uuid.New(), StringSignal{NewDirectSignal(GQLStateSignalType), "start_server"}, time.Now()},
  }, NewLockableExt(), NewACLExt(policy), gql_ext, NewGroupExt(nil), listener_ext)
  n1 := NewNode(ctx, nil, TestNodeType, 10, nil, NewLockableExt(), NewACLExt(policy))

  err = LinkRequirement(ctx, gql.ID, n1.ID)
  fatalErr(t, err)

  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, GQLStateSignalType, func(sig StringSignal) bool {
    return sig.Str == "server_started"
  })
  fatalErr(t, err)

  skipVerifyTransport := &http.Transport{
    TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
  }
  client := &http.Client{Transport: skipVerifyTransport}
  port := gql_ext.tcp_listener.Addr().(*net.TCPAddr).Port
  url := fmt.Sprintf("https://localhost:%d/gql", port)

  req_1 := GQLPayload{
    Query: "query Node($id:String) { Node(id:$id) { ID, TypeHash } }",
    Variables: map[string]interface{}{
      "id": n1.ID.String(),
    },
  }

  req_2 := GQLPayload{
    Query: "query Node($id:String) { Node(id:$id) { ID, TypeHash, ... on GQLServer { Members { ID } , Listen, Requirements { ID, TypeHash, Dependencies { ID } } } } }",
    Variables: map[string]interface{}{
      "id": gql.ID.String(),
    },
  }

  SendGQL := func(payload GQLPayload) []byte {
    ser, err := json.MarshalIndent(&payload, "", "  ")
    fatalErr(t, err)

    req_data := bytes.NewBuffer(ser)
    req, err := http.NewRequest("GET", url, req_data)
    fatalErr(t, err)

    req.SetBasicAuth(n1.ID.String(), "BAD_PASSWORD")
    resp, err := client.Do(req)
    fatalErr(t, err)

    body, err := io.ReadAll(resp.Body)
    fatalErr(t, err)

    resp.Body.Close()
    return body
  }

  resp_1 := SendGQL(req_1)
  ctx.Log.Logf("test", "RESP_1: %s", resp_1)
  resp_2 := SendGQL(req_2)
  ctx.Log.Logf("test", "RESP_2: %s", resp_2)

  ctx.Send(n1.ID, gql.ID, StringSignal{NewDirectSignal(GQLStateSignalType), "stop_server"})
  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, GQLStateSignalType, func(sig StringSignal) bool {
    return sig.Str == "server_stopped"
  })
}

func TestGQLDB(t *testing.T) {
  ctx := logTestContext(t, []string{})

  TestUserNodeType := NodeType("TEST_USER")
  err := ctx.RegisterNodeType(TestUserNodeType, []ExtType{})
  fatalErr(t, err)
  u1 := NewNode(ctx, nil, TestUserNodeType, 10, nil)

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil, "start")
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

  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, StatusSignalType, func(sig IDStringSignal) bool {
    return sig.Str == "stopped" && sig.ID == gql.ID
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
  _, err = WaitForSignal(ctx, listener_ext, 100*time.Millisecond, StatusSignalType, func(sig IDStringSignal) bool {
    return sig.Str == "stopped" && sig.ID == gql_loaded.ID
  })
  fatalErr(t, err)

}

