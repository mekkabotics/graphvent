package graphvent

/*import (
  "testing"
  "time"
  "fmt"
  "encoding/json"
  "io"
  "net/http"
  "net"
  "crypto/tls"
  "crypto/rand"
  "crypto/ed25519"
  "bytes"
  "golang.org/x/net/websocket"
  "github.com/google/uuid"
)

func TestGQLAuth(t *testing.T) {
  ctx := logTestContext(t, []string{"test"})

  listener_1 := NewListenerExt(10)
  node_1, err := NewNode(ctx, nil, "Base", 10, nil, listener_1)
  fatalErr(t, err)
  
  listener_2 := NewListenerExt(10)
  node_2, err := NewNode(ctx, nil, "Base", 10, nil, listener_2)
  fatalErr(t, err)

  auth_header, err := AuthB64(node_1.Key, node_2.Key.Public().(ed25519.PublicKey))
  fatalErr(t, err)

  auth, err := ParseAuthB64(auth_header, node_2.Key)
  fatalErr(t, err)

  err = ValidateAuthorization(Authorization{
    AuthInfo: auth.AuthInfo,
    Key: auth.Key.Public().(ed25519.PublicKey),
  }, time.Second)
  fatalErr(t, err)

  ctx.Log.Logf("test", "AUTH: %+v", auth)
}

func TestGQLServer(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "gqlws", "gql"})

  pub, gql_key, err := ed25519.GenerateKey(rand.Reader)
  fatalErr(t, err)
  gql_id := KeyID(pub)

  group_policy_1 := NewAllNodesPolicy(Tree{
    SerializedType(SignalTypeFor[ReadSignal]()): Tree{
      SerializedType(ExtTypeFor[GroupExt]()): Tree{
        SerializedType(GetFieldTag("members")): Tree{},
      },
    },
    SerializedType(SignalTypeFor[ReadResultSignal]()): nil,
    SerializedType(SignalTypeFor[ErrorSignal]()): nil,
  })

  group_policy_2 := NewMemberOfPolicy(map[NodeID]map[string]Tree{
    gql_id: {
      "test_group": {
        SerializedType(SignalTypeFor[LinkSignal]()): nil,
        SerializedType(SignalTypeFor[LockSignal]()): nil,
        SerializedType(SignalTypeFor[StatusSignal]()): nil,
        SerializedType(SignalTypeFor[ReadSignal]()): nil,
      },
    },
  })

  user_policy_1 := NewAllNodesPolicy(Tree{
    SerializedType(SignalTypeFor[ReadResultSignal]()): nil,
    SerializedType(SignalTypeFor[ErrorSignal]()): nil,
  })

  user_policy_2 := NewMemberOfPolicy(map[NodeID]map[string]Tree{
    gql_id: {
      "test_group": {
        SerializedType(SignalTypeFor[LinkSignal]()): nil,
        SerializedType(SignalTypeFor[ReadSignal]()): nil,
        SerializedType(SignalTypeFor[LockSignal]()): nil,
      },
    },
  })

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)

  listener_ext := NewListenerExt(10)
  n1, err := NewNode(ctx, nil, "Base", 10, []Policy{user_policy_2, user_policy_1}, NewLockableExt(nil))
  fatalErr(t, err)

  gql, err := NewNode(ctx, gql_key, "Base", 10, []Policy{group_policy_2, group_policy_1},
  NewLockableExt([]NodeID{n1.ID}), gql_ext, NewGroupExt(map[string][]NodeID{"test_group": {n1.ID, gql_id}}), listener_ext)
  fatalErr(t, err)

  ctx.Log.Logf("test", "GQL:  %s", gql.ID)
  ctx.Log.Logf("test", "NODE: %s", n1.ID)

  _, err = WaitForSignal(listener_ext.Chan, 100*time.Millisecond, func(sig *StatusSignal) bool {
    return sig.Source == gql_id
  })
  fatalErr(t, err)

  skipVerifyTransport := &http.Transport{
    TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
  }
  client := &http.Client{Transport: skipVerifyTransport}
  port := gql_ext.tcp_listener.Addr().(*net.TCPAddr).Port
  url := fmt.Sprintf("http://localhost:%d/gql", port)
  ws_url := fmt.Sprintf("ws://127.0.0.1:%d/gqlws", port)

  req_1 := GQLPayload{
    Query: "query Node($id:String) { Node(id:$id) { ID, TypeHash } }",
    Variables: map[string]interface{}{
      "id": n1.ID.String(),
    },
  }

  req_2 := GQLPayload{
    Query: "query Node($id:String) { Node(id:$id) { ID, TypeHash, ... on GQLServer { SubGroups { Name, Members { ID } } , Listen, Requirements { ID, TypeHash Owner { ID } } } } }",
    Variables: map[string]interface{}{
      "id": gql.ID.String(),
    },
  }

  auth_header, err := AuthB64(n1.Key, gql.Key.Public().(ed25519.PublicKey))
  fatalErr(t, err)

  SendGQL := func(payload GQLPayload) []byte {
    ser, err := json.MarshalIndent(&payload, "", "  ")
    fatalErr(t, err)

    req_data := bytes.NewBuffer(ser)
    req, err := http.NewRequest("GET", url, req_data)
    fatalErr(t, err)

    req.Header.Add("Authorization", auth_header)
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

  sub_1 := GQLPayload{
    Query: "subscription { Self { ID, TypeHash, ... on Lockable { Requirements { ID }}}}",
  }

  SubGQL := func(payload GQLPayload) {
    config, err := websocket.NewConfig(ws_url, url)
    fatalErr(t, err)
    config.Protocol = append(config.Protocol, "graphql-ws")
    config.TlsConfig = &tls.Config{InsecureSkipVerify: true}

    ws, err := websocket.DialConfig(config)

    fatalErr(t, err)

    type payload_struct struct {
      Token string `json:"token"`
    }

    init := struct{
      ID uuid.UUID `json:"id"`
      Type string `json:"type"`
      Payload payload_struct `json:"payload"`
    }{
      uuid.New(),
      "connection_init",
      payload_struct{ auth_header },
    }

    ser, err := json.Marshal(&init)
    fatalErr(t, err)

    _, err = ws.Write(ser)
    fatalErr(t, err)

    resp := make([]byte, 1024)
    n, err := ws.Read(resp)

    var init_resp GQLWSMsg
    err = json.Unmarshal(resp[:n], &init_resp)
    fatalErr(t, err)

    if init_resp.Type != "connection_ack" {
      t.Fatal("Didn't receive connection_ack")
    }

    sub := GQLWSMsg{
      ID: uuid.New().String(),
      Type: "subscribe",
      Payload: sub_1,
    }

    ser, err = json.Marshal(&sub)
    fatalErr(t, err)
    _, err = ws.Write(ser)
    fatalErr(t, err)

    n, err = ws.Read(resp)
    fatalErr(t, err)
    ctx.Log.Logf("test", "SUB: %s", resp[:n])

    msgs := Messages{}
    test_changes := Changes{}
    AddChange[GQLExt](test_changes, "state")
    msgs = msgs.Add(ctx, gql.ID, gql, nil, NewStatusSignal(gql.ID, test_changes))
    err = ctx.Send(msgs)
    fatalErr(t, err)

    n, err = ws.Read(resp)
    fatalErr(t, err)
    ctx.Log.Logf("test", "SUB: %s", resp[:n])

    // TODO: check that there are no more messages sent to ws within a timeout
  }

  SubGQL(sub_1)

  msgs := Messages{}
  msgs = msgs.Add(ctx, gql.ID, gql, nil, NewStopSignal())
  err = ctx.Send(msgs)
  fatalErr(t, err)
  _, err = WaitForSignal(listener_ext.Chan, 100*time.Millisecond, func(sig *StoppedSignal) bool {
    return sig.Source == gql_id
  })
  fatalErr(t, err)
}

func TestGQLDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "db", "node"})

  u1, err := NewNode(ctx, nil, "Base", 10, nil)
  fatalErr(t, err)

  ctx.Log.Logf("test", "U1_ID: %s", u1.ID)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)
  gql, err := NewNode(ctx, nil, "Base", 10, nil,
                 gql_ext,
                 listener_ext,
                 NewGroupExt(nil))
  fatalErr(t, err)
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)

  msgs := Messages{}
  msgs = msgs.Add(ctx, gql.ID, gql, nil, NewStopSignal())
  err = ctx.Send(msgs)
  fatalErr(t, err)
  _, err = WaitForSignal(listener_ext.Chan, 100*time.Millisecond, func(sig *StoppedSignal) bool {
    return sig.Source == gql.ID
  })
  fatalErr(t, err)

  // Clear all loaded nodes from the context so it loads them from the database
  ctx.nodeMap = map[NodeID]*Node{}
  gql_loaded, err := LoadNode(ctx, gql.ID)
  fatalErr(t, err)

  listener_ext, err = GetExt[ListenerExt](gql_loaded)
  fatalErr(t, err)
  msgs = Messages{}
  msgs = msgs.Add(ctx, gql_loaded.ID, gql_loaded, nil, NewStopSignal())
  err = ctx.Send(msgs)
  fatalErr(t, err)
  _, err = WaitForSignal(listener_ext.Chan, 100*time.Millisecond, func(sig *StoppedSignal) bool {
    return sig.Source == gql_loaded.ID
  })
  fatalErr(t, err)
}
*/
