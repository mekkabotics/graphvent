package graphvent

import (
  "testing"
  "fmt"
  "encoding/json"
  "io"
  "net/http"
  "net"
  "crypto/tls"
  "bytes"
  "golang.org/x/net/websocket"
  "github.com/google/uuid"
)

func TestGQLServer(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "gqlws", "gql", "gql_subscribe"})

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)

  listener_ext := NewListenerExt(10)
  n1, err := NewNode(ctx, nil, "Lockable", 10, NewLockableExt(nil))
  fatalErr(t, err)

  gql, err := NewNode(ctx, nil, "Lockable", 10, NewLockableExt([]NodeID{n1.ID}), gql_ext, listener_ext)
  fatalErr(t, err)

  ctx.Log.Logf("test", "GQL:  %s", gql.ID)
  ctx.Log.Logf("test", "NODE: %s", n1.ID)

  skipVerifyTransport := &http.Transport{
    TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
  }
  client := &http.Client{Transport: skipVerifyTransport}
  port := gql_ext.tcp_listener.Addr().(*net.TCPAddr).Port
  url := fmt.Sprintf("http://localhost:%d/gql", port)
  ws_url := fmt.Sprintf("ws://127.0.0.1:%d/gqlws", port)

  req_1 := GQLPayload{
    Query: "query Node($id:graphvent_NodeID) { Node(id:$id) { ID, Type } }",
    Variables: map[string]interface{}{
      "id": n1.ID.String(),
    },
  }

  req_2 := GQLPayload{
    Query: "query Self { Self { ID, Type } }",
  }

  SendGQL := func(payload GQLPayload) []byte {
    ser, err := json.MarshalIndent(&payload, "", "  ")
    fatalErr(t, err)

    req_data := bytes.NewBuffer(ser)
    req, err := http.NewRequest("GET", url, req_data)
    fatalErr(t, err)

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
    Query: "subscription Self { Self { ID, Type } }",
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
    }{
      uuid.New(),
      "connection_init",
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

    err = ctx.Send(gql, []SendMsg{{
      Dest: gql.ID,
      Signal: NewStatusSignal(gql.ID, map[ExtType]Changes{
        ExtTypeFor[GQLExt](): {"state"},
      }),
    }})
    fatalErr(t, err)

    n, err = ws.Read(resp)
    fatalErr(t, err)
    ctx.Log.Logf("test", "SUB: %s", resp[:n])

    // TODO: check that there are no more messages sent to ws within a timeout
  }

  SubGQL(sub_1)
}

func TestGQLDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "db", "node"})

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)

  gql, err := NewNode(ctx, nil, "Base", 10, gql_ext, listener_ext)
  fatalErr(t, err)
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)
  
  err = ctx.Unload(gql.ID)
  fatalErr(t, err)

  gql_loaded, err := ctx.Load(gql.ID)
  fatalErr(t, err)

  listener_ext, err = GetExt[ListenerExt](gql_loaded)
  fatalErr(t, err)
}
