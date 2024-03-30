package graphvent

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/net/websocket"
)

func TestGQLSubscribe(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "gql"})

  n1, err := NewNode(ctx, nil, "LockableNode", 10, NewLockableExt(nil))
  fatalErr(t, err)

  listener_ext := NewListenerExt(10)

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)

  gql, err := NewNode(ctx, nil, "LockableNode", 10, NewLockableExt([]NodeID{n1.ID}), gql_ext, listener_ext)
  fatalErr(t, err)

  query := "subscription { Self { ID, Type ... on Lockable { LockableState } } }"

  ctx.Log.Logf("test", "GQL:  %s", gql.ID)
  ctx.Log.Logf("test", "Node: %s", n1.ID)
  ctx.Log.Logf("test", "Query: %s", query)

  sub_1 := GQLPayload{
    Query: query,
  }

  port := gql_ext.tcp_listener.Addr().(*net.TCPAddr).Port
  url := fmt.Sprintf("http://localhost:%d/gql", port)
  ws_url := fmt.Sprintf("ws://127.0.0.1:%d/gqlws", port)

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
    ctx.Log.Logf("test", "SUB1: %s", resp[:n])

    lock_id, err := LockLockable(ctx, gql)
    fatalErr(t, err)

    response, _, err := WaitForResponse(listener_ext.Chan, 100*time.Millisecond, lock_id)
    fatalErr(t, err)

    switch response.(type) {
    case *SuccessSignal:
      ctx.Log.Logf("test", "Locked %s", gql.ID)
    default:
      t.Errorf("Unexpected lock response: %s", response)
    }

    n, err = ws.Read(resp)
    fatalErr(t, err)
    ctx.Log.Logf("test", "SUB2: %s", resp[:n])

    n, err = ws.Read(resp)
    fatalErr(t, err)
    ctx.Log.Logf("test", "SUB3: %s", resp[:n])

    // TODO: check that there are no more messages sent to ws within a timeout
  }

  SubGQL(sub_1)
}

func TestGQLQuery(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "lockable"})

  n1_listener := NewListenerExt(10)
  n1, err := NewNode(ctx, nil, "LockableNode", 10, NewLockableExt(nil), n1_listener)
  fatalErr(t, err)

  gql_listener := NewListenerExt(10)
  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)

  gql, err := NewNode(ctx, nil, "LockableNode", 10, NewLockableExt([]NodeID{n1.ID}), gql_ext, gql_listener)
  fatalErr(t, err)

  ctx.Log.Logf("test", "GQL:  %s", gql.ID)
  ctx.Log.Logf("test", "NODE: %s", n1.ID)

  skipVerifyTransport := &http.Transport{
    TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
  }
  client := &http.Client{Transport: skipVerifyTransport}
  port := gql_ext.tcp_listener.Addr().(*net.TCPAddr).Port
  url := fmt.Sprintf("http://localhost:%d/gql", port)

  req_1 := GQLPayload{
    Query: "query Node($id:graphvent_NodeID) { Node(id:$id) { ID, Type, ... on Lockable { LockableState } } }",
    Variables: map[string]interface{}{
      "id": n1.ID.String(),
    },
  }

  req_2 := GQLPayload{
    Query: "query Self { Self { ID, Type, ... on Lockable { LockableState, Requirements { Key { ID ... on Lockable { LockableState } } } } } }",
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

  lock_id, err := LockLockable(ctx, n1)
  fatalErr(t, err)

  response, _, err := WaitForResponse(n1_listener.Chan, 100*time.Millisecond, lock_id)
  fatalErr(t, err)
  switch response := response.(type) {
  case *SuccessSignal:
  default:
    t.Fatalf("Wrong response: %s", reflect.TypeOf(response))
  }

  resp_3 := SendGQL(req_1)
  ctx.Log.Logf("test", "RESP_3: %s", resp_3)

  resp_4 := SendGQL(req_2)
  ctx.Log.Logf("test", "RESP_4: %s", resp_4)
}

func TestGQLDB(t *testing.T) {
  ctx := logTestContext(t, []string{"test", "db", "node", "serialize"})

  gql_ext, err := NewGQLExt(ctx, ":0", nil, nil)
  fatalErr(t, err)
  listener_ext := NewListenerExt(10)

  gql, err := NewNode(ctx, nil, "Node", 10, gql_ext, listener_ext)
  fatalErr(t, err)
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID)
  
  err = ctx.Unload(gql.ID)
  fatalErr(t, err)

  gql_loaded, err := ctx.Load(gql.ID)
  fatalErr(t, err)

  listener_ext, err = GetExt[ListenerExt](gql_loaded)
  fatalErr(t, err)
}
