package graphvent

import (
  "testing"
  "time"
  "net"
  "net/http"
  "io"
  "fmt"
  "encoding/json"
  "bytes"
  "crypto/rand"
  "crypto/ecdh"
  "crypto/ecdsa"
  "crypto/elliptic"
  "crypto/tls"
  "encoding/base64"
)

func TestGQLDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{"test", "signal", "thread"})
  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r
  ctx.Log.Logf("test", "L1_ID: %s", l1.ID().String())

  t1_r := NewSimpleThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r
  ctx.Log.Logf("test", "T1_ID: %s", t1.ID().String())
  listen_id := RandID()
  ctx.Log.Logf("test", "LISTENER_ID: %s", listen_id.String())
  update_channel := UpdateChannel(t1, 10, listen_id)

  u1_key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  u1_shared := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x23, 0x45, 0x67}

  u1_r := NewUser("Test User", time.Now(), &u1_key.PublicKey, u1_shared, []string{"gql"})
  u1 := &u1_r
  ctx.Log.Logf("test", "U1_ID: %s", u1.ID().String())

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)
  gql_r := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  gql := &gql_r
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID().String())

  // Policy to allow gql to perform all action on all resources
  p1_r := NewPerNodePolicy(RandID(), map[NodeID]NodeActions{
    gql.ID(): NewNodeActions(nil, []string{"*"}),
  })
  p1 := &p1_r
  p2_r := NewSimplePolicy(RandID(), NewNodeActions(NodeActions{
    "signal": []string{"read"},
  }, nil))
  p2 := &p2_r

  context := NewWriteContext(ctx)
  err = UpdateStates(context, gql, LockMap{
    p1.ID(): LockInfo{p1, nil},
    p2.ID(): LockInfo{p2, nil},
  }, func(context *StateContext) error {
    return nil
  })
  fatalErr(t, err)

  ctx.Log.Logf("test", "P1_ID: %s", p1.ID().String())
  ctx.Log.Logf("test", "P2_ID: %s", p2.ID().String())
  err = AttachPolicies(ctx, gql, p1, p2)
  fatalErr(t, err)
  err = AttachPolicies(ctx, l1, p1, p2)
  fatalErr(t, err)
  err = AttachPolicies(ctx, t1, p1, p2)
  fatalErr(t, err)
  err = AttachPolicies(ctx, u1, p1, p2)
  fatalErr(t, err)

  info := NewParentThreadInfo(true, "start", "restore")
  context = NewWriteContext(ctx)
  err = UpdateStates(context, gql, NewLockMap(
    NewLockInfo(gql, []string{"users"}),
  ), func(context *StateContext) error {
    gql.Users[KeyID(&u1_key.PublicKey)] = u1

    err := LinkThreads(context, gql, gql, t1, &info)
    if err != nil {
      return err
    }
    return LinkLockables(context, gql, gql, []Lockable{l1})
  })
  fatalErr(t, err)

  context = NewReadContext(ctx)
  err = UseStates(context, gql, NewLockInfo(gql, []string{"signal"}), func(context *StateContext) error {
    err := gql.Signal(context, NewStatusSignal("child_linked", t1.ID()))
    if err != nil {
      return nil
    }
    return gql.Signal(context, StopSignal)
  })
  fatalErr(t, err)

  err = ThreadLoop(ctx, gql, "start")
  fatalErr(t, err)

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "stopped", 100*time.Millisecond, "Didn't receive stopped on update_channel")

  context = NewReadContext(ctx)
  err = UseStates(context, gql, LockList([]Node{gql, u1}, nil), func(context *StateContext) error {
    ser1, err := gql.Serialize()
    ser2, err := u1.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser1)
    ctx.Log.Logf("test", "\n%s\n\n", ser2)
    return err
  })

  gql_loaded, err := LoadNode(ctx, gql.ID())
  fatalErr(t, err)
  var t1_loaded *SimpleThread = nil

  var update_channel_2 chan GraphSignal
  context = NewReadContext(ctx)
  err = UseStates(context, gql, NewLockInfo(gql_loaded, []string{"users", "children"}), func(context *StateContext) error {
    ser, err := gql_loaded.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    u_loaded := gql_loaded.(*GQLThread).Users[u1.ID()]
    child := gql_loaded.(Thread).Children()[0].(*SimpleThread)
    t1_loaded = child
    update_channel_2 = UpdateChannel(t1_loaded, 10, RandID())
    err = UseStates(context, gql, NewLockInfo(u_loaded, nil), func(context *StateContext) error {
      ser, err := u_loaded.Serialize()
      ctx.Log.Logf("test", "\n%s\n\n", ser)
      return err
    })
    gql_loaded.Signal(context, StopSignal)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded.(Thread), "start")
  fatalErr(t, err)
  (*GraphTester)(t).WaitForValue(ctx, update_channel_2, "stopped", 100*time.Millisecond, "Didn't receive stopped on update_channel_2")

}

func TestGQLAuth(t * testing.T) {
  ctx := logTestContext(t, []string{"test", "gql", "policy"})
  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  p1_r := NewPerTagPolicy(RandID(), map[string]NodeActions{"gql": NewNodeActions(nil, []string{"read"})})
  p1 := &p1_r

  gql_t_r := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  gql_t := &gql_t_r

  // p1 not written to DB, TODO: update write to follow links maybe
  context := NewWriteContext(ctx)
  err = UpdateStates(context, gql_t, NewLockInfo(gql_t, []string{"policies"}), func(context *StateContext) error {
    return gql_t.AddPolicy(p1)
  })

  done := make(chan error, 1)

  var update_channel chan GraphSignal
  context = NewReadContext(ctx)
  err = UseStates(context, gql_t, NewLockInfo(gql_t, nil), func(context *StateContext) error {
    update_channel = UpdateChannel(gql_t, 10, NodeID{})
    return nil
  })
  fatalErr(t, err)

  go func(done chan error, thread Thread) {
    timeout := time.After(2*time.Second)
    select {
    case <-timeout:
      ctx.Log.Logf("test", "TIMEOUT")
    case <-done:
      ctx.Log.Logf("test", "DONE")
    }
    context := NewReadContext(ctx)
    err := UseStates(context, gql_t, NewLockInfo(gql_t, []string{"signal}"}), func(context *StateContext) error {
      return thread.Signal(context, StopSignal)
    })
    fatalErr(t, err)
  }(done, gql_t)

  go func(thread Thread){
    (*GraphTester)(t).WaitForValue(ctx, update_channel, "server_started", 100*time.Millisecond, "Server didn't start")
    port := gql_t.tcp_listener.Addr().(*net.TCPAddr).Port
    ctx.Log.Logf("test", "GQL_PORT: %d", port)

    customTransport := &http.Transport{
      Proxy:                 http.DefaultTransport.(*http.Transport).Proxy,
      DialContext:           http.DefaultTransport.(*http.Transport).DialContext,
      MaxIdleConns:          http.DefaultTransport.(*http.Transport).MaxIdleConns,
      IdleConnTimeout:       http.DefaultTransport.(*http.Transport).IdleConnTimeout,
      ExpectContinueTimeout: http.DefaultTransport.(*http.Transport).ExpectContinueTimeout,
      TLSHandshakeTimeout:   http.DefaultTransport.(*http.Transport).TLSHandshakeTimeout,
      TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
    }
    client := &http.Client{Transport: customTransport}
    url := fmt.Sprintf("https://localhost:%d/auth", port)

    id, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
    fatalErr(t, err)

    auth_req, ec_key, err := NewAuthReqJSON(ecdh.P256(), id)
    fatalErr(t, err)

    str, err := json.Marshal(auth_req)
    fatalErr(t, err)

    b := bytes.NewBuffer(str)
    req, err := http.NewRequest("PUT", url, b)
    fatalErr(t, err)

    resp, err := client.Do(req)
    fatalErr(t, err)

    body, err := io.ReadAll(resp.Body)
    fatalErr(t, err)

    resp.Body.Close()

    var j AuthRespJSON
    err = json.Unmarshal(body, &j)
    fatalErr(t, err)

    shared, err := ParseAuthRespJSON(j, elliptic.P256(), ecdh.P256(), ec_key)
    fatalErr(t, err)

    url = fmt.Sprintf("https://localhost:%d/gql", port)
    ser, err := json.MarshalIndent(&GQLPayload{
      Query: "query { Self { Users { ID, Name } } }",
    }, "", "  ")
    fatalErr(t, err)

    b = bytes.NewBuffer(ser)
    req, err = http.NewRequest("GET", url, b)
    fatalErr(t, err)

    req.SetBasicAuth(KeyID(&id.PublicKey).String(), base64.StdEncoding.EncodeToString(shared))
    resp, err = client.Do(req)
    fatalErr(t, err)

    body, err = io.ReadAll(resp.Body)
    fatalErr(t, err)

    resp.Body.Close()

    ctx.Log.Logf("test", "TEST_RESP: %s", body)

    req.SetBasicAuth(KeyID(&id.PublicKey).String(), "BAD_PASSWORD")
    resp, err = client.Do(req)
    fatalErr(t, err)

    body, err = io.ReadAll(resp.Body)
    fatalErr(t, err)

    resp.Body.Close()

    ctx.Log.Logf("test", "TEST_RESP: %s", body)

    done <- nil
  }(gql_t)

  err = ThreadLoop(ctx, gql_t, "start")
  fatalErr(t, err)
}
