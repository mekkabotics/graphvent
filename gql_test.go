package graphvent

import (
  "testing"
  "time"
  "net/http"
  "net"
  "errors"
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
  ctx := logTestContext(t, []string{"test"})
  l1 := NewListener(RandID(), "Test Listener 1")
  ctx.Log.Logf("test", "L1_ID: %s", l1.ID().String())

  t1 := NewThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  ctx.Log.Logf("test", "T1_ID: %s", t1.ID().String())
  listen_id := RandID()
  ctx.Log.Logf("test", "LISTENER_ID: %s", listen_id.String())

  u1_key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  u1 := NewUser("Test User", time.Now(), &u1_key.PublicKey, []byte{})
  ctx.Log.Logf("test", "U1_ID: %s", u1.ID().String())

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)
  gql := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  ctx.Log.Logf("test", "GQL_ID: %s", gql.ID().String())

  // Policy to allow gql to perform all action on all resources
  p1 := NewPerNodePolicy(RandID(), map[NodeID]NodeActions{
    gql.ID(): NewNodeActions(nil, []string{"*"}),
  })
  p2 := NewSimplePolicy(RandID(), NewNodeActions(NodeActions{
    "signal": []string{"status"},
  }, nil))

  context := NewWriteContext(ctx)
  err = UpdateStates(context, &gql, LockMap{
    p1.ID(): LockInfo{&p1, nil},
    p2.ID(): LockInfo{&p2, nil},
  }, func(context *StateContext) error {
    return nil
  })
  fatalErr(t, err)

  ctx.Log.Logf("test", "P1_ID: %s", p1.ID().String())
  ctx.Log.Logf("test", "P2_ID: %s", p2.ID().String())
  err = AttachPolicies(ctx, &gql, &p1, &p2)
  fatalErr(t, err)
  err = AttachPolicies(ctx, &l1, &p1, &p2)
  fatalErr(t, err)
  err = AttachPolicies(ctx, &t1, &p1, &p2)
  fatalErr(t, err)
  err = AttachPolicies(ctx, &u1, &p1, &p2)
  fatalErr(t, err)

  info := NewParentThreadInfo(true, "start", "restore")
  context = NewWriteContext(ctx)
  err = UpdateStates(context, &gql, NewLockMap(
    NewLockInfo(&gql, []string{"users"}),
  ), func(context *StateContext) error {
    gql.Users[u1.ID()] = &u1

    err := LinkThreads(context, &gql, &gql, ChildInfo{&t1, map[InfoType]interface{}{
      "parent": &info,
    }})
    if err != nil {
      return err
    }
    return LinkLockables(context, &gql, &l1, []LockableNode{&gql})
  })
  fatalErr(t, err)

  context = NewReadContext(ctx)
  err = Signal(context, &gql, &gql, NewStatusSignal("child_linked", t1.ID()))
  fatalErr(t, err)
  context = NewReadContext(ctx)
  err = Signal(context, &gql, &gql, AbortSignal)
  fatalErr(t, err)

  err = ThreadLoop(ctx, &gql, "start")
  if errors.Is(err, ThreadAbortedError) == false {
    fatalErr(t, err)
  }

  (*GraphTester)(t).WaitForStatus(ctx, l1.Chan, "aborted", 100*time.Millisecond, "Didn't receive aborted on listener")

  context = NewReadContext(ctx)
  err = UseStates(context, &gql, LockList([]Node{&gql, &u1}, nil), func(context *StateContext) error {
    ser1, err := gql.Serialize()
    ser2, err := u1.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser1)
    ctx.Log.Logf("test", "\n%s\n\n", ser2)
    return err
  })

  gql_loaded, err := LoadNode(ctx, gql.ID())
  fatalErr(t, err)
  var l1_loaded *Listener = nil
  context = NewReadContext(ctx)
  err = UseStates(context, gql_loaded, NewLockInfo(gql_loaded, []string{"users", "children", "requirements"}), func(context *StateContext) error {
    ser, err := gql_loaded.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    dependency := gql_loaded.(*GQLThread).Thread.Dependencies[l1.ID()].(*Listener)
    l1_loaded = dependency
    u_loaded := gql_loaded.(*GQLThread).Users[u1.ID()]
    err = UseStates(context, gql_loaded, NewLockInfo(u_loaded, nil), func(context *StateContext) error {
      ser, err := u_loaded.Serialize()
      ctx.Log.Logf("test", "\n%s\n\n", ser)
      return err
    })
    Signal(context, gql_loaded, gql_loaded, StopSignal)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded.(ThreadNode), "start")
  fatalErr(t, err)
  (*GraphTester)(t).WaitForStatus(ctx, l1_loaded.Chan, "stopped", 100*time.Millisecond, "Didn't receive stopped on update_channel_2")

}

func TestGQLAuth(t * testing.T) {
  ctx := logTestContext(t, []string{"test", "gql", "policy"})
  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  p2 := NewSimplePolicy(RandID(), NewNodeActions(NodeActions{
    "signal": []string{"status"},
  }, nil))

  l1 := NewListener(RandID(), "GQL Thread")
  err = AttachPolicies(ctx, &l1, &p2)
  fatalErr(t, err)

  p3 := NewPerNodePolicy(RandID(), map[NodeID]NodeActions{
    l1.ID(): NewNodeActions(nil, []string{"*"}),
  })

  gql := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  err = AttachPolicies(ctx, &gql, &p2, &p3)

  context := NewWriteContext(ctx)
  err = LinkLockables(context, &l1, &l1, []LockableNode{&gql})
  fatalErr(t, err)

  done := make(chan error, 1)

  go func(done chan error, thread ThreadNode) {
    timeout := time.After(2*time.Second)
    select {
    case <-timeout:
      ctx.Log.Logf("test", "TIMEOUT")
    case <-done:
      ctx.Log.Logf("test", "DONE")
    }
    context := NewReadContext(ctx)
    err := Signal(context, thread, thread, StopSignal)
    fatalErr(t, err)
  }(done, &gql)

  go func(thread ThreadNode){
    (*GraphTester)(t).WaitForStatus(ctx, l1.Chan, "server_started", 100*time.Millisecond, "Server didn't start")
    port := gql.tcp_listener.Addr().(*net.TCPAddr).Port
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
  }(&gql)

  err = ThreadLoop(ctx, &gql, "start")
  fatalErr(t, err)
}
