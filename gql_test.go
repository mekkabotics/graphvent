package graphvent

import (
  "testing"
  "time"
  "errors"
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

func TestGQLThread(t * testing.T) {
  ctx := logTestContext(t, []string{})
  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  gql_t_r := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  gql_t := &gql_t_r

  t1_r := NewSimpleThread(RandID(), "Test thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r
  t2_r := NewSimpleThread(RandID(), "Test thread 2", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t2 := &t2_r

  err = UpdateStates(ctx, gql_t, RequestList([]Node{t1, t2}, []string{"parent"}), func(locked NodeLockMap) error {
    return UpdateMoreStates(ctx, locked, gql_t, NewLockRequest(gql_t, []string{"children"}), func(locked NodeLockMap) error {
      i1 := NewParentThreadInfo(true, "start", "restore")
      err := LinkThreads(ctx, gql_t, t1, &i1, locked)
      if err != nil {
        return err
      }

      i2 := NewParentThreadInfo(false, "start", "restore")
      return LinkThreads(ctx, gql_t, t2, &i2, locked)
    })
  })
  fatalErr(t, err)

  go func(thread Thread){
    time.Sleep(10*time.Millisecond)
    err := UseStates(ctx, thread, NewLockRequest(thread, []string{"signal"}), func(locked NodeLockMap) error {
      return thread.Signal(ctx, CancelSignal, locked)
    })
    fatalErr(t, err)
  }(gql_t)

  err = ThreadLoop(ctx, gql_t, "start")
  fatalErr(t, err)
}

func TestGQLDBLoad(t * testing.T) {
  ctx := logTestContext(t, []string{})
  l1_r := NewSimpleLockable(RandID(), "Test Lockable 1")
  l1 := &l1_r

  t1_r := NewSimpleThread(RandID(), "Test Thread 1", "init", nil, BaseThreadActions, BaseThreadHandlers)
  t1 := &t1_r
  update_channel := UpdateChannel(t1, 10, NodeID{})

  u1_key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  u1_shared := []byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x23, 0x45, 0x67}

  u1_r := NewUser("Test User", time.Now(), &u1_key.PublicKey, u1_shared, []string{"gql"})
  u1 := &u1_r

  p1_r := NewSimplePolicy(RandID(), NewNodeActions(nil, []string{"enumerate"}))
  p1 := &p1_r

  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)
  gql_r := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  gql := &gql_r

  info := NewParentThreadInfo(true, "start", "restore")
  err = UpdateStates(ctx, gql, NewLockRequest(gql, []string{"policies", "users", "requirements", "children"}), func(locked NodeLockMap) error {
    return UpdateMoreStates(ctx, locked, gql, RequestList([]Node{u1, p1}, []string{}), func(locked NodeLockMap) error {
      err := gql.AddPolicy(p1)
      if err != nil {
        return err
      }

      gql.Users[KeyID(&u1_key.PublicKey)] = u1

      return UpdateMoreStates(ctx, locked, gql, NewLockRequest(t1, []string{"parent"}), func(locked NodeLockMap) error {
        err := LinkThreads(ctx, gql, t1, &info, locked)
        if err != nil {
          return err
        }
        return UpdateMoreStates(ctx, locked, gql, NewLockRequest(l1, []string{"dependencies"}), func(locked NodeLockMap) error {
          return LinkLockables(ctx, gql, gql, []Lockable{l1}, locked)
        })
      })
    })
  })
  fatalErr(t, err)

  err = UseStates(ctx, gql, NewLockRequest(gql, []string{"signal"}), func(locked NodeLockMap) error {
    err := gql.Signal(ctx, NewStatusSignal("child_linked", t1.ID()), locked)
    if err != nil {
      return nil
    }
    return gql.Signal(ctx, CancelSignal, locked)
  })
  fatalErr(t, err)

  err = ThreadLoop(ctx, gql, "start")
  if errors.Is(err, ThreadAbortedError) {
    ctx.Log.Logf("test", "Main thread aborted by signal: %s", err)
  } else if err != nil{
    fatalErr(t, err)
  } else {
    ctx.Log.Logf("test", "Main thread cancelled by signal")
  }

  (*GraphTester)(t).WaitForValue(ctx, update_channel, "thread_aborted", 100*time.Millisecond, "Didn't receive thread_abort from t1 on t1")

  err = UseStates(ctx, gql, RequestList([]Node{gql, u1}, nil), func(locked NodeLockMap) error {
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
  err = UseStates(ctx, gql, NewLockRequest(gql_loaded, []string{"users", "children"}), func(locked NodeLockMap) error {
    ser, err := gql_loaded.Serialize()
    ctx.Log.Logf("test", "\n%s\n\n", ser)
    u_loaded := gql_loaded.(*GQLThread).Users[u1.ID()]
    child := gql_loaded.(Thread).Children()[0].(*SimpleThread)
    t1_loaded = child
    update_channel_2 = UpdateChannel(t1_loaded, 10, NodeID{})
    err = UseMoreStates(ctx, locked, gql, NewLockRequest(u_loaded, nil), func(locked NodeLockMap) error {
      ser, err := u_loaded.Serialize()
      ctx.Log.Logf("test", "\n%s\n\n", ser)
      return err
    })
    gql_loaded.Signal(ctx, AbortSignal, locked)
    return err
  })

  err = ThreadLoop(ctx, gql_loaded.(Thread), "restore")
  if errors.Is(err, ThreadAbortedError) {
    ctx.Log.Logf("test", "Main thread aborted by signal: %s", err)
  } else {
    fatalErr(t, err)
  }
  (*GraphTester)(t).WaitForValue(ctx, update_channel_2, "thread_aborted", 100*time.Millisecond, "Didn't received thread_aborted on t1_loaded from t1_loaded")

}

func TestGQLAuth(t * testing.T) {
  ctx := logTestContext(t, []string{"test", "gql"})
  key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
  fatalErr(t, err)

  p1_r := NewPerTagPolicy(RandID(), map[string]NodeActions{"gql": NewNodeActions(nil, []string{"read"})})
  p1 := &p1_r

  gql_t_r := NewGQLThread(RandID(), "GQL Thread", "init", ":0", ecdh.P256(), key, nil, nil)
  gql_t := &gql_t_r

  // p1 not written to DB, TODO: update write to follow links maybe
  err = UpdateStates(ctx, gql_t, NewLockRequest(gql_t, []string{"policies"}), func(locked NodeLockMap) error {
    return gql_t.AddPolicy(p1)
  })

  done := make(chan error, 1)

  var update_channel chan GraphSignal
  err = UseStates(ctx, gql_t, NewLockRequest(gql_t, nil), func(locked NodeLockMap) error {
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
    err := UseStates(ctx, gql_t, NewLockRequest(gql_t, []string{"signal}"}), func(locked NodeLockMap) error {
      return thread.Signal(ctx, CancelSignal, locked)
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
