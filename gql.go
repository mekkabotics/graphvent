package graphvent

import (
  "time"
  "net"
  "net/http"
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/parser"
  "github.com/graphql-go/graphql/language/source"
  "github.com/graphql-go/graphql/language/ast"
  "context"
  "encoding/json"
  "encoding/base64"
  "io"
  "reflect"
  "fmt"
  "sync"
  "github.com/gobwas/ws"
  "github.com/gobwas/ws/wsutil"
  "strings"
  "crypto/ecdh"
  "crypto/ecdsa"
  "crypto/elliptic"
  "crypto/sha512"
  "crypto/rand"
  "crypto/x509"
  "crypto/tls"
  "crypto/x509/pkix"
  "math/big"
  "encoding/pem"
)

type AuthReqJSON struct {
  Time time.Time `json:"time"`
  Pubkey []byte `json:"pubkey"`
  ECDHPubkey []byte `json:"ecdh_client"`
  Signature []byte `json:"signature"`
}

func NewAuthReqJSON(curve ecdh.Curve, id *ecdsa.PrivateKey) (AuthReqJSON, *ecdh.PrivateKey, error) {
  ec_key, err := curve.GenerateKey(rand.Reader)
  if err != nil {
    return AuthReqJSON{}, nil, err
  }
  now := time.Now()
  time_bytes, err := now.MarshalJSON()
  if err != nil {
    return AuthReqJSON{}, nil, err
  }
  sig_data := append(ec_key.PublicKey().Bytes(), time_bytes...)
  sig_hash := sha512.Sum512(sig_data)
  sig, err := ecdsa.SignASN1(rand.Reader, id, sig_hash[:])

  id_ecdh, err := id.ECDH()
  if err != nil {
    return AuthReqJSON{}, nil, err
  }

  return AuthReqJSON{
    Time: now,
    Pubkey: id_ecdh.PublicKey().Bytes(),
    ECDHPubkey: ec_key.PublicKey().Bytes(),
    Signature: sig,
  }, ec_key, nil
}

type AuthRespJSON struct {
  Granted time.Time `json:"granted"`
  ECDHPubkey []byte `json:"echd_server"`
  Signature []byte `json:"signature"`
}

func NewAuthRespJSON(thread *GQLThread, req AuthReqJSON) (AuthRespJSON, *ecdsa.PublicKey, []byte, error) {
    // Check if req.Time is within +- 1 second of now
    now := time.Now()
    earliest := now.Add(-1 * time.Second)
    latest := now.Add(1 * time.Second)
    // If req.Time is before the earliest acceptable time, or after the latest acceptible time
    if req.Time.Compare(earliest) == -1 {
      return AuthRespJSON{}, nil, nil, fmt.Errorf("GQL_AUTH_TIME_TOO_LATE: %s", req.Time)
    } else if req.Time.Compare(latest) == 1 {
      return AuthRespJSON{}, nil, nil, fmt.Errorf("GQL_AUTH_TIME_TOO_EARLY: %s", req.Time)
    }

    x, y := elliptic.Unmarshal(thread.Key.Curve, req.Pubkey)
    if x == nil {
      return AuthRespJSON{}, nil, nil, fmt.Errorf("GQL_AUTH_UNMARSHAL_FAIL: %+v", req.Pubkey)
    }

    remote, err := thread.ECDH.NewPublicKey(req.ECDHPubkey)
    if err != nil {
      return AuthRespJSON{}, nil, nil, err
    }

    // Verify the signature
    time_bytes, _ := req.Time.MarshalJSON()
    sig_data := append(req.ECDHPubkey, time_bytes...)
    sig_hash := sha512.Sum512(sig_data)

    remote_key := &ecdsa.PublicKey{
      Curve: thread.Key.Curve,
      X: x,
      Y: y,
    }

    verified := ecdsa.VerifyASN1(
      remote_key,
      sig_hash[:],
      req.Signature,
    )

    if verified == false {
      return AuthRespJSON{}, nil, nil, fmt.Errorf("GQL_AUTH_VERIFY_FAIL: %+v", req)
    }

    ec_key, err := thread.ECDH.GenerateKey(rand.Reader)
    if err != nil {
      return AuthRespJSON{}, nil, nil, err
    }

    ec_key_pub := ec_key.PublicKey().Bytes()

    granted := time.Now()
    time_ser, _ := granted.MarshalJSON()
    resp_sig_data := append(ec_key_pub, time_ser...)
    resp_sig_hash := sha512.Sum512(resp_sig_data)

    resp_sig, err := ecdsa.SignASN1(rand.Reader, thread.Key, resp_sig_hash[:])
    if err != nil {
      return AuthRespJSON{}, nil, nil, err
    }

    shared_secret, err := ec_key.ECDH(remote)
    if err != nil {
      return AuthRespJSON{}, nil, nil, err
    }

    return AuthRespJSON{
      Granted: granted,
      ECDHPubkey: ec_key_pub,
      Signature: resp_sig,
    }, remote_key, shared_secret, nil
}

func ParseAuthRespJSON(resp AuthRespJSON, ecdsa_curve elliptic.Curve, ecdh_curve ecdh.Curve, ec_key *ecdh.PrivateKey) ([]byte, error) {
  remote, err := ecdh_curve.NewPublicKey(resp.ECDHPubkey)
  if err != nil {
    return nil, err
  }

  shared_secret, err := ec_key.ECDH(remote)
  if err != nil {
    return nil, err
  }

  return shared_secret, nil
}

func AuthHandler(ctx *Context, server *GQLThread) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r *http.Request) {
    ctx.Log.Logf("gql", "GQL_AUTH_REQUEST: %s", r.RemoteAddr)
    enableCORS(&w)

    str, err := io.ReadAll(r.Body)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_READ_ERR: %e", err)
      return
    }

    var req AuthReqJSON
    err = json.Unmarshal([]byte(str), &req)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_UNMARHSHAL_ERR: %e", err)
      return
    }

    resp, remote_id, shared, err := NewAuthRespJSON(server, req)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_VERIFY_ERROR: %s", err)
      return
    }

    ser, err := json.Marshal(resp)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_RESP_MARSHAL_ERR: %e", err)
      return
    }

    wrote, err := w.Write(ser)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_RESP_ERR: %e", err)
      return
    } else if wrote != len(ser) {
      ctx.Log.Logf("gql", "GQL_AUTH_RESP_BAD_LENGTH: %d/%d", wrote, len(ser))
      return
    }

    key_id := KeyID(remote_id)

    _, exists := server.Users[key_id]
    if exists {
      ctx.Log.Logf("gql", "REFRESHING AUTH FOR %s", key_id)
    } else {
      ctx.Log.Logf("gql", "AUTHORIZING NEW USER %s - %s", key_id, shared)

      new_user := NewUser(fmt.Sprintf("GQL_USER %s", key_id.String()), time.Now(), remote_id, shared, []string{"gql"})
      err := UpdateStates(ctx, []Node{server, &new_user}, func(nodes NodeMap) error {
        server.Users[key_id] = &new_user
        return nil
      })
      if err != nil {
        ctx.Log.Logf("gql", "GQL_AUTH_UPDATE_ERR: %s", err)
        return
      }
    }

  }
}

func GraphiQLHandler() func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    graphiql_string := fmt.Sprintf(`
    <!--
    *  Copyright (c) 2021 GraphQL Contributors
    *  All rights reserved.
    *
    *  This source code is licensed under the license found in the
    *  LICENSE file in the root directory of this source tree.
    -->
    <!DOCTYPE html>
    <html lang="en">
    <head>
      <title>GraphiQL</title>
      <style>
        body {
          height: 100%%;
          margin: 0;
          width: 100%%;
          overflow: hidden;
        }
        #graphiql {
          height: 100vh;
        }
      </style>
      <!--
        This GraphiQL example depends on Promise and fetch, which are available in
        modern browsers, but can be "polyfilled" for older browsers.
        GraphiQL itself depends on React DOM.
        If you do not want to rely on a CDN, you can host these files locally or
        include them directly in your favored resource bundler.
      -->
      <script
        crossorigin
        src="https://unpkg.com/react@18/umd/react.development.js"
      ></script>
      <script
        crossorigin
        src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"
      ></script>
      <!--
        These two files can be found in the npm module, however you may wish to
        copy them directly into your environment, or perhaps include them in your
        favored resource bundler.
       -->
      <link rel="stylesheet" href="https://unpkg.com/graphiql/graphiql.min.css" />
    </head>
    <body>
      <div id="graphiql">Loading...</div>
      <script
        src="https://unpkg.com/graphiql/graphiql.min.js"
        type="application/javascript"
      ></script>
      <script>
        const root = ReactDOM.createRoot(document.getElementById('graphiql'));
        root.render(
          React.createElement(GraphiQL, {
            fetcher: GraphiQL.createFetcher({
              url: '/gql',
            }),
            defaultEditorToolsVisibility: true,
          }),
        );
      </script>
    </body>
    </html>
    `)

    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.WriteHeader(http.StatusOK)
    io.WriteString(w, graphiql_string)
  }

}

type GQLPayload struct {
  OperationName string `json:"operationName,omitempty"`
  Query string `json:"query,omitempty"`
  Variables map[string]interface{} `json:"variables,omitempty"`
  Extensions map[string]interface{} `json:"extensions,omitempty"`
  Data string `json:"data,omitempty"`
}

type GQLWSMsg struct {
  ID      string `json:"id,omitempty"`
  Type    string `json:"type"`
  Payload GQLPayload `json:"payload,omitempty"`
}

func enableCORS(w *http.ResponseWriter) {
 (*w).Header().Set("Access-Control-Allow-Origin", "*")
 (*w).Header().Set("Access-Control-Allow-Credentials", "true")
 (*w).Header().Set("Access-Control-Allow-Headers", "*")
 (*w).Header().Set("Access-Control-Allow-Methods", "*")
}

type GQLUnauthorized string

func (e GQLUnauthorized) Is(target error) bool {
  error_type := reflect.TypeOf(GQLUnauthorized(""))
  target_type := reflect.TypeOf(target)
  return error_type == target_type
}

func (e GQLUnauthorized) Error() string {
  return fmt.Sprintf("GQL_UNAUTHORIZED_ERROR: %s", string(e))
}

func (e GQLUnauthorized) MarshalJSON() ([]byte, error) {
  return json.MarshalIndent(&struct{
    Error string `json:"error"`
  }{
    Error: string(e),
  }, "", "  ")
}

func checkForAuthHeader(header http.Header) (string, bool) {
  auths, ok := header["Authorization"]
  if ok == false {
    return "", false
  }
  for _, auth := range(auths) {
    parts := strings.SplitN(auth, " ", 2)
    if len(parts) != 2 {
      continue
    }
    if parts[0] == "TM" {
      return parts[1], true
    }
  }

  return "", false
}

func CheckAuth(server *GQLThread, r *http.Request) (*User, error) {
  username, password, ok := r.BasicAuth()
  if ok == false {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: no auth header included in request header")
  }

  auth_id, err := ParseID(username)
  if err != nil {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: failed to parse ID from auth username: %s", username)
  }

  user, exists := server.Users[auth_id]
  if exists == false {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: no existing authorization for client %s", auth_id)
  }

  if base64.StdEncoding.EncodeToString(user.Shared) != password {
    return nil, fmt.Errorf("GQL_AUTH_FAIL")
  }

  return user, nil
}

func GQLHandler(ctx * Context, server * GQLThread) func(http.ResponseWriter, *http.Request) {
  gql_ctx := context.Background()
  gql_ctx = context.WithValue(gql_ctx, "graph_context", ctx)
  gql_ctx = context.WithValue(gql_ctx, "gql_server", server)

  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gql", "GQL REQUEST: %s", r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }
    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")

    user, err := CheckAuth(server, r)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ERR: %s", err)
      json.NewEncoder(w).Encode(GQLUnauthorized(fmt.Sprintf("%s", err)))
      return
    }
    req_ctx := context.WithValue(gql_ctx, "user", user)

    str, err := io.ReadAll(r.Body)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_READ_ERR: %s", err)
      json.NewEncoder(w).Encode(fmt.Sprintf("%e", err))
      return
 }
    query := GQLPayload{}
    json.Unmarshal(str, &query)

    params := graphql.Params{
      Schema: ctx.GQL.Schema,
      Context: req_ctx,
      RequestString: query.Query,
    }
    if query.OperationName != "" {
      params.OperationName = query.OperationName
    }
    if len(query.Variables) > 0 {
      params.VariableValues = query.Variables
    }
    result := graphql.Do(params)
    if len(result.Errors) > 0 {
      extra_fields := map[string]interface{}{}
      extra_fields["body"] = string(str)
      extra_fields["headers"] = r.Header
      ctx.Log.Logm("gql", extra_fields, "wrong result, unexpected errors: %v", result.Errors)
    }
    json.NewEncoder(w).Encode(result)
  }
}

func sendOneResultAndClose(res *graphql.Result) chan *graphql.Result {
  resultChannel := make(chan *graphql.Result)
  go func() {
    resultChannel <- res
    close(resultChannel)
  }()
  return resultChannel
}


func getOperationTypeOfReq(p graphql.Params) string{
  source := source.NewSource(&source.Source{
    Body: []byte(p.RequestString),
    Name: "GraphQL request",
  })

  AST, err := parser.Parse(parser.ParseParams{Source: source})
  if err != nil {
    return ""
  }

  for _, node := range AST.Definitions {
    if operationDef, ok := node.(*ast.OperationDefinition); ok {
      name := ""
      if operationDef.Name != nil {
        name = operationDef.Name.Value
      }
      if name == p.OperationName || p.OperationName == "" {
        return operationDef.Operation
      }
    }
  }
  return ""
}

func GQLWSDo(ctx * Context, p graphql.Params) chan *graphql.Result {
  operation := getOperationTypeOfReq(p)
  ctx.Log.Logf("gqlws", "GQLWSDO_OPERATION: %s %+v", operation, p.RequestString)

  if operation == ast.OperationTypeSubscription {
    return graphql.Subscribe(p)
  }

  res := graphql.Do(p)
  return sendOneResultAndClose(res)
}

func GQLWSHandler(ctx * Context, server * GQLThread) func(http.ResponseWriter, *http.Request) {
  gql_ctx := context.Background()
  gql_ctx = context.WithValue(gql_ctx, "graph_context", ctx)
  gql_ctx = context.WithValue(gql_ctx, "gql_server", server)

  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gqlws_new", "HANDLING %s",r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }

    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")
    user, err := CheckAuth(server, r)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ERR: %s", err)
      return
    }
    req_ctx := context.WithValue(gql_ctx, "user", user)

    u := ws.HTTPUpgrader{
      Protocol: func(protocol string) bool {
        ctx.Log.Logf("gqlws", "UPGRADE_PROTOCOL: %s", string(protocol))
        if string(protocol) == "graphql-transport-ws" || string(protocol) == "graphql-ws" {
          return true
        }
        return false
      },
    }
    conn, _, _, err := u.Upgrade(r, w)
    if err == nil {
      defer conn.Close()
      conn_state := "init"
      for {
        msg_raw, op, err := wsutil.ReadClientData(conn)
        ctx.Log.Logf("gqlws_hb", "MSG: %s\nOP: 0x%02x\nERR: %+v\n", string(msg_raw), op, err)
        msg := GQLWSMsg{}
        json.Unmarshal(msg_raw, &msg)
        if err != nil {
          ctx.Log.Logf("gqlws", "WS_CLIENT_ERROR")
          break
        }
        if msg.Type == "connection_init" {
          if conn_state != "init" {
            ctx.Log.Logf("gqlws", "WS_CLIENT_ERROR: INIT WHILE IN %s", conn_state)
            break
          }
          conn_state = "ready"
          err = wsutil.WriteServerMessage(conn, 1, []byte("{\"type\": \"connection_ack\"}"))
          if err != nil {
            ctx.Log.Logf("gqlws", "WS_SERVER_ERROR: FAILED TO SEND connection_ack")
            break
          }
        } else if msg.Type == "ping" {
          ctx.Log.Logf("gqlws_hb", "PING FROM %s", r.RemoteAddr)
          err = wsutil.WriteServerMessage(conn, 1, []byte("{\"type\": \"pong\"}"))
          if err != nil {
            ctx.Log.Logf("gqlws", "WS_SERVER_ERROR: FAILED TO SEND PONG")
          }
        } else if msg.Type == "subscribe" {
          ctx.Log.Logf("gqlws", "SUBSCRIBE: %+v", msg.Payload)
          params := graphql.Params{
            Schema: ctx.GQL.Schema,
            Context: req_ctx,
            RequestString: msg.Payload.Query,
          }
          if msg.Payload.OperationName != "" {
            params.OperationName = msg.Payload.OperationName
          }
          if len(msg.Payload.Variables) > 0 {
            params.VariableValues = msg.Payload.Variables
          }

          res_chan := GQLWSDo(ctx, params)
          if res_chan == nil {
            ctx.Log.Logf("gqlws", "res_chan is nil")
          } else {
            ctx.Log.Logf("gqlws", "res_chan: %+v", res_chan)
          }

          go func(res_chan chan *graphql.Result) {
            for {
              next, ok := <-res_chan
              if ok == false {
                ctx.Log.Logf("gqlws", "response channel was closed")
                return
              }
              if next == nil {
                ctx.Log.Logf("gqlws", "NIL_ON_CHANNEL")
                return
              }
              if len(next.Errors) > 0 {
                extra_fields := map[string]interface{}{}
                extra_fields["query"] = string(msg.Payload.Query)
                ctx.Log.Logm("gqlws", extra_fields, "ERROR: wrong result, unexpected errors: %+v", next.Errors)
                continue
              }
              ctx.Log.Logf("gqlws", "DATA: %+v", next.Data)
              data, err := json.Marshal(next.Data)
              if err != nil {
                ctx.Log.Logf("gqlws", "ERROR: %+v", err)
                continue
              }
              msg, err := json.Marshal(GQLWSMsg{
                ID: msg.ID,
                Type: "next",
                Payload: GQLPayload{
                  Data: string(data),
                },
              })
              if err != nil {
                ctx.Log.Logf("gqlws", "ERROR: %+v", err)
                continue
              }

              err = wsutil.WriteServerMessage(conn, 1, msg)
              if err != nil {
                ctx.Log.Logf("gqlws", "ERROR: %+v", err)
                continue
              }
            }
          }(res_chan)
        } else {
        }
      }
      return
    } else {
      panic("Failed to upgrade websocket")
    }
  }
}

type GQLThread struct {
  SimpleThread
  tcp_listener net.Listener
  http_server *http.Server
  http_done *sync.WaitGroup
  tls_key []byte
  tls_cert []byte
  Listen string
  Users map[NodeID]*User
  Key *ecdsa.PrivateKey
  ECDH ecdh.Curve
}

func (thread * GQLThread) Type() NodeType {
  return NodeType("gql_thread")
}

func (thread * GQLThread) Serialize() ([]byte, error) {
  thread_json := NewGQLThreadJSON(thread)
  return json.MarshalIndent(&thread_json, "", "  ")
}

func (thread * GQLThread) DeserializeInfo(ctx *Context, data []byte) (ThreadInfo, error) {
  var info ParentThreadInfo
  err := json.Unmarshal(data, &info)
  if err != nil {
    return nil, err
  }
  return &info, nil
}

type GQLThreadJSON struct {
  SimpleThreadJSON
  Listen string `json:"listen"`
  Users []string `json:"users"`
  Key []byte `json:"key"`
  ECDH uint8 `json:"ecdh_curve"`
  TLSKey []byte `json:"ssl_key"`
  TLSCert []byte `json:"ssl_cert"`
}

var ecdsa_curves = map[uint8]elliptic.Curve{
  0: elliptic.P256(),
}

var ecdsa_curve_ids = map[elliptic.Curve]uint8{
  elliptic.P256(): 0,
}

var ecdh_curves = map[uint8]ecdh.Curve{
  0: ecdh.P256(),
}

var ecdh_curve_ids = map[ecdh.Curve]uint8{
  ecdh.P256(): 0,
}

func NewGQLThreadJSON(thread *GQLThread) GQLThreadJSON {
  thread_json := NewSimpleThreadJSON(&thread.SimpleThread)

  ser_key, err := x509.MarshalECPrivateKey(thread.Key)
  if err != nil {
    panic(err)
  }

  users := make([]string, len(thread.Users))
  i := 0
  for id, _ := range(thread.Users) {
    users[i] = id.String()
    i += 1
  }

  return GQLThreadJSON{
    SimpleThreadJSON: thread_json,
    Listen: thread.Listen,
    Users: users,
    Key: ser_key,
    ECDH: ecdh_curve_ids[thread.ECDH],
    TLSKey: thread.tls_key,
    TLSCert: thread.tls_cert,
  }
}

func LoadGQLThread(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j GQLThreadJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  ecdh_curve, ok := ecdh_curves[j.ECDH]
  if ok == false {
    return nil, fmt.Errorf("%d is not a known ECDH curve ID", j.ECDH)
  }

  key, err := x509.ParseECPrivateKey(j.Key)
  if err != nil {
    return nil, err
  }

  thread := NewGQLThread(id, j.Name, j.StateName, j.Listen, ecdh_curve, key, j.TLSCert, j.TLSKey)
  nodes[id] = &thread

  thread.Users = map[NodeID]*User{}
  for _, id_str := range(j.Users) {
    id, err := ParseID(id_str)
    if err != nil {
      return nil, err
    }
    user, err := LoadNodeRecurse(ctx, id, nodes)
    if err != nil {
      return nil, err
    }
    thread.Users[id] = user.(*User)
  }

  err = RestoreSimpleThread(ctx, &thread, j.SimpleThreadJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &thread, nil
}

func NewGQLThread(id NodeID, name string, state_name string, listen string, ecdh_curve ecdh.Curve, key *ecdsa.PrivateKey, tls_cert []byte, tls_key []byte) GQLThread {
  if tls_cert == nil || tls_key == nil {
    ssl_key, err := ecdsa.GenerateKey(key.Curve, rand.Reader)
    if err != nil {
      panic(err)
    }

    ssl_key_bytes, err := x509.MarshalECPrivateKey(ssl_key)
    if err != nil {
      panic(err)
    }

    ssl_key_pem := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: ssl_key_bytes})

    serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
    serialNumber, _ := rand.Int(rand.Reader, serialNumberLimit)
    notBefore := time.Now()
    notAfter := notBefore.Add(365*24*time.Hour)
    template := x509.Certificate{
      SerialNumber: serialNumber,
      Subject: pkix.Name{
        Organization: []string{"mekkanized"},
      },
      NotBefore: notBefore,
      NotAfter: notAfter,
      KeyUsage: x509.KeyUsageDigitalSignature,
      ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
      BasicConstraintsValid: true,
    }

    ssl_cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &ssl_key.PublicKey, ssl_key)
    if err != nil {
      panic(err)
    }

    ssl_cert_pem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ssl_cert})

    tls_cert = ssl_cert_pem
    tls_key = ssl_key_pem
  }
  return GQLThread{
    SimpleThread: NewSimpleThread(id, name, state_name, reflect.TypeOf((*ParentThreadInfo)(nil)), gql_actions, gql_handlers),
    Listen: listen,
    Users: map[NodeID]*User{},
    http_done: &sync.WaitGroup{},
    Key: key,
    ECDH: ecdh_curve,
    tls_cert: tls_cert,
    tls_key: tls_key,
  }
}

var gql_actions ThreadActions = ThreadActions{
  "wait": ThreadWait,
  "restore": func(ctx * Context, thread Thread) (string, error) {
    // Start all the threads that should be "started"
    ctx.Log.Logf("gql", "GQL_THREAD_RESTORE: %s", thread.ID())
    ThreadRestore(ctx, thread)
    return "start_server", nil
  },
  "start": func(ctx * Context, thread Thread) (string, error) {
    ctx.Log.Logf("gql", "GQL_START")
    err := ThreadStart(ctx, thread)
    if err != nil {
      return "", err
    }

    return "start_server", nil
  },
  "start_server": func(ctx * Context, thread Thread) (string, error) {
    server, ok := thread.(*GQLThread)
    if ok == false {
      return "", fmt.Errorf("GQL_THREAD_START: %s is not GQLThread, %+v", thread.ID(), thread.State())
    }

    ctx.Log.Logf("gql", "GQL_START_SERVER")
    // Serve the GQL http and ws handlers
    mux := http.NewServeMux()
    mux.HandleFunc("/auth", AuthHandler(ctx, server))
    mux.HandleFunc("/gql", GQLHandler(ctx, server))
    mux.HandleFunc("/gqlws", GQLWSHandler(ctx, server))

    // Server a graphiql interface(TODO make configurable whether to start this)
    mux.HandleFunc("/graphiql", GraphiQLHandler())

    // Server the ./site directory to /site (TODO make configurable with better defaults)
    fs := http.FileServer(http.Dir("./site"))
    mux.Handle("/site/", http.StripPrefix("/site", fs))

    http_server := &http.Server{
      Addr: server.Listen,
      Handler: mux,
    }

    l, err := net.Listen("tcp", http_server.Addr)
    if err != nil {
      return "", fmt.Errorf("Failed to start listener for server on %s", http_server.Addr)
    }

    cert, err := tls.X509KeyPair(server.tls_cert, server.tls_key)
    if err != nil {
      return "", err
    }

    config := tls.Config{
      Certificates: []tls.Certificate{cert},
      NextProtos: []string{"http/1.1"},
    }

    listener := tls.NewListener(l, &config)

    server.http_done.Add(1)
    go func(server *GQLThread) {
      defer server.http_done.Done()

      err := http_server.Serve(listener)
      if err != http.ErrServerClosed {
          panic(fmt.Sprintf("Failed to start gql server: %s", err))
      }
    }(server)


    UseStates(ctx, []Node{server}, func(nodes NodeMap)(error){
      server.tcp_listener = listener
      server.http_server = http_server
      return server.Signal(ctx, NewSignal(server, "server_started"), nodes)
    })

    return "wait", nil
  },
}

var gql_handlers ThreadHandlers = ThreadHandlers{
  "child_added": func(ctx * Context, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_THREAD_CHILD_ADDED: %+v", signal)
    UpdateStates(ctx, []Node{thread}, func(nodes NodeMap) error {
      should_run, exists := thread.ChildInfo(signal.Source()).(*ParentThreadInfo)
      if exists == false {
        ctx.Log.Logf("gql", "GQL_THREAD_CHILD_ADDED: tried to start %s whis is not a child")
        return nil
      }
      if should_run.Start == true {
        ChildGo(ctx, thread, thread.Child(signal.Source()), should_run.StartAction)
      }
      return nil
    })
    return "wait", nil
  },
  "start_child": func(ctx *Context, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_START_CHILD")
    sig, ok := signal.(StartChildSignal)
    if ok == false {
      ctx.Log.Logf("gql", "GQL_START_CHILD_BAD_SIGNAL: %+v", signal)
      return "wait", nil
    }

    err := ThreadStartChild(ctx, thread, sig)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_START_CHILD_ERR: %s", err)
    } else {
      ctx.Log.Logf("gql", "GQL_START_CHILD: %s", sig.ChildID.String())
    }

    return "wait", nil
  },
  "abort": func(ctx * Context, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_ABORT")
    server := thread.(*GQLThread)
    server.http_server.Shutdown(context.TODO())
    server.http_done.Wait()
    return ThreadAbort(ctx, thread, signal)
  },
  "cancel": func(ctx * Context, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_CANCEL")
    server := thread.(*GQLThread)
    server.http_server.Shutdown(context.TODO())
    server.http_done.Wait()
    return ThreadCancel(ctx, thread, signal)
  },
}

