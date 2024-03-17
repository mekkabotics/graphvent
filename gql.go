package graphvent

import (
  "context"
  "crypto/ecdh"
  "crypto/ecdsa"
  "crypto/elliptic"
  "crypto/rand"
  "crypto/x509"
  "encoding/json"
  "fmt"
  "io"
  "os"
  "net"
  "net/http"
  "reflect"
  "sync"
  "time"

  "github.com/gobwas/ws"
  "github.com/gobwas/ws/wsutil"
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/ast"
  "github.com/graphql-go/graphql/language/parser"
  "github.com/graphql-go/graphql/language/source"

  "crypto/x509/pkix"
  "encoding/pem"
  "math/big"

  "github.com/google/uuid"
)

func PrepResolve(p graphql.ResolveParams) (*ResolveContext, error) {
  resolve_context, ok := p.Context.Value("resolve").(*ResolveContext)
  if ok == false {
    return nil, fmt.Errorf("Bad resolve in params context")
  }

  return resolve_context, nil
}

// TODO: Make composabe by checking if K is a slice, then recursing in the same way that ExtractList does
func ExtractParam[K interface{}](p graphql.ResolveParams, name string) (K, error) {
  var zero K
  arg_if, ok := p.Args[name]
  if ok == false {
    return zero, fmt.Errorf("No Arg of name %s", name)
  }

  arg, ok := arg_if.(K)
  if ok == false {
    return zero, fmt.Errorf("Failed to cast arg %s(%+v) to %+v", name, arg_if, reflect.TypeOf(zero))
  }

  return arg, nil
}

func ExtractList[K interface{}](p graphql.ResolveParams, name string) ([]K, error) {
  var zero K

  arg_list, err := ExtractParam[[]interface{}](p, name)
  if err != nil {
    return nil, err
  }

  ret := make([]K, len(arg_list))
  for i, val := range(arg_list) {
    val_conv, ok := arg_list[i].(K)
    if ok == false {
      return nil, fmt.Errorf("Failed to cast arg %s[%d](%+v) to %+v", name, i, val, reflect.TypeOf(zero))
    }
    ret[i] = val_conv
  }

  return ret, nil
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
    if (window.authToken === undefined) {
      const root = ReactDOM.createRoot(document.getElementById('graphiql'));
      root.render(
        React.createElement(GraphiQL, {
          fetcher: GraphiQL.createFetcher({
            url: '/gql',
          }),
          defaultEditorToolsVisibility: true,
        }),
      );
    } else {
      authToken().then(function(res){
        const root = ReactDOM.createRoot(document.getElementById('graphiql'));
        root.render(
          React.createElement(GraphiQL, {
            fetcher: GraphiQL.createFetcher({
              url: '/gql',
              headers: {
                "Authorization": ` + "`${res}`" + `,
              },
            }),
            defaultEditorToolsVisibility: true,
          }),
        );
      });
    }
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

// Context passed to each resolve execution
type ResolveContext struct {
  // Resolution ID
  ID uuid.UUID

  // Channels for the gql extension to route data to this context
  Chans map[uuid.UUID]chan Signal

  // Graph Context this resolver is running under
  Context *Context

  // Pointer to the node that's currently processing this request
  Server *Node

  // The state data for the node processing this request
  Ext *GQLExt

  // Cache of resolved nodes
  NodeCache map[NodeID]NodeResult
}

func NewResolveContext(ctx *Context, server *Node, gql_ext *GQLExt) (*ResolveContext, error) {
  return &ResolveContext{
    ID: uuid.New(),
    Ext: gql_ext,
    Chans: map[uuid.UUID]chan Signal{},
    Context: ctx,
    NodeCache: map[NodeID]NodeResult{},
    Server: server,
  }, nil
}

func GQLHandler(ctx *Context, server *Node, gql_ext *GQLExt) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gql", "GQL REQUEST: %s", r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }
    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")

    resolve_context, err := NewResolveContext(ctx, server, gql_ext)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ERR: %s", err)
      json.NewEncoder(w).Encode(GQLUnauthorized(""))
      return
    }

    req_ctx := context.Background()
    req_ctx = context.WithValue(req_ctx, "resolve", resolve_context)

    str, err := io.ReadAll(r.Body)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_READ_ERR: %s", err)
      json.NewEncoder(w).Encode(fmt.Sprintf("%e", err))
      return
    }
    query := GQLPayload{}
    json.Unmarshal(str, &query)

    schema := ctx.Extensions[ExtTypeFor[GQLExt]()].Data.(graphql.Schema)

    params := graphql.Params{
      Schema: schema,
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
      ctx.Log.Logm("gql_errors", extra_fields, "wrong result, unexpected errors: %v", result.Errors)
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
    return err.Error()
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
  return "END_OF_FUNCTION"
}

func GQLWSDo(ctx * Context, p graphql.Params) chan *graphql.Result {
  operation := getOperationTypeOfReq(p)
  ctx.Log.Logf("gqlws", "GQLWSDO_OPERATION: %s - %+v", operation, p.RequestString)

  if operation == ast.OperationTypeSubscription {
    return graphql.Subscribe(p)
  } else {
    res := graphql.Do(p)
    return sendOneResultAndClose(res)
  }
}

func GQLWSHandler(ctx * Context, server *Node, gql_ext *GQLExt) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gqlws_new", "HANDLING %s",r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }

    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")
    resolve_context, err := NewResolveContext(ctx, server, gql_ext)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ERR: %s", err)
      return
    }

    req_ctx := context.Background()
    req_ctx = context.WithValue(req_ctx, "resolve", resolve_context)

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

          connection_params := struct {
            Payload struct {
              Token string `json:"token"`
            } `json:"payload"`
          }{}

          err := json.Unmarshal([]byte(msg_raw), &connection_params)
          if err != nil {
            ctx.Log.Logf("gqlws", "WS_UNMARSHAL_ERROR: %s - %+v", msg_raw, err)
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
            break
          }
        } else if msg.Type == "subscribe" {
          ctx.Log.Logf("gqlws", "SUBSCRIBE: %+v", msg.Payload)
          schema := ctx.Extensions[ExtTypeFor[GQLExt]()].Data.(graphql.Schema)
          params := graphql.Params{
            Schema: schema,
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
                ctx.Log.Logm("gql_errors", extra_fields, "ERROR: wrong result, unexpected errors: %+v", next.Errors)
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

type Interface struct {
  Interface *graphql.Interface
  Default *graphql.Object
  List *graphql.List
  Extensions []ExtType
}

type Type struct {
  Type *graphql.Object
  List *graphql.List
}

type Field struct {
  Ext ExtType
  Name string
  Field *graphql.Field
}

type NodeResult struct {
  NodeID NodeID
  NodeType NodeType
  Data map[ExtType]map[string]interface{}
}

type ListField struct {
  ACLName string
  Extension ExtType
  ResolveFn func(graphql.ResolveParams, *ResolveContext, reflect.Value) ([]NodeID, error)
}

type SelfField struct {
  ACLName string
  Extension ExtType
  ResolveFn func(graphql.ResolveParams, *ResolveContext, reflect.Value) (*NodeID, error)
}

type SubscriptionInfo struct {
  ID uuid.UUID
  Channel chan interface{}
}

type GQLExt struct {
  tcp_listener net.Listener
  http_server *http.Server
  http_done sync.WaitGroup

  subscriptions []SubscriptionInfo
  subscriptions_lock sync.RWMutex

  // map of read request IDs to response channels
  resolver_response map[uuid.UUID]chan Signal
  resolver_response_lock sync.RWMutex

  State string `gv:"state"`
  TLSKey []byte `gv:"tls_key"`
  TLSCert []byte `gv:"tls_cert"`
  Listen string `gv:"listen" gql:"GQLListen"`
}

func (ext *GQLExt) Load(ctx *Context, node *Node) error {
  ctx.Log.Logf("gql", "Loading GQL server extension on %s", node.ID)
  return ext.StartGQLServer(ctx, node)
}

func (ext *GQLExt) Unload(ctx *Context, node *Node) {
  ctx.Log.Logf("gql", "Unloading GQL server extension on %s", node.ID)
  err := ext.StopGQLServer()
  if err != nil {
    ctx.Log.Logf("gql", "Error unloading GQL server extension on %s: %s", node.ID, err)
  } else {
    ctx.Log.Logf("gql", "Unloaded GQL server extension on %s", node.ID)
  }
}

func (ext *GQLExt) PostDeserialize(*Context) error {
  ext.resolver_response = map[uuid.UUID]chan Signal{}
  ext.subscriptions = []SubscriptionInfo{}

  return nil
}

func (ext *GQLExt) AddSubscription(id uuid.UUID) (chan interface{}, error) {
  ext.subscriptions_lock.Lock()
  defer ext.subscriptions_lock.Unlock()

  for _, info := range(ext.subscriptions) {
    if info.ID == id {
      return nil, fmt.Errorf("%+v already in subscription list", info.ID)
    }
  }

  c := make(chan interface{}, 100)

  ext.subscriptions = append(ext.subscriptions, SubscriptionInfo{
    id,
    c,
  })

  return c, nil
}

func (ext *GQLExt) RemoveSubscription(id uuid.UUID) error {
  ext.subscriptions_lock.Lock()
  defer ext.subscriptions_lock.Unlock()

  for i, info := range(ext.subscriptions) {
    if info.ID == id {
      ext.subscriptions[i] = ext.subscriptions[len(ext.subscriptions)]
      ext.subscriptions = ext.subscriptions[:len(ext.subscriptions)-1]
      return nil
    }
  }

  return fmt.Errorf("%+v not in subscription list", id)
}

func (ext *GQLExt) FindResponseChannel(req_id uuid.UUID) chan Signal {
  ext.resolver_response_lock.RLock()
  response_chan, _ := ext.resolver_response[req_id]
  ext.resolver_response_lock.RUnlock()
  return response_chan
}

func (ext *GQLExt) GetResponseChannel(req_id uuid.UUID) chan Signal {
  response_chan := make(chan Signal, 1)
  ext.resolver_response_lock.Lock()
  ext.resolver_response[req_id] = response_chan
  ext.resolver_response_lock.Unlock()
  return response_chan
}

func (ext *GQLExt) FreeResponseChannel(req_id uuid.UUID) chan Signal {
  response_chan := ext.FindResponseChannel(req_id)

  if response_chan != nil {
    ext.resolver_response_lock.Lock()
    delete(ext.resolver_response, req_id)
    ext.resolver_response_lock.Unlock()
  }
  return response_chan
}

func (ext *GQLExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) ([]SendMsg, Changes) {
  // Process ReadResultSignalType by forwarding it to the waiting resolver
  var changes Changes = nil
  var messages []SendMsg = nil

  switch sig := signal.(type) {
  case *SuccessSignal:
    response_chan := ext.FreeResponseChannel(sig.ReqID)
    if response_chan != nil {
      select {
      case response_chan <- sig:
        ctx.Log.Logf("gql", "Forwarded success to resolver, %+v", sig.ReqID)
      default:
        ctx.Log.Logf("gql", "Resolver channel overflow %+v", sig)
      }
    } else {
      ctx.Log.Logf("gql", "received success signal response %+v with no mapped resolver", sig)
    }

  case *ErrorSignal:
    // TODO: Forward to resolver if waiting for it
    response_chan := ext.FreeResponseChannel(sig.ReqID)
    if response_chan != nil {
      select {
      case response_chan <- sig:
        ctx.Log.Logf("gql", "Forwarded error to resolver, %+v", sig.Error)
      default:
        ctx.Log.Logf("gql", "Resolver channel overflow %+v", sig)
      }

    } else {
      ctx.Log.Logf("gql", "received error signal response %+v with no mapped resolver", sig)
    }

  case *ReadResultSignal:
    response_chan := ext.FindResponseChannel(sig.ReqID)
    if response_chan != nil {
      select {
      case response_chan <- sig:
        ctx.Log.Logf("gql", "Forwarded to resolver, %+v", sig)
      default:
        ctx.Log.Logf("gql", "Resolver channel overflow %+v", sig)
      }
    } else {
      ctx.Log.Logf("gql", "Received read result that wasn't expected - %+v", sig)
    }

  case *StatusSignal:
    ext.subscriptions_lock.RLock()
    ctx.Log.Logf("gql", "forwarding status signal from %+v to resolvers %+v", sig.Source, ext.subscriptions)
    for _, resolver := range(ext.subscriptions) {
      select {
      case resolver.Channel <- sig:
        ctx.Log.Logf("gql_subscribe", "forwarded status signal to resolver: %+v", resolver.ID)
      default:
        ctx.Log.Logf("gql_subscribe", "resolver channel overflow: %+v", resolver.ID)
      }
    }
    ext.subscriptions_lock.RUnlock()
  }

  return messages, changes
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

func NewGQLExt(ctx *Context, listen string, tls_cert []byte, tls_key []byte) (*GQLExt, error) {
  if tls_cert == nil || tls_key == nil {
    ssl_key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
    if err != nil {
      return nil, err
    }

    ssl_key_bytes, err := x509.MarshalPKCS8PrivateKey(ssl_key)
    if err != nil {
      return nil, err
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

    ssl_cert, err := x509.CreateCertificate(rand.Reader, &template, &template, ssl_key.Public(), ssl_key)
    if err != nil {
      return nil, err
    }

    ssl_cert_pem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ssl_cert})

    tls_cert = ssl_cert_pem
    tls_key = ssl_key_pem
  }

  return &GQLExt{
    Listen: listen,
    resolver_response: map[uuid.UUID]chan Signal{},
    subscriptions: []SubscriptionInfo{},
    TLSCert: tls_cert,
    TLSKey: tls_key,
  }, nil
}

// Returns "${base}/${path}" if it's a file or "${base}/${path}/index.html" if it's a directory
// Returns os.ErrInvalid if "${base}/${path}/index.html" is a directory
func getContentPath(base string, path string) (string, error) {
  full_path := fmt.Sprintf("%s%s", base, path)
  path_info, err := os.Stat(full_path)

  if err != nil && err != os.ErrNotExist {
    return "", err
  } else if path_info.IsDir() == true {
    index_path := ""
    if path[len(path)-1] != '/' {
      index_path = fmt.Sprintf("%s%s/index.html", base, path)
    } else {
      index_path = fmt.Sprintf("%s%sindex.html", base, path)
    }
    index_info, err := os.Stat(index_path)
    if err != nil {
      return "", err
    } else if index_info.IsDir() == true {
      return index_path, os.ErrInvalid
    } else {
      return index_path, nil
    }
  } else {
    return full_path, nil
  }
}

func (ext *GQLExt) StartGQLServer(ctx *Context, node *Node) error {
  if ext.tcp_listener != nil || ext.http_server != nil {
    return fmt.Errorf("listener or server is still running, stop them first")
  }
  mux := http.NewServeMux()
  mux.HandleFunc("/gql", GQLHandler(ctx, node, ext))
  mux.HandleFunc("/gqlws", GQLWSHandler(ctx, node, ext))

  mux.HandleFunc("/graphiql", GraphiQLHandler())

  // Server the ./site directory to /site (TODO make configurable with better defaults)

  mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request){
    custom_path, err_1 := getContentPath("./custom", r.URL.Path)
    if err_1 != nil {
      static_path, err_2 := getContentPath("./site", r.URL.Path)
      if err_2 != nil {
        ctx.Log.Logf("gql", "File Resolve errors: %s - %s", err_1, err_2)
        w.WriteHeader(501)
        w.Write([]byte("{\"error\": \"server_error\"}"))
      } else {
        ctx.Log.Logf("gql", "STATIC_FILE: %s", static_path)
        file, err := os.Open(static_path)
        if err != nil {
          w.WriteHeader(501)
          w.Write([]byte("{\"error\": \"server_error\"}"))
        }
        http.ServeContent(w, r, static_path, time.Time{}, file)
      }
    } else {
      ctx.Log.Logf("gql", "CUSTOM_FILE: %s", custom_path)
      file, err := os.Open(custom_path)
      if err != nil {
        w.WriteHeader(501)
        w.Write([]byte("{\"error\": \"server_error\"}"))
      }
      http.ServeContent(w, r, custom_path, time.Time{}, file)
      w.WriteHeader(200)
    }
  })

  http_server := &http.Server{
    Addr: ext.Listen,
    Handler: mux,
  }

  l, err := net.Listen("tcp", http_server.Addr)
  if err != nil {
    return fmt.Errorf("Failed to start listener for server on %s", http_server.Addr)
  }

  ext.http_done.Add(1)
  go func(qql_ext *GQLExt) {
    defer ext.http_done.Done()

    err := http_server.Serve(l)
    if err != http.ErrServerClosed {
      panic(fmt.Sprintf("Failed to start gql server: %s", err))
    }
  }(ext)


  ext.tcp_listener = l
  ext.http_server = http_server
  ext.State = "running"
  return nil
}

func (ext *GQLExt) StopGQLServer() error {
  if ext.tcp_listener == nil || ext.http_server == nil {
    return fmt.Errorf("already shutdown, cannot shut down again" )
  }
  ext.http_server.Shutdown(context.TODO())
  ext.http_done.Wait()
  ext.tcp_listener = nil
  ext.http_server = nil
  ext.State = "stopped"
  return nil
}
