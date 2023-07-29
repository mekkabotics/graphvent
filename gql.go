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
  "crypto/rand"
  "crypto/x509"
  "crypto/tls"
  "crypto/x509/pkix"
  "math/big"
  "encoding/pem"
  "github.com/google/uuid"
)

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

// Context passed to each resolve execution
type ResolveContext struct {
  // ID generated for the context so the gql extension can route data to it
  ID uuid.UUID

  // Channel for the gql extension to route data to this context
  Chan chan *ReadResultSignal

  // Graph Context this resolver is running under
  Context *Context

  // GQL Extension context this resolver is running under
  GQLContext *GQLExtContext

  // Pointer to the node that's currently processing this request
  Server *Node

  // The state data for the node processing this request
  Ext *GQLExt

  // ID of the user that made this request
  // TODO: figure out auth
  User NodeID
}

const GQL_RESOLVER_CHAN_SIZE = 10
func NewResolveContext(ctx *Context, server *Node, gql_ext *GQLExt, r *http.Request) (*ResolveContext, error) {
  username, _, ok := r.BasicAuth()
  if ok == false {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: no auth header included in request header")
  }

  auth_id, err := ParseID(username)
  if err != nil {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: failed to parse ID from auth username: %s", username)
  }

  return &ResolveContext{
    Ext: gql_ext,
    ID: uuid.New(),
    Chan: make(chan *ReadResultSignal, GQL_RESOLVER_CHAN_SIZE),
    Context: ctx,
    GQLContext: ctx.Extensions[Hash(GQLExtType)].Data.(*GQLExtContext),
    Server: server,
    User: auth_id,
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

    resolve_context, err := NewResolveContext(ctx, server, gql_ext, r)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ERR: %s", err)
      json.NewEncoder(w).Encode(GQLUnauthorized(fmt.Sprintf("%s", err)))
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

    gql_context := ctx.Extensions[Hash(GQLExtType)].Data.(*GQLExtContext)

    params := graphql.Params{
      Schema: gql_context.Schema,
      Context: req_ctx,
      RequestString: query.Query,
    }
    if query.OperationName != "" {
      params.OperationName = query.OperationName
    }
    if len(query.Variables) > 0 {
      params.VariableValues = query.Variables
    }

    gql_ext.resolver_chans_lock.Lock()
    gql_ext.resolver_chans[resolve_context.ID] = resolve_context.Chan
    gql_ext.resolver_chans_lock.Unlock()

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

func GQLWSHandler(ctx * Context, server *Node, gql_ext *GQLExt) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gqlws_new", "HANDLING %s",r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }

    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")
    resolve_context, err := NewResolveContext(ctx, server, gql_ext, r)
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
          gql_context := ctx.Extensions[Hash(GQLExtType)].Data.(*GQLExtContext)
          params := graphql.Params{
            Schema: gql_context.Schema,
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

func NewGQLNodeType(node_type NodeType, interfaces []*graphql.Interface, init func(*Type)) *Type {
  var gql Type
  gql.Type = graphql.NewObject(graphql.ObjectConfig{
    Name: string(node_type),
    Interfaces: interfaces,
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      node, ok := p.Value.(*Node)
      if ok == false {
        return false
      }
      return node.Type == node_type
    },
    Fields: graphql.Fields{},
  })
  gql.List = graphql.NewList(gql.Type)

  init(&gql)
  return &gql
}

type Field struct {
  Ext ExtType
  Name string
}

// GQL Specific Context information
type GQLExtContext struct {
  // Generated GQL schema
  Schema graphql.Schema

  // Custom graphql types, mapped to NodeTypes
  NodeTypes map[NodeType]*graphql.Object
  Interfaces []*Interface
  Fields map[string]Field

  // Schema parameters
  Types []graphql.Type
  Query *graphql.Object
  Mutation *graphql.Object
  Subscription *graphql.Object
}

func (ctx *GQLExtContext) GetACLFields(obj_name string, names []string) (map[ExtType][]string, error) {
  ext_fields := map[ExtType][]string{}
  for _, name := range(names) {
    switch name {
    case "ID":
    case "TypeHash":
    default:
     field, exists := ctx.Fields[name]
     if exists == false {
       return nil, fmt.Errorf("%s is not a know field in GQLContext, cannot resolve", name)
     }

     ext, exists := ext_fields[field.Ext]
     if exists == false {
       ext = []string{}
     }
     ext = append(ext, field.Name)
     ext_fields[field.Ext] = ext
    }
  }

  return ext_fields, nil
}

func BuildSchema(ctx *GQLExtContext) (graphql.Schema, error) {
  schemaConfig := graphql.SchemaConfig{
    Types: ctx.Types,
    Query: ctx.Query,
    Mutation: ctx.Mutation,
    Subscription: ctx.Subscription,
  }

  return graphql.NewSchema(schemaConfig)
}

func (ctx *GQLExtContext) RegisterField(gql_name string, ext ExtType, acl_name string) error {
  _, exists := ctx.Fields[gql_name]
  if exists == true {
    return fmt.Errorf("%s is already a field in the context, cannot add again", gql_name)
  }

  ctx.Fields[gql_name] = Field{ext, acl_name}
  return nil
}

func (ctx *GQLExtContext) AddInterface(i *Interface) error {
  if i == nil {
    return fmt.Errorf("interface is nil")
  }

  if i.Interface == nil || i.Extensions == nil || i.Default == nil || i.List == nil {
    return fmt.Errorf("invalid interface, contains nil")
  }

  ctx.Interfaces = append(ctx.Interfaces, i)
  ctx.Types = append(ctx.Types, i.Default)

  return nil
}

func (ctx *GQLExtContext) RegisterNodeType(node_type NodeType, gql_type *graphql.Object) error {
  if gql_type == nil {
    return fmt.Errorf("gql_type is nil")
  }

  _, exists := ctx.NodeTypes[node_type]
  if exists == true {
    return fmt.Errorf("%s already in GQLExtContext.NodeTypes", node_type)
  }

  ctx.NodeTypes[node_type] = gql_type
  ctx.Types = append(ctx.Types, gql_type)

  return nil
}

func NewGQLExtContext() *GQLExtContext {
  query := graphql.NewObject(graphql.ObjectConfig{
    Name: "Query",
    Fields: graphql.Fields{},
  })

  query.AddFieldConfig("Self", QuerySelf)
  query.AddFieldConfig("Node", QueryNode)

  mutation := graphql.NewObject(graphql.ObjectConfig{
    Name: "Mutation",
    Fields: graphql.Fields{},
  })

  mutation.AddFieldConfig("stop", MutationStop)

  subscription := graphql.NewObject(graphql.ObjectConfig{
    Name: "Subscription",
    Fields: graphql.Fields{},
  })

  subscription.AddFieldConfig("Self", SubscriptionSelf)
  subscription.AddFieldConfig("Self", SubscriptionNode)

  context := GQLExtContext{
    Schema: graphql.Schema{},
    Types: []graphql.Type{},
    Query: query,
    Mutation: mutation,
    Subscription: subscription,
    NodeTypes: map[NodeType]*graphql.Object{},
    Interfaces: []*Interface{},
  }

  var err error
  err = context.AddInterface(InterfaceNode)
  if err != nil {
    panic(err)
  }
  err = context.AddInterface(InterfaceLockable)
  if err != nil {
    panic(err)
  }

  schema, err := BuildSchema(&context)
  if err != nil {
    panic(err)
  }

  context.Schema = schema

  return &context
}

type GQLExt struct {
  tcp_listener net.Listener
  http_server *http.Server
  http_done sync.WaitGroup

  // map of read request IDs to gql request ID
  resolver_reads map[uuid.UUID]uuid.UUID
  resolver_reads_lock sync.RWMutex
  // map of gql request ID to active channel
  resolver_chans map[uuid.UUID]chan *ReadResultSignal
  resolver_chans_lock sync.RWMutex

  tls_key []byte
  tls_cert []byte
  Listen string
}

func (ext *GQLExt) Field(name string) interface{} {
  return ResolveFields(ext, name, map[string]func(*GQLExt)interface{}{
    "listen": func(ext *GQLExt) interface{} {
      return ext.Listen
    },
  })
}

func (ext *GQLExt) Process(ctx *Context, source NodeID, node *Node, signal Signal) {
  // Process ReadResultSignalType by forwarding it to the waiting resolver
  if signal.Type() == ReadResultSignalType {
    sig := signal.(ReadResultSignal)
    ext.resolver_reads_lock.RLock()
    resolver_id, exists := ext.resolver_reads[sig.UUID]
    ext.resolver_reads_lock.RUnlock()

    if exists == true {
      ext.resolver_reads_lock.Lock()
      delete(ext.resolver_reads, sig.UUID)
      ext.resolver_reads_lock.Unlock()

      ext.resolver_chans_lock.RLock()
      resolver_chan, exists := ext.resolver_chans[resolver_id]
      ext.resolver_chans_lock.RUnlock()

      if exists == true {
        select {
        case resolver_chan <- &sig:
          ctx.Log.Logf("gql", "Forwarded to resolver %s, %+v", resolver_id, sig)
        default:
          ctx.Log.Logf("gql", "Resolver %s channel overflow %+v", resolver_id, sig)
          ext.resolver_chans_lock.Lock()
          delete(ext.resolver_chans, resolver_id)
          ext.resolver_chans_lock.Unlock()
        }
      } else {
        ctx.Log.Logf("gql", "Received message for waiting resolver %s which doesn't exist - %+v", resolver_id, sig)
      }
    } else {
      ctx.Log.Logf("gql", "Received read result that wasn't expected - %+v", sig)
    }
  } else if signal.Type() == GQLStateSignalType {
    sig := signal.(StateSignal)
    switch sig.State {
    case "start_server":
      err := ext.StartGQLServer(ctx, node)
      if err == nil {
        ctx.Send(node.ID, source, StateSignal{NewDirectSignal(GQLStateSignalType), "server_started"})
      }
    case "stop_server":
      err := ext.StopGQLServer()
      if err == nil {
        ctx.Send(node.ID, source, StateSignal{NewDirectSignal(GQLStateSignalType), "server_stopped"})
      }
    default:
      ctx.Log.Logf("gql", "unknown gql state %s", sig.State)
    }
  }
}

func (ext *GQLExt) Type() ExtType {
  return GQLExtType
}

type GQLExtJSON struct {
  Listen string `json:"listen"`
  TLSKey []byte `json:"ssl_key"`
  TLSCert []byte `json:"ssl_cert"`
}

func (ext *GQLExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(&GQLExtJSON{
    Listen: ext.Listen,
    TLSKey: ext.tls_key,
    TLSCert: ext.tls_cert,
  }, "", "  ")
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

func LoadGQLExt(ctx *Context, data []byte) (Extension, error) {
  var j GQLExtJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  return NewGQLExt(ctx, j.Listen, j.TLSCert, j.TLSKey)
}

func NewGQLExt(ctx *Context, listen string, tls_cert []byte, tls_key []byte) (*GQLExt, error) {
  if tls_cert == nil || tls_key == nil {
    ssl_key, err := ecdsa.GenerateKey(ctx.ECDSA, rand.Reader)
    if err != nil {
      return nil, err
    }

    ssl_key_bytes, err := x509.MarshalECPrivateKey(ssl_key)
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

    ssl_cert, err := x509.CreateCertificate(rand.Reader, &template, &template, &ssl_key.PublicKey, ssl_key)
    if err != nil {
      return nil, err
    }

    ssl_cert_pem := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ssl_cert})

    tls_cert = ssl_cert_pem
    tls_key = ssl_key_pem
  }
  return &GQLExt{
    Listen: listen,
    resolver_reads: map[uuid.UUID]uuid.UUID{},
    resolver_chans: map[uuid.UUID]chan *ReadResultSignal{},
    tls_cert: tls_cert,
    tls_key: tls_key,
  }, nil
}

func (ext *GQLExt) StartGQLServer(ctx *Context, node *Node) error {
  if ext.tcp_listener != nil || ext.http_server != nil {
    return fmt.Errorf("listener or server is still running, stop them first")
  }
  mux := http.NewServeMux()
  mux.HandleFunc("/gql", GQLHandler(ctx, node, ext))
  mux.HandleFunc("/gqlws", GQLWSHandler(ctx, node, ext))

  // Server a graphiql interface(TODO make configurable whether to start this)
  mux.HandleFunc("/graphiql", GraphiQLHandler())

  // Server the ./site directory to /site (TODO make configurable with better defaults)
  fs := http.FileServer(http.Dir("./site"))
  mux.Handle("/site/", http.StripPrefix("/site", fs))

  http_server := &http.Server{
    Addr: ext.Listen,
    Handler: mux,
  }

  l, err := net.Listen("tcp", http_server.Addr)
  if err != nil {
    return fmt.Errorf("Failed to start listener for server on %s", http_server.Addr)
  }

  cert, err := tls.X509KeyPair(ext.tls_cert, ext.tls_key)
  if err != nil {
    return err
  }

  config := tls.Config{
    Certificates: []tls.Certificate{cert},
    NextProtos: []string{"http/1.1"},
  }

  listener := tls.NewListener(l, &config)

  ext.http_done.Add(1)
  go func(qql_ext *GQLExt) {
    defer ext.http_done.Done()

    err := http_server.Serve(listener)
    if err != http.ErrServerClosed {
        panic(fmt.Sprintf("Failed to start gql server: %s", err))
    }
  }(ext)


  ext.tcp_listener = listener
  ext.http_server = http_server
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
  return nil
}
