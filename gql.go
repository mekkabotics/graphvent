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
  "crypto/ecdsa"
  "crypto/elliptic"
  "crypto/ecdh"
  "crypto/ed25519"
  "crypto/rand"
  "crypto/x509"
  "crypto/tls"
  "crypto/x509/pkix"
  "math/big"
  "encoding/pem"
  "github.com/google/uuid"
)

func NodeInterfaceDefaultIsType(required_extensions []ExtType) func(graphql.IsTypeOfParams) bool {
  return func(p graphql.IsTypeOfParams) bool {
    ctx, ok := p.Context.Value("resolve").(*ResolveContext)
    if ok == false {
      return false
    }
    node, ok := p.Value.(NodeResult)
    if ok == false {
      return false
    }

    node_type_def, exists := ctx.Context.Nodes[node.NodeType]
    if exists == false {
      return false
    } else {
      for _, ext := range(required_extensions) {
        found := false
        for _, e := range(node_type_def.Extensions) {
          if e == ext {
            found = true
            break
          }
        }
        if found == false {
          return false
        }
      }
    }

    return true
  }
}

func NodeInterfaceResolveType(required_extensions []ExtType, default_type **graphql.Object)func(graphql.ResolveTypeParams) *graphql.Object {
  return func(p graphql.ResolveTypeParams) *graphql.Object {
    ctx, ok := p.Context.Value("resolve").(*ResolveContext)
    if ok == false {
      return nil
    }

    node, ok := p.Value.(NodeResult)
    if ok == false {
      return nil
    }

    gql_type, exists := ctx.GQLContext.NodeTypes[node.NodeType]
    ctx.Context.Log.Logf("gql", "GQL_INTERFACE_RESOLVE_TYPE(%+v): %+v - %t - %+v - %+v", node, gql_type, exists, required_extensions, *default_type)
    if exists == false {
      node_type_def, exists := ctx.Context.Nodes[node.NodeType]
      if exists == false {
        return nil
      } else {
        for _, ext := range(required_extensions) {
          found := false
          for _, e := range(node_type_def.Extensions) {
            if e == ext {
              found = true
              break
            }
          }
          if found == false {
            return nil
          }
        }
      }
      return *default_type
    }

    return gql_type
  }
}

func PrepResolve(p graphql.ResolveParams) (*ResolveContext, error) {
  resolve_context, ok := p.Context.Value("resolve").(*ResolveContext)
  if ok == false {
    return nil, fmt.Errorf("Bad resolve in params context")
  }

  return resolve_context, nil
}

// TODO: Make composabe by checkinf if K is a slice, then recursing in the same way that ExtractList does
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

func ExtractID(p graphql.ResolveParams, name string) (NodeID, error) {
  id_str, err := ExtractParam[string](p, name)
  if err != nil {
    return ZeroID, err
  }

  id, err := ParseID(id_str)
  if err != nil {
    return ZeroID, err
  }

  return id, nil
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

// Context passed to each resolve execution
type ResolveContext struct {
  // Channels for the gql extension to route data to this context
  Chans map[uuid.UUID]chan Signal

  // Graph Context this resolver is running under
  Context *Context

  // GQL Extension context this resolver is running under
  GQLContext *GQLExtContext

  // Pointer to the node that's currently processing this request
  Server *Node

  // The state data for the node processing this request
  Ext *GQLExt

  // ID of the user that made this request
  User NodeID

  // Cache of resolved nodes
  NodeCache map[NodeID]NodeResult

  // Key for the user that made this request, to sign resolver requests
  // TODO: figure out some way to use a generated key so that the server can't impersonate the user afterwards
  Key ed25519.PrivateKey
}

func NewResolveContext(ctx *Context, server *Node, gql_ext *GQLExt, r *http.Request) (*ResolveContext, error) {
  id_b64, key_b64, ok := r.BasicAuth()
  if ok == false {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: no auth header included in request header")
  }

  id_bytes, err := base64.StdEncoding.DecodeString(id_b64)
  if err != nil {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: failed to parse ID bytes from auth username: %+v", id_b64)
  }

  auth_uuid, err := uuid.FromBytes([]byte(id_bytes))
  if err != nil {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: failed to parse ID from id_bytes %+v", id_bytes)
  }
  auth_id := NodeID(auth_uuid)

  key_bytes, err := base64.StdEncoding.DecodeString(key_b64)
  if err != nil {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: failed to parse key bytes from auth password: %+v", key_b64)
  }

  key_raw, err := x509.ParsePKCS8PrivateKey([]byte(key_bytes))
  if err != nil {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: failed to parse ecdsa key from auth password: %s", key_bytes)
  }

  var key ed25519.PrivateKey
  switch k := key_raw.(type) {
  case ed25519.PrivateKey:
    key = k
  default:
    return nil, fmt.Errorf("GQL_REQUEST_ERR: wrong type for key: %s", reflect.TypeOf(key_raw))
  }

  key_id := KeyID(key.Public().(ed25519.PublicKey))
  if auth_id != key_id {
    return nil, fmt.Errorf("GQL_REQUEST_ERR: key_id(%s) != auth_id(%s)", auth_id, key_id)
  }

  return &ResolveContext{
    Ext: gql_ext,
    Chans: map[uuid.UUID]chan Signal{},
    Context: ctx,
    GQLContext: ctx.Extensions[GQLExtType].Data.(*GQLExtContext),
    NodeCache: map[NodeID]NodeResult{},
    Server: server,
    User: key_id,
    Key: key,
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

    gql_context := ctx.Extensions[GQLExtType].Data.(*GQLExtContext)

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
          gql_context := ctx.Extensions[GQLExtType].Data.(*GQLExtContext)
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

type Field struct {
  Ext ExtType
  Name string
  Field *graphql.Field
}

// GQL Specific Context information
type GQLExtContext struct {
  // Generated GQL schema
  Schema graphql.Schema

  // Custom graphql types, mapped to NodeTypes
  NodeTypes map[NodeType]*graphql.Object
  Interfaces map[string]*Interface
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

func (ctx *GQLExtContext) RegisterField(gql_type graphql.Type, gql_name string, ext_type ExtType, gv_tag string, resolve_fn func(graphql.ResolveParams, *ResolveContext, reflect.Value)(interface{}, error)) error {
  if ctx == nil {
    return fmt.Errorf("ctx is nil")
  }

  if resolve_fn == nil {
    return fmt.Errorf("resolve_fn cannot be nil")
  }

  _, exists := ctx.Fields[gql_name]
  if exists == true {
    return fmt.Errorf("%s is already a field in the context, cannot add again", gql_name)
  }

  // Resolver has p.Source.(NodeResult) = read result of current node
  resolver := func(p graphql.ResolveParams)(interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    node, ok := p.Source.(NodeResult)
    if ok == false {
      return nil, fmt.Errorf("p.Value is not NodeResult")
    }

    ext, ext_exists := node.Data[ext_type]
    if ext_exists == false {
      return nil, fmt.Errorf("%+v is not in the extensions of the result", ext_type)
    }

    val_ser, field_exists := ext[gv_tag]
    if field_exists == false {
      return nil, fmt.Errorf("%s is not in the fields of %+v in the result", gv_tag, ext_type)
    }

    if val_ser.TypeStack[0] == ErrorType {
      return nil, fmt.Errorf(string(val_ser.Data))
    }

    field_type, field_value, _, err := DeserializeValue(ctx.Context, val_ser)
    if err != nil {
      return nil, err
    }

    if field_value == nil {
      return nil, fmt.Errorf("%s returned a nil value of %+v type", gv_tag, field_type)
    }

    ctx.Context.Log.Logf("gql", "Resolving %+v", field_value)

    return resolve_fn(p, ctx, *field_value)
  }

  ctx.Fields[gql_name] = Field{ext_type, gv_tag, &graphql.Field{
    Type: gql_type,
    Resolve: resolver,
  }}
  return nil
}

func GQLInterfaces(ctx *GQLExtContext, interface_names []string) ([]*graphql.Interface, error) {
  ret := make([]*graphql.Interface, len(interface_names))
  for i, in := range(interface_names) {
    ctx_interface, exists := ctx.Interfaces[in]
    if exists == false {
      return nil, fmt.Errorf("%s is not in GQLExtContext.Interfaces", in)
    }
    ret[i] = ctx_interface.Interface
  }

  return ret, nil
}

func GQLFields(ctx *GQLExtContext, field_names []string) (graphql.Fields, []ExtType, error) {
  fields := graphql.Fields{
    "ID": &graphql.Field{
      Type: graphql.String,
      Resolve: ResolveNodeID,
    },
    "TypeHash": &graphql.Field{
      Type: graphql.String,
      Resolve: ResolveNodeTypeHash,
    },
  }

  exts := map[ExtType]ExtType{}
  ext_list := []ExtType{}
  for _, name := range(field_names) {
    field, exists := ctx.Fields[name]
    if exists == false {
      return nil, nil, fmt.Errorf("%s is not in GQLExtContext.Fields", name)
    }
    fields[name] = field.Field
    _, exists = exts[field.Ext]
    if exists == false {
      ext_list = append(ext_list, field.Ext)
      exts[field.Ext] = field.Ext
    }
  }

  return fields, ext_list, nil
}

type NodeResult struct {
  NodeID NodeID
  NodeType NodeType
  Data map[ExtType]map[string]SerializedValue
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

func (ctx *GQLExtContext) RegisterInterface(name string, default_name string, interfaces []string, fields []string, self_fields map[string]SelfField, list_fields map[string]ListField) error {
  if interfaces == nil {
    return fmt.Errorf("interfaces is nil")
  }

  if fields == nil {
    return fmt.Errorf("fields is nil")
  }

  _, exists := ctx.Interfaces[name]
  if exists == true {
    return fmt.Errorf("%s is already an interface in ctx", name)
  }

  node_interfaces, err := GQLInterfaces(ctx, interfaces)
  if err != nil {
    return err
  }

  node_fields, node_exts, err := GQLFields(ctx, fields)
  if err != nil {
    return err
  }

  ctx_interface := Interface{}

  ctx_interface.Interface = graphql.NewInterface(graphql.InterfaceConfig{
    Name: name,
    ResolveType: NodeInterfaceResolveType(node_exts, &ctx_interface.Default),
    Fields: node_fields,
  })
  ctx_interface.List = graphql.NewList(ctx_interface.Interface)

  for field_name, field := range(self_fields) {
    self_field := field
    err := ctx.RegisterField(ctx_interface.Interface, field_name, self_field.Extension, self_field.ACLName,
    func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value)(interface{}, error) {
      id, err := self_field.ResolveFn(p, ctx, value)
      if err != nil {
        return nil, err
      }

      if id != nil {
        nodes, err := ResolveNodes(ctx, p, []NodeID{*id})
        if err != nil {
          return nil, err
        } else if len(nodes) != 1 {
          return nil, fmt.Errorf("wrong length of nodes returned")
        }
        return nodes[0], nil
      } else {
        return nil, nil
      }
    })
    if err != nil {
      return err
    }

    ctx_interface.Interface.AddFieldConfig(field_name, ctx.Fields[field_name].Field)
    node_fields[field_name] = ctx.Fields[field_name].Field
  }

  for field_name, field := range(list_fields) {
    list_field := field
    resolve_fn := func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value)(interface{}, error) {
      var zero NodeID
      ids, err := list_field.ResolveFn(p, ctx, value)
      if err != nil {
        return zero, err
      }

      nodes, err := ResolveNodes(ctx, p, ids)
      if err != nil {
        return nil, err
      } else if len(nodes) != len(ids) {
        return nil, fmt.Errorf("wrong length of nodes returned")
      }
      return nodes, nil
    }

    err := ctx.RegisterField(ctx_interface.List, field_name, list_field.Extension, list_field.ACLName, resolve_fn)
    if err != nil {
      return err
    }
    ctx_interface.Interface.AddFieldConfig(field_name, ctx.Fields[field_name].Field)
    node_fields[field_name] = ctx.Fields[field_name].Field
  }

  ctx_interface.Default = graphql.NewObject(graphql.ObjectConfig{
    Name: default_name,
    Interfaces: append(node_interfaces, ctx_interface.Interface),
    IsTypeOf: NodeInterfaceDefaultIsType(node_exts),
    Fields: node_fields,
  })

  ctx.Interfaces[name] = &ctx_interface
  ctx.Types = append(ctx.Types, ctx_interface.Default)

  return nil
}

func (ctx *GQLExtContext) RegisterNodeType(node_type NodeType, name string, interface_names []string, field_names []string) error {
  if field_names == nil {
    return fmt.Errorf("fields is nil")
  }

  _, exists := ctx.NodeTypes[node_type]
  if exists == true {
    return fmt.Errorf("%+v already in GQLExtContext.NodeTypes", node_type)
  }

  node_interfaces, err := GQLInterfaces(ctx, interface_names)
  if err != nil {
    return err
  }

  gql_fields, _, err := GQLFields(ctx, field_names)
  if err != nil {
    return err
  }

  gql_type := graphql.NewObject(graphql.ObjectConfig{
    Name: name,
    Interfaces: node_interfaces,
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      node, ok := p.Value.(NodeResult)
      if ok == false {
        return false
      }

      return node.NodeType == node_type
    },
    Fields: gql_fields,
  })

  ctx.NodeTypes[node_type] = gql_type
  ctx.Types = append(ctx.Types, gql_type)

  return nil
}

func NewGQLExtContext() *GQLExtContext {
  query := graphql.NewObject(graphql.ObjectConfig{
    Name: "Query",
    Fields: graphql.Fields{},
  })

  mutation := graphql.NewObject(graphql.ObjectConfig{
    Name: "Mutation",
    Fields: graphql.Fields{},
  })

  subscription := graphql.NewObject(graphql.ObjectConfig{
    Name: "Subscription",
    Fields: graphql.Fields{},
  })

  context := GQLExtContext{
    Schema: graphql.Schema{},
    Types: []graphql.Type{},
    Query: query,
    Mutation: mutation,
    Subscription: subscription,
    NodeTypes: map[NodeType]*graphql.Object{},
    Interfaces: map[string]*Interface{},
    Fields: map[string]Field{},
  }

  var err error
  err = context.RegisterInterface("Node", "DefaultNode", []string{}, []string{}, map[string]SelfField{}, map[string]ListField{})
  if err != nil {
    panic(err)
  }

  err = context.RegisterField(context.Interfaces["Node"].List, "Members", GroupExtType, "members",
  func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value)(interface{}, error) {
    node_map, ok := value.Interface().(map[NodeID]string)
    if ok == false {
      return nil, fmt.Errorf("value is %+v, not map[NodeID]string", value.Type())
    }
    node_list := []NodeID{}
    i := 0
    for id := range(node_map) {
      node_list = append(node_list, id)
      i += 1
    }

    nodes, err := ResolveNodes(ctx, p, node_list)
    if err != nil {
      return nil, err
    }

    return nodes, nil
  })
  if err != nil {
    panic(err)
  }

  err = context.RegisterInterface("Group", "DefaultGroup", []string{"Node"}, []string{"Members"}, map[string]SelfField{}, map[string]ListField{})
  if err != nil {
    panic(err)
  }

  err = context.RegisterInterface("Lockable", "DefaultLockable", []string{"Node"}, []string{}, map[string]SelfField{
    "Owner": {
      "owner",
      LockableExtType,
      func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value) (*NodeID, error) {
        id, ok := value.Interface().(*NodeID)
        if ok == false {
          return nil, fmt.Errorf("can't parse %+v as *NodeID", value.Type())
        }

        return id, nil
      },
    },
  }, map[string]ListField{
    "Requirements": {
      "requirements",
      LockableExtType,
      func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value) ([]NodeID, error) {
        id_strs, ok := value.Interface().(map[NodeID]ReqState)
        if ok == false {
          return nil, fmt.Errorf("can't parse requirements %+v as map[NodeID]ReqState", value.Type())
        }

        ids := []NodeID{}
        for id := range(id_strs) {
          ids = append(ids, id)
        }
        return ids, nil
      },
    },
  })

  if err != nil {
    panic(err)
  }

  err = context.RegisterField(graphql.String, "Listen", GQLExtType, "listen", func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value) (interface{}, error) {
    return value.String(), nil
  })
  if err != nil {
    panic(err)
  }

  err = context.RegisterNodeType(GQLNodeType, "GQLServer", []string{"Node", "Lockable", "Group"}, []string{"Listen", "Owner", "Requirements", "Members"})
  if err != nil {
    panic(err)
  }

  context.Mutation.AddFieldConfig("stop", &graphql.Field{
    Type: graphql.String,
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      return nil, fmt.Errorf("NOT_IMPLEMENTED")
    },
  })

  context.Subscription.AddFieldConfig("Self", &graphql.Field{
    Type: context.NodeTypes[GQLNodeType],
    Subscribe: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }
      c := make(chan interface{}, 1)
      nodes, err := ResolveNodes(ctx, p, []NodeID{ctx.Server.ID})
      if err != nil {
        return nil, err
      } else if len(nodes) != 1 {
        return nil, fmt.Errorf("wrong length of nodes returned")
      }

      ctx.Context.Log.Logf("gql", "NODES: %+v", nodes[0])
      c <- nodes[0]

      return c, nil
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      switch source := p.Source.(type) {
      case NodeResult:
      case StatusSignal:
        delete(ctx.NodeCache, source.Source)
        ctx.Context.Log.Logf("gql_subscribe", "Deleting %+v from NodeCache", source.Source)
      default:
        return nil, fmt.Errorf("Don't know how to handle %+v", source)
      }

      return ctx.NodeCache[ctx.Server.ID], nil
    },
  })

  context.Query.AddFieldConfig("Self", &graphql.Field{
    Type: context.Interfaces["Node"].Interface,
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      nodes, err := ResolveNodes(ctx, p, []NodeID{ctx.Server.ID})
      if err != nil {
        return nil, err
      } else if len(nodes) != 1 {
        return nil, fmt.Errorf("wrong length of resolved nodes returned")
      }

      return nodes[0], nil
    },
  })

  context.Query.AddFieldConfig("Node", &graphql.Field{
    Type: context.Interfaces["Node"].Interface,
    Args: graphql.FieldConfigArgument{
      "id": &graphql.ArgumentConfig{
        Type: graphql.String,
      },
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      id, err := ExtractID(p, "id")
      if err != nil {
        return nil, err
      }

      nodes, err := ResolveNodes(ctx, p, []NodeID{id})
      if err != nil {
        return nil, err
      } else if len(nodes) != 1 {
        return nil, fmt.Errorf("wrong length of resolved nodes returned")
      }

      return nodes[0], nil
    },
  })

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

  // map of read request IDs to response channels
  resolver_response map[uuid.UUID]chan Signal
  resolver_response_lock sync.RWMutex

  TLSKey []byte `gv:"tls_key"`
  TLSCert []byte `gv:"tls_cert"`
  Listen string `gv:"listen"`
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

func (ext *GQLExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  // Process ReadResultSignalType by forwarding it to the waiting resolver
  switch sig := signal.(type) {
  case *ErrorSignal:
    // TODO: Forward to resolver if waiting for it
    response_chan := ext.FreeResponseChannel(sig.Header().ReqID)
    if response_chan != nil {
      select {
      case response_chan <- sig:
        ctx.Log.Logf("gql", "Forwarded error to resolver, %+v", sig)
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
  case *StartSignal:
    ctx.Log.Logf("gql", "starting gql server %s", node.ID)
    err := ext.StartGQLServer(ctx, node)
    if err == nil {
      node.QueueSignal(time.Now(), NewStatusSignal(node.ID, "server_started"))
    } else {
      ctx.Log.Logf("gql", "GQL_RESTART_ERROR: %s", err)
    }
  }
  return nil
}

func (ext *GQLExt) Type() ExtType {
  return GQLExtType
}

func (ext *GQLExt) MarshalBinary() ([]byte, error) {
  return json.Marshal(ext)
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

func (ext *GQLExt) Deserialize(ctx *Context, data []byte) error {
  ext.resolver_response = map[uuid.UUID]chan Signal{}
  return json.Unmarshal(data, &ext)
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
    TLSCert: tls_cert,
    TLSKey: tls_key,
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

  cert, err := tls.X509KeyPair(ext.TLSCert, ext.TLSKey)
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
