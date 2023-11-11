package graphvent

import (
  "bytes"
  "context"
  "crypto"
  "crypto/aes"
  "crypto/cipher"
  "crypto/ecdh"
  "crypto/ecdsa"
  "crypto/ed25519"
  "crypto/elliptic"
  "crypto/rand"
  "crypto/x509"
  "encoding/base64"
  "encoding/json"
  "fmt"
  "io"
  "os"
  "net"
  "net/http"
  "reflect"
  "strings"
  "sync"
  "time"

  "filippo.io/edwards25519"
  "crypto/sha512"
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

  // GQL Extension context this resolver is running under
  GQLContext *GQLExtContext

  // Pointer to the node that's currently processing this request
  Server *Node

  // The state data for the node processing this request
  Ext *GQLExt

  // Cache of resolved nodes
  NodeCache map[NodeID]NodeResult

  // Authorization from the user that started this request
  Authorization *ClientAuthorization
}

func AuthB64(client_key ed25519.PrivateKey, server_pubkey ed25519.PublicKey) (string, error) {
  token_start := time.Now()
  token_start_bytes, err := token_start.MarshalBinary()
  if err != nil {
    return "", err
  }

  session_key_public, session_key_private, err := ed25519.GenerateKey(rand.Reader)
  if err != nil {
    return "", err
  }

  session_h := sha512.Sum512(session_key_private.Seed())
  ecdh_client, err := ECDH.NewPrivateKey(session_h[:32])
  if err != nil {
    return "", err
  }

  server_point, err := (&edwards25519.Point{}).SetBytes(server_pubkey)
  if err != nil {
    return "", err
  }

  ecdh_server, err := ECDH.NewPublicKey(server_point.BytesMontgomery())
  if err != nil {
    return "", err
  }


  secret, err := ecdh_client.ECDH(ecdh_server)
  if err != nil {
    return "", err
  }

  if len(secret) != 32 {
    return "", fmt.Errorf("ECDH secret not 32 bytes(for AES-256): %d bytes long", len(secret))
  }

  block, err := aes.NewCipher(secret)
  if err != nil {
    return "", err
  }

  iv := make([]byte, block.BlockSize())
  iv_len, err := rand.Reader.Read(iv)
  if err != nil {
    return "", err
  } else if iv_len != block.BlockSize() {
    return "", fmt.Errorf("Not enough iv bytes read: %d", iv_len)
  }

  var key_encrypted bytes.Buffer
  stream := cipher.NewOFB(block, iv)
  writer := &cipher.StreamWriter{S: stream, W: &key_encrypted}

  bytes_written, err := writer.Write(session_key_private.Seed())
  if err != nil {
    return "", err
  } else if bytes_written != len(ecdh_client.Bytes()) {
    return "", fmt.Errorf("wrong number of bytes encrypted %d/%d", bytes_written, len(ecdh_client.Bytes()))
  }

  digest := append(session_key_public, token_start_bytes...)
  signature, err := client_key.Sign(rand.Reader, digest, crypto.Hash(0))
  if err != nil {
    return "", err
  }

  start_b64 := base64.StdEncoding.EncodeToString(token_start_bytes)
  iv_b64 := base64.StdEncoding.EncodeToString(iv)
  encrypted_b64 := base64.StdEncoding.EncodeToString(key_encrypted.Bytes())
  key_b64 := base64.StdEncoding.EncodeToString(session_key_public)
  sig_b64 := base64.StdEncoding.EncodeToString(signature)
  id_b64 := base64.StdEncoding.EncodeToString(client_key.Public().(ed25519.PublicKey))

  return base64.StdEncoding.EncodeToString([]byte(strings.Join([]string{id_b64, iv_b64, key_b64, encrypted_b64, start_b64, sig_b64}, ":"))), nil
}

func ParseAuthB64(auth_base64 string, server_id ed25519.PrivateKey) (*ClientAuthorization, error) {
  joined_b64, err := base64.StdEncoding.DecodeString(auth_base64)
  if err != nil {
    return nil, err
  }

  auth_parts := strings.Split(string(joined_b64), ":")
  if len(auth_parts) != 6 {
    return nil, fmt.Errorf("Wrong number of delimited elements %d", len(auth_parts))
  }

  id_bytes, err := base64.StdEncoding.DecodeString(auth_parts[0])
  if err != nil {
    return nil, err
  }

  iv, err := base64.StdEncoding.DecodeString(auth_parts[1])
  if err != nil {
    return nil, err
  }

  public_key, err := base64.StdEncoding.DecodeString(auth_parts[2])
  if err != nil {
    return nil, err
  }

  key_encrypted, err := base64.StdEncoding.DecodeString(auth_parts[3])
  if err != nil {
    return nil, err
  }

  start_bytes, err := base64.StdEncoding.DecodeString(auth_parts[4])
  if err != nil {
    return nil, err
  }

  signature, err := base64.StdEncoding.DecodeString(auth_parts[5])
  if err != nil {
    return nil, err
  }

  var start time.Time
  err = start.UnmarshalBinary(start_bytes)
  if err != nil {
    return nil, err
  }

  client_id := ed25519.PublicKey(id_bytes)
  if err != nil {
    return nil, err
  }

  client_point, err := (&edwards25519.Point{}).SetBytes(public_key)
  if err != nil {
    return nil, err
  }

  ecdh_client, err := ECDH.NewPublicKey(client_point.BytesMontgomery())
  if err != nil {
    return nil, err
  }

  h := sha512.Sum512(server_id.Seed())
  ecdh_server, err := ECDH.NewPrivateKey(h[:32])
  if err != nil {
    return nil, err
  }

  secret, err := ecdh_server.ECDH(ecdh_client)
  if err != nil {
    return nil, err
  } else if len(secret) != 32 {
    return nil, fmt.Errorf("Secret wrong length: %d/32", len(secret))
  }

  block, err := aes.NewCipher(secret)
  if err != nil {
    return nil, err
  }

  encrypted_reader := bytes.NewReader(key_encrypted)
  stream := cipher.NewOFB(block, iv)
  reader := cipher.StreamReader{S: stream, R: encrypted_reader}
  var decrypted_key bytes.Buffer
  _, err = io.Copy(&decrypted_key, reader)
  if err != nil {
    return nil, err
  }

  session_key := ed25519.NewKeyFromSeed(decrypted_key.Bytes())
  digest := append(session_key.Public().(ed25519.PublicKey), start_bytes...)
  if ed25519.Verify(client_id, digest, signature) == false {
    return nil, fmt.Errorf("Failed to verify digest/signature against client_id")
  }

  return &ClientAuthorization{
    AuthInfo: AuthInfo{
      Identity: client_id,
      Start: start,
      Signature: signature,
    },
    Key: session_key,
  }, nil
}

func ValidateAuthorization(auth Authorization, valid time.Duration) error {
  // Check that the time + valid < now
  // Check that Signature is public_key + start signed with client_id
  if auth.Start.Add(valid).Compare(time.Now()) != 1 {
    return fmt.Errorf("authorization expired")
  }

  time_bytes, err := auth.Start.MarshalBinary()
  if err != nil {
    return err
  }

  digest := append(auth.Key, time_bytes...)
  if ed25519.Verify(auth.Identity, digest, auth.Signature) != true {
    return fmt.Errorf("verification failed")
  }

  return nil
}

func NewResolveContext(ctx *Context, server *Node, gql_ext *GQLExt) (*ResolveContext, error) {
  return &ResolveContext{
    ID: uuid.New(),
    Ext: gql_ext,
    Chans: map[uuid.UUID]chan Signal{},
    Context: ctx,
    GQLContext: ctx.Extensions[GQLExtType].Data.(*GQLExtContext),
    NodeCache: map[NodeID]NodeResult{},
    Server: server,
    Authorization: nil,
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

    auth, err := ParseAuthB64(r.Header.Get("Authorization"), server.Key)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ID_PARSE_ERROR: %s", err)
      json.NewEncoder(w).Encode(GQLUnauthorized(""))
      return
    }

    resolve_context, err := NewResolveContext(ctx, server, gql_ext)
    if err != nil {
      ctx.Log.Logf("gql", "GQL_AUTH_ERR: %s", err)
      json.NewEncoder(w).Encode(GQLUnauthorized(""))
      return
    }

    resolve_context.Authorization = auth

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

          authorization, err := ParseAuthB64(connection_params.Payload.Token, server.Key)
          if err != nil {
            ctx.Log.Logf("gqlws", "WS_AUTH_PARSE_ERR: %s", err)
            break
          }

          resolve_context.Authorization = authorization

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

  TypeMap map[reflect.Type]GQLTypeInfo
  KindMap map[reflect.Kind]GQLTypeInfo
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
        continue
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
      return nil, fmt.Errorf("%+v is not in the extensions of the result: %+v", ext_type, node.Data)
    }

    val_ser, field_exists := ext[gv_tag]
    if field_exists == false {
      return nil, fmt.Errorf("%s is not in the fields of %+v in the result for %s - %+v", gv_tag, ext_type, gql_name, node)
    }

    if val_ser.TypeStack[0] == ErrorType {
      return nil, fmt.Errorf(string(val_ser.Data))
    }

    field_type, _, err := DeserializeType(ctx.Context, val_ser.TypeStack)
    if err != nil {
      return nil, err
    }

    field_value, _, err := DeserializeValue(ctx.Context, field_type, val_ser.Data)
    if err != nil {
      return nil, err
    }

    ctx.Context.Log.Logf("gql", "Resolving %+v", field_value)

    return resolve_fn(p, ctx, field_value)
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

  kind_map := map[reflect.Kind]GQLTypeInfo{
    reflect.String: {
      func(ctx *GQLExtContext, reflect_type reflect.Type)(graphql.Type, error) {
        return graphql.String, nil
      },
      func(ctx *GQLExtContext, value interface{})(reflect.Value, error) {
        return reflect.ValueOf(value), nil
      },
    },
    reflect.Bool: {
      func(ctx *GQLExtContext, reflect_type reflect.Type)(graphql.Type, error) {
        return graphql.Boolean, nil
      },
      func(ctx *GQLExtContext, value interface{})(reflect.Value, error) {
        return reflect.ValueOf(value), nil
      },
    },
  }
  type_map := map[reflect.Type]GQLTypeInfo{
    reflect.TypeOf([2]NodeID{}): {
      func(ctx *GQLExtContext, reflect_type reflect.Type)(graphql.Type, error) {
        return graphql.NewList(graphql.String), nil
      },
      func(ctx *GQLExtContext, value interface{})(reflect.Value, error) {
        l, ok := value.([]interface{})
        if ok == false {
          return reflect.Value{}, fmt.Errorf("not list: %s", reflect.TypeOf(value))
        } else if len(l) != 2 {
          return reflect.Value{}, fmt.Errorf("wrong length: %d/2", len(l))
        }

        id1_str, ok := l[0].(string)
        if ok == false {
          return reflect.Value{}, fmt.Errorf("not strg: %s", reflect.TypeOf(l[0]))
        }
        id1, err := ParseID(id1_str)
        if err != nil {
          return reflect.Value{}, err
        }
        id2_str, ok := l[1].(string)
        if ok == false {
          return reflect.Value{}, fmt.Errorf("not strg: %s", reflect.TypeOf(l[1]))
        }
        id2, err := ParseID(id2_str)
        if err != nil {
          return reflect.Value{}, err
        }
        return_value := reflect.New(reflect.TypeOf([2]NodeID{})).Elem()
        return_value.Index(0).Set(reflect.ValueOf(id1))
        return_value.Index(1).Set(reflect.ValueOf(id2))

        return return_value, nil
      },
    },
    reflect.TypeOf(time.Time{}): {
      func(ctx *GQLExtContext, reflect_type reflect.Type) (graphql.Type, error) {
        return graphql.DateTime, nil
      },
      func(ctx *GQLExtContext, value interface{}) (reflect.Value, error) {
        return reflect.ValueOf(value), nil
      },
    },
    reflect.TypeOf(&NodeID{}): {
      func(ctx *GQLExtContext, reflect_type reflect.Type) (graphql.Type, error) {
        return graphql.String, nil
      },
      func(ctx *GQLExtContext, value interface{}) (reflect.Value, error) {
        str, ok := value.(string)
        if ok == false {
          return reflect.Value{}, fmt.Errorf("value is not string")
        }
  
        if str == "" {
          return reflect.New(reflect.TypeOf(&NodeID{})).Elem(), nil
        }
  
        id_parsed, err := ParseID(str)
        if err != nil {
          return reflect.Value{}, err
        }
  
        return reflect.ValueOf(&id_parsed), nil
      },
    },
    reflect.TypeOf(NodeID{}): {
      func(ctx *GQLExtContext, reflect_type reflect.Type)(graphql.Type, error) {
        return graphql.String, nil
      },
      func(ctx *GQLExtContext, value interface{})(reflect.Value, error) {
        str, ok := value.(string)
        if ok == false {
          return reflect.Value{}, fmt.Errorf("value is not string")
        }
  
        id_parsed, err := ParseID(str)
        if err != nil {
          return reflect.Value{}, err
        }
  
        return reflect.ValueOf(id_parsed), nil
      },
    },
  }

  context := GQLExtContext{
    Schema: graphql.Schema{},
    Types: []graphql.Type{},
    Query: query,
    Mutation: mutation,
    Subscription: subscription,
    NodeTypes: map[NodeType]*graphql.Object{},
    Interfaces: map[string]*Interface{},
    Fields: map[string]Field{},
    KindMap: kind_map,
    TypeMap: type_map,
  }

  var err error
  err = context.RegisterInterface("Node", "DefaultNode", []string{}, []string{}, map[string]SelfField{}, map[string]ListField{})
  if err != nil {
    panic(err)
  }

  err = context.RegisterField(graphql.String, "EventName", EventExtType, "name", func(p graphql.ResolveParams, ctx *ResolveContext, val reflect.Value)(interface{}, error) {
    name := val.String()
    return name, nil
  })

  err = context.RegisterField(graphql.String, "EventState", EventExtType, "state", func(p graphql.ResolveParams, ctx *ResolveContext, val reflect.Value)(interface{}, error) {
    state := val.String()
    return state, nil
  })

  err = context.RegisterInterface("Event", "EventNode", []string{"Node"}, []string{"EventName", "EventState"}, map[string]SelfField{}, map[string]ListField{})
  if err != nil {
    panic(err)
  }

  sub_group_type := graphql.NewObject(graphql.ObjectConfig{
    Name: "SubGroup",
    Interfaces: nil,
    Fields: graphql.Fields{
      "Name": &graphql.Field{
        Type: graphql.String,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          val, ok := p.Source.(SubGroupGQL)
          if ok == false {
            return nil, fmt.Errorf("WRONG_TYPE_RETURNED")
          }
          return val.Name, nil
        },
      },
      "Members": &graphql.Field{
        Type: context.Interfaces["Node"].List,
        Resolve: func(p graphql.ResolveParams) (interface{}, error) {
          ctx, err := PrepResolve(p)
          if err != nil {
            return nil, err
          }

          val, ok := p.Source.(SubGroupGQL)
          if ok == false {
            return nil, fmt.Errorf("WRONG_TYPE_RETURNED")
          }

          nodes, err := ResolveNodes(ctx, p, val.Members)
          if err != nil {
            return nil, err
          }

          return nodes, nil
        },
      },
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      return reflect.TypeOf(p.Value) == reflect.TypeOf(SubGroupGQL{})
    },
    Description: "SubGroup within Group",
  })
  context.Types = append(context.Types, sub_group_type)

  err = context.RegisterField(sub_group_type, "SubGroups", GroupExtType, "sub_groups",
  func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value)(interface{}, error) {
    node_map, ok := value.Interface().(map[string]SubGroup)
    if ok == false {
      return nil, fmt.Errorf("value is %+v, not map[string]SubGroup", value.Type())
    }

    sub_groups := []SubGroupGQL{}
    for name, sub_group := range(node_map) {
      sub_groups = append(sub_groups, SubGroupGQL{
        name,
        sub_group.Members,
      })
    }

    return sub_groups, nil
  })
  if err != nil {
    panic(err)
  }

  err = context.RegisterInterface("Group", "DefaultGroup", []string{"Node"}, []string{"SubGroups"}, map[string]SelfField{}, map[string]ListField{})
  if err != nil {
    panic(err)
  }

  err = context.RegisterField(graphql.String, "LockableState", LockableExtType, "state",
  func(p graphql.ResolveParams, ctx *ResolveContext, value reflect.Value)(interface{}, error) {
    state, ok := value.Interface().(ReqState)
    if ok == false {
      return nil, fmt.Errorf("value is %+v, not ReqState", value.Type())
    }

    return ReqStateStrings[state], nil
  })
  if err != nil {
    panic(err)
  }

  err = context.RegisterInterface("Lockable", "DefaultLockable", []string{"Node"}, []string{"LockableState"}, map[string]SelfField{
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

  err = context.RegisterNodeType(GQLNodeType, "GQLServer", []string{"Node", "Lockable", "Group"}, []string{"LockableState", "Listen", "Owner", "Requirements", "SubGroups"})
  if err != nil {
    panic(err)
  }

  err = context.AddSignalMutation("stop", "node_id", reflect.TypeOf(StopSignal{}))
  if err != nil {
    panic(err)
  }

  err = context.AddSignalMutation("addMember", "group_id", reflect.TypeOf(AddMemberSignal{}))
  if err != nil {
    panic(err)
  }

  err = context.AddSignalMutation("removeMember", "group_id", reflect.TypeOf(RemoveMemberSignal{}))
  if err != nil {
    panic(err)
  }

  err = context.AddSignalMutation("eventControl", "event_id", reflect.TypeOf(EventControlSignal{}))
  if err != nil {
    panic(err)
  }

  context.Subscription.AddFieldConfig("Self", &graphql.Field{
    Type: context.Interfaces["Node"].Interface,
    Subscribe: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      c, err := ctx.Ext.AddSubscription(ctx.ID)
      if err != nil {
        return nil, err
      }

      nodes, err := ResolveNodes(ctx, p, []NodeID{ctx.Server.ID})
      if err != nil {
        return nil, err
      } else if len(nodes) != 1 {
        return nil, fmt.Errorf("wrong length of nodes returned")
      }

      c <- nodes[0]

      return c, nil
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }
      ctx.Context.Log.Logf("gql_subscribe", "SUBSCRIBE_RESOLVE: %+v", p.Source)

      switch source := p.Source.(type) {
      case NodeResult:
      case *StatusSignal:
        delete(ctx.NodeCache, source.Source)
        ctx.Context.Log.Logf("gql_subscribe", "Deleting %+v from NodeCache", source.Source)
        if source.Source == ctx.Server.ID {
          nodes, err := ResolveNodes(ctx, p, []NodeID{ctx.Server.ID})
          if err != nil {
            return nil, err
          } else if len(nodes) != 1 {
            return nil, fmt.Errorf("wrong length of nodes returned")
          }
          ctx.NodeCache[ctx.Server.ID] = nodes[0]
        }
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
  Listen string `gv:"listen"`
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

  c := make(chan interface{}, 1)

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

func (ext *GQLExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  // Process ReadResultSignalType by forwarding it to the waiting resolver
  var changes = Changes{}

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

  case *StopSignal:
    ctx.Log.Logf("gql", "stopping gql server %s", node.ID)
    err := ext.StopGQLServer()
    if err != nil {
      ctx.Log.Logf("gql", "GQL_STOP_ERROR: %s", err)
    }

  case *StartSignal:
    ctx.Log.Logf("gql", "starting gql server %s", node.ID)
    err := ext.StartGQLServer(ctx, node)
    if err == nil {
      ctx.Log.Logf("gql", "started gql server on %s", ext.Listen)
      changes.Add(GQLExtType, "state")
    } else {
      ctx.Log.Logf("gql", "GQL_RESTART_ERROR: %s", err)
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

  return nil, changes
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
