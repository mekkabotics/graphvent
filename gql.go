package graphvent

import (
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

type GQLWSPayload struct {
  OperationName string `json:"operationName,omitempty"`
  Query string `json:"query,omitempty"`
  Variables map[string]interface{} `json:"variables,omitempty"`
  Extensions map[string]interface{} `json:"extensions,omitempty"`
  Data string `json:"data,omitempty"`
}

type GQLWSMsg struct {
  ID      string `json:"id,omitempty"`
  Type    string `json:"type"`
  Payload GQLWSPayload `json:"payload,omitempty"`
}

func enableCORS(w *http.ResponseWriter) {
 (*w).Header().Set("Access-Control-Allow-Origin", "*")
 (*w).Header().Set("Access-Control-Allow-Credentials", "true")
 (*w).Header().Set("Access-Control-Allow-Headers", "*")
 (*w).Header().Set("Access-Control-Allow-Methods", "*")
}

func GQLHandler(ctx * GraphContext, schema graphql.Schema, gql_ctx context.Context) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gql", "GQL REQUEST: %s", r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }
    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")

    str, err := io.ReadAll(r.Body)
    if err != nil {
      ctx.Log.Logf("gql", "failed to read request body: %s", err)
      return
    }
    query := GQLWSPayload{}
    json.Unmarshal(str, &query)

    params := graphql.Params{
      Schema: schema,
      Context: gql_ctx,
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

func GQLWSDo(ctx * GraphContext, p graphql.Params) chan *graphql.Result {
  operation := getOperationTypeOfReq(p)
  ctx.Log.Logf("gqlws", "GQLWSDO_OPERATION: %s %+v", operation, p.RequestString)

  if operation == ast.OperationTypeSubscription {
    return graphql.Subscribe(p)
  }

  res := graphql.Do(p)
  return sendOneResultAndClose(res)
}

func GQLWSHandler(ctx * GraphContext, schema graphql.Schema, gql_ctx context.Context) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    ctx.Log.Logf("gqlws_new", "HANDLING %s",r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }
    ctx.Log.Logm("gql", header_map, "REQUEST_HEADERS")
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
        // TODO: Make this a select between reading client data and getting updates from the event to push to clients"
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
            Schema: schema,
            Context: gql_ctx,
            RequestString: msg.Payload.Query,
          }
          if msg.Payload.OperationName != "" {
            params.OperationName = msg.Payload.OperationName
          }
          if len(msg.Payload.Variables) > 0 {
            params.VariableValues = msg.Payload.Variables
          }

          res_chan := GQLWSDo(ctx, params)

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
                Payload: GQLWSPayload{
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

type ObjTypeMap map[reflect.Type]*graphql.Object
type FieldMap map[string]*graphql.Field

type GQLThread struct {
  BaseThread
  http_server *http.Server
  http_done *sync.WaitGroup
  extended_types ObjTypeMap
  extended_queries FieldMap
  extended_subscriptions FieldMap
  extended_mutations FieldMap
}

type GQLThreadInfo struct {
  ThreadInfo
  Start bool
  Started bool
}

func NewGQLThreadInfo(start bool) GQLThreadInfo {
  info := GQLThreadInfo{
    Start: start,
    Started: false,
  }
  return info
}

type GQLThreadState struct {
  BaseThreadState
  Listen string
}

func NewGQLThreadState(listen string) GQLThreadState {
  state := GQLThreadState{
    BaseThreadState: NewBaseThreadState("GQL Server"),
    Listen: listen,
  }
  state.InfoType = reflect.TypeOf((*GQLThreadInfo)(nil))
  return state
}

var gql_actions ThreadActions = ThreadActions{
  "start": func(ctx * GraphContext, thread Thread) (string, error) {
    ctx.Log.Logf("gql", "SERVER_STARTED")
    server := thread.(*GQLThread)

    mux := http.NewServeMux()
    http_handler, ws_handler := MakeGQLHandlers(ctx, server)
    mux.HandleFunc("/gql", http_handler)
    mux.HandleFunc("/gqlws", ws_handler)
    mux.HandleFunc("/graphiql", GraphiQLHandler())
    fs := http.FileServer(http.Dir("./site"))
    mux.Handle("/site/", http.StripPrefix("/site", fs))

    UseStates(ctx, []GraphNode{server}, func(states []NodeState)(interface{}, error){
      server_state := states[0].(*GQLThreadState)
      server.http_server = &http.Server{
        Addr: server_state.Listen,
        Handler: mux,
      }
      return nil, nil
    })

    server.http_done.Add(1)
    go func(server *GQLThread) {
      defer server.http_done.Done()
      err := server.http_server.ListenAndServe()
      if err != http.ErrServerClosed {
        panic(fmt.Sprintf("Failed to start gql server: %s", err))
      }
    }(server)

    return "wait", nil
  },
}

var gql_handlers ThreadHandlers = ThreadHandlers{
  "child_added": func(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_THREAD_CHILD_ADDED: %+v", signal)
    UseStates(ctx, []GraphNode{thread}, func(states []NodeState)(interface{}, error) {
      server_state := states[0].(*GQLThreadState)
      should_run, exists := server_state.child_info[signal.Source()].(*GQLThreadInfo)
      if exists == false {
        ctx.Log.Logf("gql", "GQL_THREAD_CHILD_ADDED: tried to start %s whis is not a child")
        return nil, nil
      }
      if should_run.Start == true && should_run.Started == false {
        ChildGo(ctx, server_state, thread, signal.Source())
        should_run.Started = false
      }
      return nil, nil
    })
    return "wait", nil
  },
  "abort": func(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_ABORT")
    server := thread.(*GQLThread)
    server.http_server.Shutdown(context.TODO())
    server.http_done.Wait()
    return "", fmt.Errorf("GQLThread aborted by signal")
  },
  "cancel": func(ctx * GraphContext, thread Thread, signal GraphSignal) (string, error) {
    ctx.Log.Logf("gql", "GQL_CANCEL")
    server := thread.(*GQLThread)
    server.http_server.Shutdown(context.TODO())
    server.http_done.Wait()
    return "", nil
  },
}

func NewGQLThread(ctx * GraphContext, listen string, requirements []Lockable, extended_types ObjTypeMap, extended_queries FieldMap, extended_mutations FieldMap, extended_subscriptions FieldMap) (*GQLThread, error) {
  state := NewGQLThreadState(listen)
  base_thread, err := NewBaseThread(ctx, gql_actions, gql_handlers, &state)
  if err != nil {
    return nil, err
  }

  thread := &GQLThread {
    BaseThread: base_thread,
    http_server: nil,
    http_done: &sync.WaitGroup{},
    extended_types: extended_types,
    extended_queries: extended_queries,
    extended_mutations: extended_mutations,
    extended_subscriptions: extended_subscriptions,
  }

  err = LinkLockables(ctx, thread, requirements)
  if err != nil {
    return nil, err
  }
  return thread, nil
}

func MakeGQLHandlers(ctx * GraphContext, server * GQLThread) (func(http.ResponseWriter, *http.Request), func(http.ResponseWriter, *http.Request)) {
  valid_nodes := map[reflect.Type]*graphql.Object{}
  valid_lockables := map[reflect.Type]*graphql.Object{}
  valid_threads := map[reflect.Type]*graphql.Object{}
  valid_lockables[reflect.TypeOf((*BaseLockable)(nil))] = GQLTypeBaseNode()
  for t, v := range(valid_lockables) {
    valid_nodes[t] = v
  }
  valid_threads[reflect.TypeOf((*BaseThread)(nil))] = GQLTypeBaseThread()
  valid_threads[reflect.TypeOf((*GQLThread)(nil))] = GQLTypeGQLThread()
  for t, v := range(valid_threads) {
    valid_lockables[t] = v
    valid_nodes[t] = v
  }


  gql_types := []graphql.Type{GQLTypeSignal(), GQLTypeSignalInput()}
  for _, v := range(valid_nodes) {
    gql_types = append(gql_types, v)
  }

  node_type := reflect.TypeOf((GraphNode)(nil))
  lockable_type := reflect.TypeOf((Lockable)(nil))
  thread_type := reflect.TypeOf((Thread)(nil))

  for go_t, gql_t := range(server.extended_types) {
    if go_t.Implements(node_type) {
      valid_nodes[go_t] = gql_t
    }
    if go_t.Implements(lockable_type) {
      valid_lockables[go_t] = gql_t
    }
    if go_t.Implements(thread_type) {
      valid_threads[go_t] = gql_t
    }
    gql_types = append(gql_types, gql_t)
  }

  gql_queries := graphql.Fields{
    "Self": GQLQuerySelf(),
  }

  for key, value := range(server.extended_queries) {
    gql_queries[key] = value
  }

  gql_subscriptions := graphql.Fields{
    "Update": GQLSubscriptionUpdate(),
  }

  for key, value := range(server.extended_subscriptions) {
    gql_subscriptions[key] = value
  }

  gql_mutations := graphql.Fields{
    "SendUpdate": GQLMutationSendUpdate(),
  }

  for key, value := range(server.extended_mutations) {
    gql_mutations[key] = value
  }

  schemaConfig  := graphql.SchemaConfig{
    Types: gql_types,
    Query: graphql.NewObject(graphql.ObjectConfig{
      Name: "Query",
      Fields: gql_queries,
    }),
    Mutation: graphql.NewObject(graphql.ObjectConfig{
      Name: "Mutation",
      Fields: gql_mutations,
    }),
    Subscription: graphql.NewObject(graphql.ObjectConfig{
      Name: "Subscription",
      Fields: gql_subscriptions,
    }),
  }

  schema, err := graphql.NewSchema(schemaConfig)
  if err != nil{
    panic(err)
  }
  gql_ctx := context.Background()
  gql_ctx = context.WithValue(gql_ctx, "valid_nodes", valid_nodes)
  gql_ctx = context.WithValue(gql_ctx, "node_type", node_type)
  gql_ctx = context.WithValue(gql_ctx, "valid_lockables", valid_lockables)
  gql_ctx = context.WithValue(gql_ctx, "lockable_type", lockable_type)
  gql_ctx = context.WithValue(gql_ctx, "valid_threads", valid_threads)
  gql_ctx = context.WithValue(gql_ctx, "thread_type", thread_type)
  gql_ctx = context.WithValue(gql_ctx, "gql_server", server)
  gql_ctx = context.WithValue(gql_ctx, "graph_context", ctx)
  return GQLHandler(ctx, schema, gql_ctx), GQLWSHandler(ctx, schema, gql_ctx)
}
