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

func GQLHandler(ctx * GraphContext, server * GQLThread) func(http.ResponseWriter, *http.Request) {
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

    str, err := io.ReadAll(r.Body)
    if err != nil {
      ctx.Log.Logf("gql", "failed to read request body: %s", err)
      return
    }
    query := GQLWSPayload{}
    json.Unmarshal(str, &query)

    params := graphql.Params{
      Schema: ctx.GQL.Schema,
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

func GQLWSHandler(ctx * GraphContext, server * GQLThread) func(http.ResponseWriter, *http.Request) {
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
            Schema: ctx.GQL.Schema,
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

type TypeList []graphql.Type
type ObjTypeMap map[reflect.Type]*graphql.Object
type FieldMap map[string]*graphql.Field

type GQLThread struct {
  BaseThread
  http_server *http.Server
  http_done *sync.WaitGroup
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

type GQLThreadStateJSON struct {
  BaseThreadStateJSON
  Listen string `json:"listen"`
}

type GQLThreadState struct {
  BaseThreadState
  Listen string
}

func (state * GQLThreadState) MarshalJSON() ([]byte, error) {
  thread_state := SaveBaseThreadState(&state.BaseThreadState)
  return json.Marshal(&GQLThreadStateJSON{
    BaseThreadStateJSON: thread_state,
    Listen: state.Listen,
  })
}

func LoadGQLThreadState(ctx * GraphContext, data []byte, loaded_nodes NodeMap) (NodeState, error){
  var j GQLThreadStateJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  thread_state, err := RestoreBaseThreadState(ctx, j.BaseThreadStateJSON, loaded_nodes)
  if err != nil {
    return nil, err
  }

  state := &GQLThreadState{
    BaseThreadState: *thread_state,
    Listen: j.Listen,
  }

  return state, nil
}

func LoadGQLThread(ctx * GraphContext, id NodeID) (GraphNode, error) {
  thread := RestoreBaseThread(ctx, id)
  gql_thread := GQLThread{
    BaseThread: thread,
    http_server: nil,
    http_done: &sync.WaitGroup{},
  }

  return &gql_thread, nil
}

func NewGQLThreadState(listen string) GQLThreadState {
  state := GQLThreadState{
    BaseThreadState: NewBaseThreadState("GQL Server", "gql_thread"),
    Listen: listen,
  }
  state.InfoType = reflect.TypeOf((*GQLThreadInfo)(nil))
  return state
}

var gql_actions ThreadActions = ThreadActions{
  "start": func(ctx * GraphContext, thread Thread) (string, error) {
    ctx.Log.Logf("gql", "SERVER_STARTED")
    server := thread.(*GQLThread)

    // Serve the GQL http and ws handlers
    mux := http.NewServeMux()
    mux.HandleFunc("/gql", GQLHandler(ctx, server))
    mux.HandleFunc("/gqlws", GQLWSHandler(ctx, server))

    // Server a graphiql interface(TODO make configurable whether to start this)
    mux.HandleFunc("/graphiql", GraphiQLHandler())

    // Server the ./site directory to /site (TODO make configurable with better defaults)
    fs := http.FileServer(http.Dir("./site"))
    mux.Handle("/site/", http.StripPrefix("/site", fs))

    UseStates(ctx, []GraphNode{server}, func(states NodeStateMap)(error){
      server_state := states[server.ID()].(*GQLThreadState)
      server.http_server = &http.Server{
        Addr: server_state.Listen,
        Handler: mux,
      }
      return nil
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
    UseStates(ctx, []GraphNode{thread}, func(states NodeStateMap)(error) {
      server_state := states[thread.ID()].(*GQLThreadState)
      should_run, exists := server_state.child_info[signal.Source()].(*GQLThreadInfo)
      if exists == false {
        ctx.Log.Logf("gql", "GQL_THREAD_CHILD_ADDED: tried to start %s whis is not a child")
        return nil
      }
      if should_run.Start == true && should_run.Started == false {
        ChildGo(ctx, server_state, thread, signal.Source())
        should_run.Started = false
      }
      return nil
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

func NewGQLThread(ctx * GraphContext, listen string, requirements []Lockable) (*GQLThread, error) {
  state := NewGQLThreadState(listen)
  base_thread, err := NewBaseThread(ctx, gql_actions, gql_handlers, &state)
  if err != nil {
    return nil, err
  }

  thread := &GQLThread {
    BaseThread: base_thread,
    http_server: nil,
    http_done: &sync.WaitGroup{},
  }

  err = LinkLockables(ctx, thread, requirements)
  if err != nil {
    return nil, err
  }
  return thread, nil
}
