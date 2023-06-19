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
  "errors"
  "fmt"
  "sync"
  "time"
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

var gql_type_base_node *graphql.Object = nil
func GQLTypeBaseNode() *graphql.Object {
  if gql_type_base_node == nil {
    gql_type_base_node = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseNode",
      Interfaces: []*graphql.Interface{
        GQLInterfaceNode(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*BaseNode)
        return ok
      },
      Fields: graphql.Fields{},
    })

    gql_type_base_node.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceID,
    })

    gql_type_base_node.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceName,
    })

    gql_type_base_node.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceDescription,
    })
  }

  return gql_type_base_node
}

var gql_interface_node *graphql.Interface = nil
func GQLInterfaceNode() *graphql.Interface {
  if gql_interface_node == nil {
    gql_interface_node = graphql.NewInterface(graphql.InterfaceConfig{
      Name: "Node",
      ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
        valid_events, ok := p.Context.Value("valid_events").(map[reflect.Type]*graphql.Object)
        if ok == false {
          return nil
        }

        valid_resources, ok := p.Context.Value("valid_resources").(map[reflect.Type]*graphql.Object)
        if ok == false {
          return nil
        }

        for key, value := range(valid_events) {
          if reflect.TypeOf(p.Value) == key {
            return value
          }
        }

        for key, value := range(valid_resources) {
          if reflect.TypeOf(p.Value) == key {
            return value
          }
        }

        return GQLTypeBaseNode()
      },
      Fields: graphql.Fields{},
    })

    gql_interface_node.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_node.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_node.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
    })

  }

  return gql_interface_node
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

func GQLHandler(schema graphql.Schema, ctx context.Context) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    log.Logf("gql", "GQL REQUEST: %s", r.RemoteAddr)
    enableCORS(&w)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }
    log.Logm("gql", header_map, "REQUEST_HEADERS")

    str, err := io.ReadAll(r.Body)
    if err != nil {
      log.Logf("gql", "failed to read request body: %s", err)
      return
    }
    query := GQLWSPayload{}
    json.Unmarshal(str, &query)

    params := graphql.Params{
      Schema: schema,
      Context: ctx,
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
      log.Logm("gql", extra_fields, "wrong result, unexpected errors: %v", result.Errors)
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

func GQLWSDo(p graphql.Params) chan *graphql.Result {
  operation := getOperationTypeOfReq(p)
  log.Logf("gqlws", "GQLWSDO_OPERATION: %s %+v", operation, p.RequestString)

  if operation == ast.OperationTypeSubscription {
    return graphql.Subscribe(p)
  }

  res := graphql.Do(p)
  return sendOneResultAndClose(res)
}

func GQLWSHandler(schema graphql.Schema, ctx context.Context) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    log.Logf("gqlws_new", "HANDLING %s",r.RemoteAddr)
    header_map := map[string]interface{}{}
    for header, value := range(r.Header) {
      header_map[header] = value
    }
    log.Logm("gql", header_map, "REQUEST_HEADERS")
    u := ws.HTTPUpgrader{
      Protocol: func(protocol string) bool {
        log.Logf("gqlws", "UPGRADE_PROTOCOL: %s", string(protocol))
        return string(protocol) == "graphql-transport-ws"
      },
    }
    conn, _, _, err := u.Upgrade(r, w)
    if err == nil {
      defer conn.Close()
      conn_state := "init"
      for {
        // TODO: Make this a select between reading client data and getting updates from the event to push to clients"
        msg_raw, op, err := wsutil.ReadClientData(conn)
        log.Logf("gqlws_hb", "MSG: %s\nOP: 0x%02x\nERR: %+v\n", string(msg_raw), op, err)
        msg := GQLWSMsg{}
        json.Unmarshal(msg_raw, &msg)
        if err != nil {
          log.Logf("gqlws", "WS_CLIENT_ERROR")
          break
        }
        if msg.Type == "connection_init" {
          if conn_state != "init" {
            log.Logf("gqlws", "WS_CLIENT_ERROR: INIT WHILE IN %s", conn_state)
            break
          }
          conn_state = "ready"
          err = wsutil.WriteServerMessage(conn, 1, []byte("{\"type\": \"connection_ack\"}"))
          if err != nil {
            log.Logf("gqlws", "WS_SERVER_ERROR: FAILED TO SEND connection_ack")
            break
          }
        } else if msg.Type == "ping" {
          log.Logf("gqlws_hb", "PING FROM %s", r.RemoteAddr)
          err = wsutil.WriteServerMessage(conn, 1, []byte("{\"type\": \"pong\"}"))
          if err != nil {
            log.Logf("gqlws", "WS_SERVER_ERROR: FAILED TO SEND PONG")
          }
        } else if msg.Type == "subscribe" {
          log.Logf("gqlws", "SUBSCRIBE: %+v", msg.Payload)
          params := graphql.Params{
            Schema: schema,
            Context: ctx,
            RequestString: msg.Payload.Query,
          }
          if msg.Payload.OperationName != "" {
            params.OperationName = msg.Payload.OperationName
          }
          if len(msg.Payload.Variables) > 0 {
            params.VariableValues = msg.Payload.Variables
          }

          res_chan := GQLWSDo(params)

          go func(res_chan chan *graphql.Result) {
            for {
              next, ok := <-res_chan
              if ok == false {
                log.Logf("gqlws", "response channel was closed")
                return
              }
              if next == nil {
                log.Logf("gqlws", "NIL_ON_CHANNEL")
                return
              }
              if len(next.Errors) > 0 {
                extra_fields := map[string]interface{}{}
                extra_fields["query"] = string(msg.Payload.Query)
                log.Logm("gqlws", extra_fields, "ERROR: wrong result, unexpected errors: %+v", next.Errors)
                continue
              }
              log.Logf("gqlws", "DATA: %+v", next.Data)
              data, err := json.Marshal(next.Data)
              if err != nil {
                log.Logf("gqlws", "ERROR: %+v", err)
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
                log.Logf("gqlws", "ERROR: %+v", err)
                continue
              }

              err = wsutil.WriteServerMessage(conn, 1, msg)
              if err != nil {
                log.Logf("gqlws", "ERROR: %+v", err)
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

func GQLEventFn(p graphql.ResolveParams, fn func(Event, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
    if event, ok := p.Source.(Event); ok {
      return fn(event, p)
    }
    return nil, errors.New("Failed to cast source to event")
}

func GQLEventID(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams)(interface{}, error) {
    return event.ID(), nil
  })
}

func GQLEventName(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams)(interface{}, error) {
    return event.Name(), nil
  })
}

func GQLEventDescription(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams)(interface{}, error) {
    return event.Description(), nil
  })
}

func GQLEventChildren(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams)(interface{}, error) {
    return event.Children(), nil
  })
}

var gql_list_resource * graphql.List = nil
func GQLListResource() * graphql.List {
  if gql_list_resource == nil {
    gql_list_resource = graphql.NewList(GQLInterfaceResource())
  }

  return gql_list_resource
}

var gql_interface_resource * graphql.Interface = nil
func GQLInterfaceResource() * graphql.Interface {
  if gql_interface_resource == nil {
    gql_interface_resource = graphql.NewInterface(graphql.InterfaceConfig{
      Name: "Resource",
      ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
        if p.Value == nil {
          return GQLTypeBaseResource()
        }
        valid_resources, ok := p.Context.Value("valid_resources").(map[reflect.Type]*graphql.Object)
        if ok == false {
          return nil
        }
        for key, value := range(valid_resources) {
          if reflect.TypeOf(p.Value) == key {
            return value
          }
        }
        return nil
      },
      Fields: graphql.Fields{},
    })

    if gql_list_resource == nil {
      gql_list_resource = graphql.NewList(gql_interface_resource)
    }

    gql_interface_resource.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_resource.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_resource.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_resource.AddFieldConfig("Parents", &graphql.Field{
      Type: GQLListResource(),
    })

    gql_interface_resource.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceNode(),
    })

  }

  return gql_interface_resource
}

func GQLResourceFn(p graphql.ResolveParams, fn func(Resource, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
    if resource, ok := p.Source.(Resource); ok {
      return fn(resource, p)
    }
    return nil, errors.New(fmt.Sprintf("Failed to cast source to resource, %+v", p.Source))
}

func GQLResourceID(p graphql.ResolveParams) (interface{}, error) {
  return GQLResourceFn(p, func(resource Resource, p graphql.ResolveParams) (interface{}, error) {
    return resource.ID(), nil
  })
}

func GQLResourceName(p graphql.ResolveParams) (interface{}, error) {
  return GQLResourceFn(p, func(resource Resource, p graphql.ResolveParams) (interface{}, error) {
    return resource.Name(), nil
  })
}

func GQLResourceDescription(p graphql.ResolveParams) (interface{}, error) {
  return GQLResourceFn(p, func(resource Resource, p graphql.ResolveParams) (interface{}, error) {
    return resource.Description(), nil
  })
}

func GQLResourceParents(p graphql.ResolveParams) (interface{}, error) {
  return GQLResourceFn(p, func(resource Resource, p graphql.ResolveParams) (interface{}, error) {
    return resource.Parents(), nil
  })
}

func GQLResourceOwner(p graphql.ResolveParams) (interface{}, error) {
  return GQLResourceFn(p, func(resource Resource, p graphql.ResolveParams) (interface{}, error) {
    return resource.Owner(), nil
  })
}

var gql_type_base_resource *graphql.Object = nil
func GQLTypeBaseResource() * graphql.Object {
  if gql_type_base_resource == nil {
    gql_type_base_resource = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseResource",
      Interfaces: []*graphql.Interface{
        GQLInterfaceResource(),
        GQLInterfaceNode(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*BaseResource)
        return ok
      },
      Fields: graphql.Fields{},
    })

    gql_type_base_resource.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceID,
    })

    gql_type_base_resource.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceName,
    })

    gql_type_base_resource.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceDescription,
    })

    gql_type_base_resource.AddFieldConfig("Parents", &graphql.Field{
      Type: GQLListResource(),
      Resolve: GQLResourceParents,
    })

    gql_type_base_resource.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceNode(),
      Resolve: GQLResourceOwner,
    })
  }

  return gql_type_base_resource
}

var gql_list_event * graphql.List = nil
func GQLListEvent() * graphql.List {
  if gql_list_event == nil {
    gql_list_event = graphql.NewList(GQLInterfaceEvent())
  }
  return gql_list_event
}

var gql_interface_event * graphql.Interface = nil
func GQLInterfaceEvent() * graphql.Interface {
  if gql_interface_event == nil {
    gql_interface_event = graphql.NewInterface(graphql.InterfaceConfig{
      Name: "Event",
      ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
        valid_events, ok := p.Context.Value("valid_events").(map[reflect.Type]*graphql.Object)
        if ok == false {
          return nil
        }
        for key, value := range(valid_events) {
          if reflect.TypeOf(p.Value) == key {
            return value
          }
        }
        return nil
      },
      Fields: graphql.Fields{},
    })

    if gql_list_event == nil {
      gql_list_event = graphql.NewList(gql_interface_event)
    }

    gql_interface_event.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_event.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_event.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_event.AddFieldConfig("Children", &graphql.Field{
      Type: gql_list_event,
    })
  }

  return gql_interface_event
}

var gql_type_base_event * graphql.Object = nil
func GQLTypeBaseEvent() * graphql.Object {
  if gql_type_base_event == nil {
    gql_type_base_event = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseEvent",
      Interfaces: []*graphql.Interface{
        GQLInterfaceEvent(),
        GQLInterfaceNode(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*BaseEvent)
        return ok
      },
      Fields: graphql.Fields{},
    })
    gql_type_base_event.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventID,
    })

    gql_type_base_event.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventName,
    })

    gql_type_base_event.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventDescription,
    })

    gql_type_base_event.AddFieldConfig("Children", &graphql.Field{
      Type: GQLListEvent(),
      Resolve: GQLEventChildren,
    })
  }

  return gql_type_base_event
}

var gql_type_event_queue * graphql.Object = nil
func GQLTypeEventQueue() * graphql.Object {
  if gql_type_event_queue == nil {
    gql_type_event_queue = graphql.NewObject(graphql.ObjectConfig{
      Name: "EventQueue",
      Interfaces: []*graphql.Interface{
        GQLInterfaceEvent(),
        GQLInterfaceNode(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*EventQueue)
        return ok
      },
      Fields: graphql.Fields{},
    })
    gql_type_event_queue.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventID,
    })
    gql_type_event_queue.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventName,
    })
    gql_type_event_queue.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventDescription,
    })
    gql_type_event_queue.AddFieldConfig("Children", &graphql.Field{
      Type: GQLListEvent(),
      Resolve: GQLEventChildren,
    })
  }
  return gql_type_event_queue
}

func GQLSignalFn(p graphql.ResolveParams, fn func(GraphSignal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
    if signal, ok := p.Source.(GraphSignal); ok {
      return fn(signal, p)
    }
    return nil, errors.New("Failed to cast source to event")
}

func GQLSignalType(p graphql.ResolveParams) (interface{}, error) {
  return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
    return signal.Type(), nil
  })
}

func GQLSignalSource(p graphql.ResolveParams) (interface{}, error) {
  return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
    return signal.Source(), nil
  })
}

func GQLSignalDownwards(p graphql.ResolveParams) (interface{}, error) {
    return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
      return signal.Downwards(), nil
    })
}

func GQLSignalString(p graphql.ResolveParams) (interface{}, error) {
    return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
      return signal.String(), nil
    })
}


var gql_type_signal *graphql.Object = nil
func GQLTypeSignal() *graphql.Object {
  if gql_type_signal == nil {
    gql_type_signal = graphql.NewObject(graphql.ObjectConfig{
      Name: "SignalOut",
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(GraphSignal)
        return ok
      },
      Fields: graphql.Fields{},
    })

    gql_type_signal.AddFieldConfig("Type", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLSignalType,
    })
    gql_type_signal.AddFieldConfig("Source", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLSignalSource,
    })
    gql_type_signal.AddFieldConfig("Downwards", &graphql.Field{
      Type: graphql.Boolean,
      Resolve: GQLSignalDownwards,
    })
    gql_type_signal.AddFieldConfig("String", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLSignalString,
    })
  }
  return gql_type_signal
}

var gql_type_signal_input *graphql.InputObject = nil
func GQLTypeSignalInput() *graphql.InputObject {
  if gql_type_signal_input == nil {
    gql_type_signal_input = graphql.NewInputObject(graphql.InputObjectConfig{
      Name: "SignalIn",
      Fields: graphql.InputObjectConfigFieldMap{},
    })
    gql_type_signal_input.AddFieldConfig("Type", &graphql.InputObjectFieldConfig{
      Type: graphql.String,
    })
    gql_type_signal_input.AddFieldConfig("Description", &graphql.InputObjectFieldConfig{
      Type: graphql.String,
      DefaultValue: "",
    })
    gql_type_signal_input.AddFieldConfig("Time", &graphql.InputObjectFieldConfig{
      Type: graphql.DateTime,
      DefaultValue: time.Now(),
    })
  }
  return gql_type_signal_input
}

var gql_mutation_update_event *graphql.Field = nil
func GQLMutationUpdateEvent() *graphql.Field {
  if gql_mutation_update_event == nil {
    gql_mutation_update_event = &graphql.Field{
      Type: GQLTypeSignal(),
      Args: graphql.FieldConfigArgument{
        "id": &graphql.ArgumentConfig{
          Type: graphql.String,
        },
        "signal": &graphql.ArgumentConfig{
          Type: GQLTypeSignalInput(),
        },
      },
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        server, ok := p.Context.Value("gql_server").(*GQLServer)
        if ok == false {
          return nil, errors.New(fmt.Sprintf("Failed to cast context gql_server to GQLServer: %+v", p.Context.Value("gql_server")))
        }

        signal_map, ok := p.Args["signal"].(map[string]interface{})
        if ok == false {
          return nil, errors.New(fmt.Sprintf("Failed to cast arg signal to GraphSignal: %+v", p.Args["signal"]))
        }
        var signal GraphSignal = nil
        if signal_map["Downwards"] == false {
          signal = NewSignal(server, signal_map["Type"].(string))
        } else {
          signal = NewDownSignal(server, signal_map["Type"].(string))
        }

        id , ok := p.Args["id"].(string)
        if ok == false {
          return nil, errors.New("Failed to cast arg id to string")
        }

        owner := server.Owner()
        if owner == nil {
          return nil, errors.New("Cannot send update without owner")
        }

        root_event, ok := owner.(Event)
        if ok == false {
          return nil, errors.New("Cannot send update to Event unless owned by an Event")
        }

        node := FindChild(root_event, id)
        if node == nil {
          return nil, errors.New("Failed to find id in event tree from server")
        }

        SendUpdate(node, signal)
        return signal, nil
      },
    }
  }

  return gql_mutation_update_event
}

type GQLServer struct {
  BaseResource
  abort chan error
  listen string
  gql_channel chan error
  extended_types map[reflect.Type]*graphql.Object
  extended_queries map[string]*graphql.Field
  extended_subscriptions map[string]*graphql.Field
  extended_mutations map[string]*graphql.Field
}

func NewGQLServer(listen string, extended_types map[reflect.Type]*graphql.Object, extended_queries map[string]*graphql.Field, extended_mutations map[string]*graphql.Field, extended_subscriptions map[string]*graphql.Field) * GQLServer {
  server := &GQLServer{
    BaseResource: NewBaseResource("GQL Server", "graphql server for event signals", []Resource{}),
    listen: listen,
    abort: make(chan error, 1),
    gql_channel: make(chan error, 1),
    extended_types: extended_types,
    extended_queries: extended_queries,
    extended_mutations: extended_mutations,
    extended_subscriptions: extended_subscriptions,
  }

  return server
}

func (server * GQLServer) PropagateUpdate(signal GraphSignal) {
  server.signal <- signal
  server.BaseResource.PropagateUpdate(signal)
}

func GQLSubscribeSignal(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error) {
    return signal, nil
  })
}

func GQLSubscribeFn(p graphql.ResolveParams, fn func(GraphSignal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
  server, ok := p.Context.Value("gql_server").(*GQLServer)
  if ok == false {
    return nil, fmt.Errorf("Failed to get gql_Server from context and cast to GQLServer")
  }

  c := make(chan interface{})
  go func(c chan interface{}, server *GQLServer) {
    sig_c := server.UpdateChannel()
    for {
      val, ok := <- sig_c
      if ok == false {
        return
      }
      ret, err := fn(val, p)
      if err != nil {
        log.Logf("gqlws", "type convertor error %s", err)
        return
      }
      c <- ret
    }
  }(c, server)
  return c, nil
}

var gql_subscription_update * graphql.Field = nil
func GQLSubscriptionUpdate() * graphql.Field {
  if gql_subscription_update == nil {
    gql_subscription_update = &graphql.Field{
      Type: GQLTypeSignal(),
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        return p.Source, nil
      },
      Subscribe: GQLSubscribeSignal,
    }
  }

  return gql_subscription_update
}

func MakeGQLHandlers(server * GQLServer) (func(http.ResponseWriter, *http.Request), func(http.ResponseWriter, *http.Request)) {
  valid_events := map[reflect.Type]*graphql.Object{}
  valid_events[reflect.TypeOf((*BaseEvent)(nil))] = GQLTypeBaseEvent()
  valid_events[reflect.TypeOf((*EventQueue)(nil))] = GQLTypeEventQueue()

  valid_resources := map[reflect.Type]*graphql.Object{}
  valid_resources[reflect.TypeOf((*BaseResource)(nil))] = GQLTypeBaseResource()

  gql_types := []graphql.Type{GQLTypeBaseEvent(), GQLTypeEventQueue(), GQLTypeSignal(), GQLTypeSignalInput()}
  for go_t, gql_t := range(server.extended_types) {
    valid_events[go_t] = gql_t
    gql_types = append(gql_types, gql_t)
  }

  gql_queries := graphql.Fields{
    "Owner": GQLQueryOwner(),
  }

  for key, value := range(server.extended_queries) {
    gql_queries[key] = value
  }

  gql_subscriptions := graphql.Fields{
    "Updates": GQLSubscriptionUpdate(),
  }

  for key, value := range(server.extended_subscriptions) {
    gql_subscriptions[key] = value
  }

  gql_mutations := graphql.Fields{
    "updateEvent": GQLMutationUpdateEvent(),
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
  ctx := context.Background()
  ctx = context.WithValue(ctx, "valid_events", valid_events)
  ctx = context.WithValue(ctx, "valid_resources", valid_resources)
  ctx = context.WithValue(ctx, "gql_server", server)
  return GQLHandler(schema, ctx), GQLWSHandler(schema, ctx)
}

var gql_query_owner *graphql.Field = nil
func GQLQueryOwner() *graphql.Field {
  if gql_query_owner == nil {
    gql_query_owner = &graphql.Field{
      Type: GQLInterfaceEvent(),
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        server, ok := p.Context.Value("gql_server").(*GQLServer)

        if ok == false {
          panic("Failed to get/cast gql_server from context")
        }

        return server.Owner(), nil
      },
    }
  }

  return gql_query_owner
}

func (server * GQLServer) Init(abort chan error) bool {
  go func(abort chan error) {
    log.Logf("gql", "GOROUTINE_START for %s", server.ID())

    mux := http.NewServeMux()
    http_handler, ws_handler := MakeGQLHandlers(server)
    mux.HandleFunc("/gql", http_handler)
    mux.HandleFunc("/gqlws", ws_handler)
    mux.HandleFunc("/", GraphiQLHandler())

    srv := &http.Server{
      Addr: server.listen,
      Handler: mux,
    }

    http_done := &sync.WaitGroup{}
    http_done.Add(1)
    go func(srv *http.Server, http_done *sync.WaitGroup) {
      defer http_done.Done()
      err := srv.ListenAndServe()
      if err != http.ErrServerClosed {
        panic(fmt.Sprintf("Failed to start gql server: %s", err))
      }
    }(srv, http_done)

    for true {
      select {
      case <-abort:
        log.Logf("gql", "GOROUTINE_ABORT for %s", server.ID())
        err := srv.Shutdown(context.Background())
        if err != nil{
          panic(fmt.Sprintf("Failed to shutdown gql server: %s", err))
        }
        http_done.Wait()
        break
      case signal:=<-server.signal:
        log.Logf("gql", "GOROUTINE_SIGNAL for %s: %+v", server.ID(), signal)
        // Take signals to resource and send to GQL subscriptions
      }
    }
  }(abort)
  return true
}
