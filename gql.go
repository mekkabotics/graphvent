package main

import (
  "net/http"
  "github.com/graphql-go/graphql"
  "context"
  "encoding/json"
  "io"
  "reflect"
  "errors"
  "fmt"
  "sync"
)

func GraphiQLHandler() func(http.ResponseWriter, *http.Request) {
  graphiql_string := `
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
        height: 100%;
        margin: 0;
        width: 100%;
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
            url: 'http://localhost:8080/gql',
          }),
          defaultEditorToolsVisibility: true,
        }),
      );
    </script>
  </body>
</html>
`

  return func(w http.ResponseWriter, r * http.Request) {
    w.Header().Set("Content-Type", "text/html; charset=utf-8")
    w.WriteHeader(http.StatusOK)
    io.WriteString(w, graphiql_string)
  }

}

type GQLQuery struct {
  Query string `json:"query"`
}

func GQLHandler(schema graphql.Schema, ctx context.Context) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r * http.Request) {
    str, err := io.ReadAll(r.Body)
    if err != nil {
      log.Logf("gql", "failed to read request body: %s", err)
      return
    }
    res := GQLQuery{}
    json.Unmarshal(str, &res)
    result := graphql.Do(graphql.Params{
      Schema: schema,
      Context: ctx,
      RequestString: res.Query,
    })
    if len(result.Errors) > 0 {
      log.Logf("gql", "wrong result, unexpected errors: %v", result.Errors)
    }
    json.NewEncoder(w).Encode(result)
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

var gql_type_base_resource *graphql.Object = nil
func GQLTypeBaseResource() * graphql.Object {
  if gql_type_base_resource == nil {
    gql_type_base_resource = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseResource",
      Interfaces: []*graphql.Interface{
        GQLInterfaceResource(),
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

type GQLServer struct {
  BaseResource
  abort chan error
  listen string
  gql_channel chan error
  extended_types map[reflect.Type]*graphql.Object
}

func NewGQLServer(listen string, extended_types map[reflect.Type]*graphql.Object) * GQLServer {
  server := &GQLServer{
    BaseResource: NewBaseResource("GQL Server", "graphql server for event signals", []Resource{}),
    listen: listen,
    abort: make(chan error, 1),
    gql_channel: make(chan error, 1),
    extended_types: extended_types,
  }

  return server
}

func (server * GQLServer) update(signal GraphSignal) {
  server.signal <- signal
  server.BaseResource.update(signal)
}

func (server * GQLServer) Handler() func(http.ResponseWriter, *http.Request) {
  valid_events := map[reflect.Type]*graphql.Object{}
  valid_events[reflect.TypeOf((*BaseEvent)(nil))] = GQLTypeBaseEvent()
  valid_events[reflect.TypeOf((*EventQueue)(nil))] = GQLTypeEventQueue()

  valid_resources := map[reflect.Type]*graphql.Object{}
  valid_resources[reflect.TypeOf((*BaseResource)(nil))] = GQLTypeBaseResource()

  gql_types := []graphql.Type{GQLTypeBaseEvent(), GQLTypeEventQueue()}
  for go_t, gql_t := range(server.extended_types) {
    valid_events[go_t] = gql_t
    gql_types = append(gql_types, gql_t)
  }

  schemaConfig  := graphql.SchemaConfig{
    Types: gql_types,
    Query: graphql.NewObject(graphql.ObjectConfig{
      Name: "Query",
      Fields: graphql.Fields{
        "Owner": &graphql.Field{
          Type: GQLInterfaceEvent(),
          Resolve: func(p graphql.ResolveParams) (interface{}, error) {
            server.lock_holder_lock.Lock()
            defer server.lock_holder_lock.Unlock()

            owner := server.Owner()

            return owner, nil
          },
        },
      },
    }),
  }

  schema, err := graphql.NewSchema(schemaConfig)
  if err != nil{
    panic(err)
  }
  ctx := context.Background()
  ctx = context.WithValue(ctx, "valid_events", valid_events)
  ctx = context.WithValue(ctx, "valid_resources", valid_resources)
  return GQLHandler(schema, ctx)
}

func (server * GQLServer) Init(abort chan error) bool {
  go func(abort chan error) {
    log.Logf("gql", "GOROUTINE_START for %s", server.ID())

    mux := http.NewServeMux()
    mux.HandleFunc("/gql", server.Handler())
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
