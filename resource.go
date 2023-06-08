package main

import (
  "fmt"
  "io"
  "reflect"
  "errors"
  "sync"
  "net/http"
  "github.com/graphql-go/graphql"
  "context"
  "encoding/json"
)

// Resources propagate update up to multiple parents, and not downwards
// (subscriber to team won't get update to alliance, but subscriber to alliance will get update to team)
func (resource * BaseResource) update(signal GraphSignal) {
  new_signal := signal.Trace(resource.ID())

  for _, parent := range resource.Parents() {
    SendUpdate(parent, new_signal)
  }
  resource.lock_holder_lock.Lock()
  if resource.lock_holder != nil {
    if resource.lock_holder.ID() != signal.Last() {
      lock_holder := resource.lock_holder
      resource.lock_holder_lock.Unlock()
      SendUpdate(lock_holder, new_signal)
    } else {
      resource.lock_holder_lock.Unlock()
    }
  } else {
    resource.lock_holder_lock.Unlock()
  }

}

// Resource is the interface that DAG nodes are made from
// A resource needs to be able to represent logical entities and connections to physical entities.
// A resource lock could be aborted at any time if this connection is broken, if that happens the event locking it must be aborted
// The device connection should be maintained as much as possible(requiring some reconnection behaviour in the background)
type Resource interface {
  GraphNode
  Owner() GraphNode
  Children() []Resource
  Parents() []Resource

  AddParent(parent Resource) error
  LockParents()
  UnlockParents()

  SetOwner(owner GraphNode)
  LockState()
  UnlockState()

  Init(abort chan error) bool
  lock(node GraphNode) error
  unlock(node GraphNode) error
}

func AddParent(resource Resource, parent Resource) error {
  if parent.ID() == resource.ID() {
    error_str := fmt.Sprintf("Will not add %s as parent of itself", parent.Name())
    return errors.New(error_str)
  }

  resource.LockParents()
  for _, p := range resource.Parents() {
    if p.ID() == parent.ID() {
      error_str := fmt.Sprintf("%s is already a parent of %s, will not double-bond", p.Name(), resource.Name())
      return errors.New(error_str)
    }
  }

  err := resource.AddParent(parent)
  resource.UnlockParents()

  return err
}

func UnlockResource(resource Resource, event Event) error {
  var err error = nil
  resource.LockState()
  if resource.Owner() == nil {
    resource.UnlockState()
    return errors.New("Resource already unlocked")
  }

  if resource.Owner().ID() != event.ID() {
    resource.UnlockState()
    return errors.New("Resource not locked by parent, unlock failed")
  }

  var lock_err error = nil
  for _, child := range resource.Children() {
    err := UnlockResource(child, event)
    if err != nil {
      lock_err = err
      break
    }
  }

  if lock_err != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Resource failed to unlock: %s", lock_err)
    return errors.New(err_str)
  }

  resource.SetOwner(nil)

  err = resource.unlock(event)
  if err != nil {
    resource.UnlockState()
    return errors.New("Failed to unlock resource")
  }

  resource.UnlockState()
  return nil
}

func LockResource(resource Resource, node GraphNode) error {
  resource.LockState()
  if resource.Owner() != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Resource already locked: %s", resource.Name())
    return errors.New(err_str)
  }

  err := resource.lock(node)
  if err != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Failed to lock resource: %s", err)
    return errors.New(err_str)
  }

  var lock_err error = nil
  locked_resources := []Resource{}
  for _, child := range resource.Children() {
    err := LockResource(child, node)
    if err != nil{
      lock_err = err
      break
    }
    locked_resources = append(locked_resources, child)
  }

  if lock_err != nil {
    resource.UnlockState()
    err_str := fmt.Sprintf("Resource failed to lock: %s", lock_err)
    return errors.New(err_str)
  }

  log.Logf("resource", "Locked %s", resource.Name())
  resource.SetOwner(node)


  resource.UnlockState()
  return nil
}

func NotifyResourceLocked(resource Resource) {
  signal := NewSignal(resource, "lock_changed")
  signal.description = "lock"

  for _, child := range resource.Children() {
    NotifyResourceLocked(child)
  }

  go SendUpdate(resource, signal)
}

func NotifyResourceUnlocked(resource Resource) {
  signal := NewSignal(resource, "lock_changed")
  signal.description = "unlock"

  for _, child := range(resource.Children()) {
    NotifyResourceUnlocked(child)
  }

  go SendUpdate(resource, signal)
}

// BaseResource is the most basic resource that can exist in the DAG
// It holds a single state variable, which contains a pointer to the event that is locking it
type BaseResource struct {
  BaseNode
  parents []Resource
  parents_lock sync.Mutex
  children []Resource
  children_lock sync.Mutex
  lock_holder GraphNode
  lock_holder_lock sync.Mutex
  state_lock sync.Mutex
}

func (resource * BaseResource) SetOwner(owner GraphNode) {
  resource.lock_holder_lock.Lock()
  resource.lock_holder = owner
  resource.lock_holder_lock.Unlock()
}

func (resource * BaseResource) LockState() {
  resource.state_lock.Lock()
}

func (resource * BaseResource) UnlockState() {
  resource.state_lock.Unlock()
}

func (resource * BaseResource) Init(abort chan error) bool {
  return false
}

func (resource * BaseResource) Owner() GraphNode {
  return resource.lock_holder
}

//BaseResources don't check anything special when locking/unlocking
func (resource * BaseResource) lock(node GraphNode) error {
  return nil
}

func (resource * BaseResource) unlock(node GraphNode) error {
  return nil
}

func (resource * BaseResource) Children() []Resource {
  return resource.children
}

func (resource * BaseResource) Parents() []Resource {
  return resource.parents
}

func (resource * BaseResource) LockParents() {
  resource.parents_lock.Lock()
}

func (resource * BaseResource) UnlockParents() {
  resource.parents_lock.Unlock()
}

func (resource * BaseResource) AddParent(parent Resource) error {
  resource.parents = append(resource.parents, parent)
  return nil
}

func NewBaseResource(name string, description string, children []Resource) BaseResource {
  resource := BaseResource{
    BaseNode: NewBaseNode(name, description, randid()),
    parents: []Resource{},
    children: children,
  }

  return resource
}

func NewResource(name string, description string, children []Resource) * BaseResource {
  resource := NewBaseResource(name, description, children)
  return &resource
}

type GQLServer struct {
  BaseResource
  abort chan error
  listen string
  gql_channel chan error
}

func NewGQLServer(listen string) * GQLServer {
  server := &GQLServer{
    BaseResource: NewBaseResource("GQL Server", "graphql server for event signals", []Resource{}),
    listen: listen,
    abort: make(chan error, 1),
    gql_channel: make(chan error, 1),
  }

  return server
}

func (server * GQLServer) update(signal GraphSignal) {
  server.signal <- signal
  server.BaseResource.update(signal)
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

func (server * GQLServer) Handler() func(http.ResponseWriter, *http.Request) {
  valid_events := map[reflect.Type]*graphql.Object{}
  valid_events[reflect.TypeOf((*BaseEvent)(nil))] = GQLTypeBaseEvent()
  valid_events[reflect.TypeOf((*EventQueue)(nil))] = GQLTypeEventQueue()

  schemaConfig  := graphql.SchemaConfig{
    Types: []graphql.Type{GQLTypeBaseEvent(), GQLTypeEventQueue()},
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

  return GQLHandler(schema, context.WithValue(context.Background(), "valid_events", valid_events))
}

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
