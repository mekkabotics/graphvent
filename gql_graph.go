package graphvent

import (
  "github.com/graphql-go/graphql"
  "reflect"
  "fmt"
)

var gql_interface_graph_node *graphql.Interface = nil
func GQLInterfaceGraphNode() *graphql.Interface {
  if gql_interface_graph_node == nil {
    gql_interface_graph_node = graphql.NewInterface(graphql.InterfaceConfig{
      Name: "GraphNode",
      ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return nil
        }

        valid_nodes := ctx.GQL.ValidNodes
        node_type := ctx.GQL.NodeType
        p_type := reflect.TypeOf(p.Value)

        for key, value := range(valid_nodes) {
          if p_type == key {
            return value
          }
        }

        if p_type.Implements(node_type) {
          return GQLTypeBaseNode()
        }

        return nil
      },
      Fields: graphql.Fields{},
    })

    gql_interface_graph_node.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_graph_node.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
    })
  }

  return gql_interface_graph_node
}

var gql_list_thread *graphql.List = nil
func GQLListThread() *graphql.List {
  if gql_list_thread == nil {
    gql_list_thread = graphql.NewList(GQLInterfaceThread())
  }
  return gql_list_thread
}

var gql_interface_thread *graphql.Interface = nil
func GQLInterfaceThread() *graphql.Interface {
  if gql_interface_thread == nil {
    gql_interface_thread = graphql.NewInterface(graphql.InterfaceConfig{
      Name: "Thread",
      ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return nil
        }

        valid_threads := ctx.GQL.ValidThreads
        thread_type := ctx.GQL.ThreadType
        p_type := reflect.TypeOf(p.Value)

        for key, value := range(valid_threads) {
          if p_type == key {
            return value
          }
        }

        if p_type.Implements(thread_type) {
          return GQLTypeBaseThread()
        }

        ctx.Log.Logf("gql", "Found no type that matches %+v: %+v", p_type, p_type.Implements(thread_type))
        return nil
      },
      Fields: graphql.Fields{},
    })

    gql_interface_thread.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_thread.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_thread.AddFieldConfig("Children", &graphql.Field{
      Type: GQLListThread(),
    })

    gql_interface_thread.AddFieldConfig("Parent", &graphql.Field{
      Type: GQLInterfaceThread(),
    })

    gql_interface_thread.AddFieldConfig("Requirements", &graphql.Field{
      Type: GQLListLockable(),
    })

    gql_interface_thread.AddFieldConfig("Dependencies", &graphql.Field{
      Type: GQLListLockable(),
    })

    gql_interface_thread.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceLockable(),
    })
  }

  return gql_interface_thread
}

var gql_list_lockable *graphql.List = nil
func GQLListLockable() *graphql.List {
  if gql_list_lockable == nil {
    gql_list_lockable = graphql.NewList(GQLInterfaceLockable())
  }
  return gql_list_lockable
}

var gql_interface_lockable *graphql.Interface = nil
func GQLInterfaceLockable() *graphql.Interface {
  if gql_interface_lockable == nil {
    gql_interface_lockable = graphql.NewInterface(graphql.InterfaceConfig{
      Name: "Lockable",
      ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return nil
        }

        valid_lockables := ctx.GQL.ValidLockables
        lockable_type := ctx.GQL.LockableType
        p_type := reflect.TypeOf(p.Value)

        for key, value := range(valid_lockables) {
          if p_type == key {
            return value
          }
        }

        if p_type.Implements(lockable_type) {
          return GQLTypeBaseLockable()
        }
        return nil
      },
      Fields: graphql.Fields{},
    })

    gql_interface_lockable.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
    })

    gql_interface_lockable.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
    })

    if gql_list_lockable == nil {
      gql_list_lockable = graphql.NewList(gql_interface_lockable)
    }

    gql_interface_lockable.AddFieldConfig("Requirements", &graphql.Field{
      Type: gql_list_lockable,
    })

    gql_interface_lockable.AddFieldConfig("Dependencies", &graphql.Field{
      Type: gql_list_lockable,
    })

    gql_interface_lockable.AddFieldConfig("Owner", &graphql.Field{
      Type: gql_interface_lockable,
    })

  }

  return gql_interface_lockable
}

func GQLNodeID(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(GraphNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GraphNode")
  }
  return node.ID(), nil
}

func GQLNodeName(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(GraphNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GraphNode")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  name := ""
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    name = states[node.ID()].Name()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return name, nil
}

func GQLThreadListen(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(*GQLThread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GQLThread")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  listen := ""
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    gql_thread, ok := states[node.ID()].(*GQLThreadState)
    if ok == false {
      return fmt.Errorf("Failed to cast state to GQLThreadState")
    }
    listen = gql_thread.Listen
    return nil
  })

  if err != nil {
    return nil, err
  }

  return listen, nil
}

func GQLThreadParent(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Thread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  var parent Thread = nil
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    gql_thread, ok := states[node.ID()].(ThreadState)
    if ok == false {
      return fmt.Errorf("Failed to cast state to ThreadState")
    }
    parent = gql_thread.Parent()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return parent, nil
}

func GQLThreadChildren(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Thread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  var children []Thread = nil
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    gql_thread, ok := states[node.ID()].(ThreadState)
    if ok == false {
      return fmt.Errorf("Failed to cast state to ThreadState")
    }
    children = gql_thread.Children()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return children, nil
}

func GQLLockableRequirements(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  var requirements []Lockable = nil
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    gql_thread, ok := states[node.ID()].(LockableState)
    if ok == false {
      return fmt.Errorf("Failed to cast state to LockableState")
    }
    requirements = gql_thread.Requirements()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return requirements, nil
}

func GQLLockableDependencies(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  var dependencies []Lockable = nil
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    gql_thread, ok := states[node.ID()].(LockableState)
    if ok == false {
      return fmt.Errorf("Failed to cast state to LockableState")
    }
    dependencies = gql_thread.Dependencies()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return dependencies, nil
}

func GQLLockableOwner(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext")
  }

  var owner GraphNode = nil
  err := UseStates(ctx, []GraphNode{node}, func(states NodeStateMap) (error) {
    gql_thread, ok := states[node.ID()].(LockableState)
    if ok == false {
      return fmt.Errorf("Failed to cast state to LockableState")
    }
    owner = gql_thread.Owner()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return owner, nil
}


var gql_type_gql_thread *graphql.Object = nil
func GQLTypeGQLThread() * graphql.Object {
  if gql_type_gql_thread == nil {
    gql_type_gql_thread = graphql.NewObject(graphql.ObjectConfig{
      Name: "GQLThread",
      Interfaces: []*graphql.Interface{
        GQLInterfaceGraphNode(),
        GQLInterfaceThread(),
        GQLInterfaceLockable(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*GQLThread)
        return ok
      },
      Fields: graphql.Fields{},
    })

    gql_type_gql_thread.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeID,
    })

    gql_type_gql_thread.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeName,
    })

    gql_type_gql_thread.AddFieldConfig("Children", &graphql.Field{
      Type: GQLListThread(),
      Resolve: GQLThreadChildren,
    })

    gql_type_gql_thread.AddFieldConfig("Parent", &graphql.Field{
      Type: GQLInterfaceThread(),
      Resolve: GQLThreadParent,
    })

    gql_type_gql_thread.AddFieldConfig("Listen", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLThreadListen,
    })

    gql_type_gql_thread.AddFieldConfig("Requirements", &graphql.Field{
      Type: GQLListLockable(),
      Resolve: GQLLockableRequirements,
    })

    gql_type_gql_thread.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceLockable(),
      Resolve: GQLLockableOwner,
    })

    gql_type_gql_thread.AddFieldConfig("Dependencies", &graphql.Field{
      Type: GQLListLockable(),
      Resolve: GQLLockableDependencies,
    })
  }
  return gql_type_gql_thread
}

var gql_type_base_thread *graphql.Object = nil
func GQLTypeBaseThread() * graphql.Object {
  if gql_type_base_thread == nil {
    gql_type_base_thread = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseThread",
      Interfaces: []*graphql.Interface{
        GQLInterfaceGraphNode(),
        GQLInterfaceThread(),
        GQLInterfaceLockable(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return false
        }

        thread_type := ctx.GQL.ThreadType

        value_type := reflect.TypeOf(p.Value)

        if value_type.Implements(thread_type) {
          return true
        }

        return false
      },
      Fields: graphql.Fields{},
    })
    gql_type_base_thread.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeID,
    })

    gql_type_base_thread.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeName,
    })

    gql_type_base_thread.AddFieldConfig("Children", &graphql.Field{
      Type: GQLListThread(),
      Resolve: GQLThreadChildren,
    })

    gql_type_base_thread.AddFieldConfig("Parent", &graphql.Field{
      Type: GQLInterfaceThread(),
      Resolve: GQLThreadParent,
    })

    gql_type_base_thread.AddFieldConfig("Requirements", &graphql.Field{
      Type: GQLListLockable(),
      Resolve: GQLLockableRequirements,
    })

    gql_type_base_thread.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceLockable(),
      Resolve: GQLLockableOwner,
    })

    gql_type_base_thread.AddFieldConfig("Dependencies", &graphql.Field{
      Type: GQLListLockable(),
      Resolve: GQLLockableDependencies,
    })
  }
  return gql_type_base_thread
}

var gql_type_base_lockable *graphql.Object = nil
func GQLTypeBaseLockable() * graphql.Object {
  if gql_type_base_lockable == nil {
    gql_type_base_lockable = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseLockable",
      Interfaces: []*graphql.Interface{
        GQLInterfaceGraphNode(),
        GQLInterfaceLockable(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return false
        }

        lockable_type := ctx.GQL.LockableType
        value_type := reflect.TypeOf(p.Value)

        if value_type.Implements(lockable_type) {
          return true
        }

        return false
      },
      Fields: graphql.Fields{},
    })

    gql_type_base_lockable.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeID,
    })

    gql_type_base_lockable.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeName,
    })

    gql_type_base_lockable.AddFieldConfig("Requirements", &graphql.Field{
      Type: GQLListLockable(),
      Resolve: GQLLockableRequirements,
    })

    gql_type_base_lockable.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceLockable(),
      Resolve: GQLLockableOwner,
    })

    gql_type_base_lockable.AddFieldConfig("Dependencies", &graphql.Field{
      Type: GQLListLockable(),
      Resolve: GQLLockableDependencies,
    })
  }
  return gql_type_base_lockable
}

var gql_type_base_node *graphql.Object = nil
func GQLTypeBaseNode() * graphql.Object {
  if gql_type_base_node == nil {
    gql_type_base_node = graphql.NewObject(graphql.ObjectConfig{
      Name: "BaseNode",
      Interfaces: []*graphql.Interface{
        GQLInterfaceGraphNode(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return false
        }

        node_type := ctx.GQL.NodeType
        value_type := reflect.TypeOf(p.Value)

        if value_type.Implements(node_type) {
          return true
        }

        return false
      },
      Fields: graphql.Fields{},
    })

    gql_type_base_node.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeID,
    })

    gql_type_base_node.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLNodeName,
    })
  }

  return gql_type_base_node
}

func GQLSignalFn(p graphql.ResolveParams, fn func(GraphSignal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
    if signal, ok := p.Source.(GraphSignal); ok {
      return fn(signal, p)
    }
    return nil, fmt.Errorf("Failed to cast source to event")
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

func GQLSignalDirection(p graphql.ResolveParams) (interface{}, error) {
    return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
      direction := signal.Direction()
      if direction == Up {
        return "up", nil
      } else if direction == Down {
        return "down", nil
      } else if direction == Direct {
        return "direct", nil
      }
      return nil, fmt.Errorf("Invalid direction: %+v", direction)
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
    gql_type_signal.AddFieldConfig("Direction", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLSignalDirection,
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
      DefaultValue: "cancel",
    })
    gql_type_signal_input.AddFieldConfig("Direction", &graphql.InputObjectFieldConfig{
      Type: graphql.String,
      DefaultValue: "down",
    })
  }
  return gql_type_signal_input
}

func GQLSubscribeSignal(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, false, func(ctx *GraphContext, server *GQLThread, signal GraphSignal, p graphql.ResolveParams)(interface{}, error) {
    return signal, nil
  })
}

func GQLSubscribeSelf(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, true, func(ctx *GraphContext, server *GQLThread, signal GraphSignal, p graphql.ResolveParams)(interface{}, error) {
    return server, nil
  })
}

func GQLSubscribeFn(p graphql.ResolveParams, send_nil bool, fn func(*GraphContext, *GQLThread, GraphSignal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
  server, ok := p.Context.Value("gql_server").(*GQLThread)
  if ok == false {
    return nil, fmt.Errorf("Failed to get gql_server from context and cast to GQLServer")
  }

  ctx, ok := p.Context.Value("graph_context").(*GraphContext)
  if ok == false {
    return nil, fmt.Errorf("Failed to get graph_context from context and cast to GraphContext")
  }

  c := make(chan interface{})
  go func(c chan interface{}, server *GQLThread) {
    ctx.Log.Logf("gqlws", "GQL_SUBSCRIBE_THREAD_START")
    sig_c := server.UpdateChannel(1)
    if send_nil == true {
      sig_c <- nil
    }
    for {
      val, ok := <- sig_c
      if ok == false {
        return
      }
      ret, err := fn(ctx, server, val, p)
      if err != nil {
        ctx.Log.Logf("gqlws", "type convertor error %s", err)
        return
      }
      c <- ret
    }
  }(c, server)
  return c, nil
}

var gql_subscription_self * graphql.Field = nil
func GQLSubscriptionSelf() * graphql.Field {
  if gql_subscription_self == nil {
    gql_subscription_self = &graphql.Field{
      Type: GQLTypeGQLThread(),
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        return p.Source, nil
      },
      Subscribe: GQLSubscribeSelf,
    }
  }

  return gql_subscription_self
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

var gql_mutation_send_update *graphql.Field = nil
func GQLMutationSendUpdate() *graphql.Field {
  if gql_mutation_send_update == nil {
    gql_mutation_send_update = &graphql.Field{
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
        server, ok := p.Context.Value("gql_server").(*GQLThread)
        if ok == false {
          return nil, fmt.Errorf("Failed to cast context gql_server to GQLServer: %+v", p.Context.Value("gql_server"))
        }

        ctx, ok := p.Context.Value("graph_context").(*GraphContext)
        if ok == false {
          return nil, fmt.Errorf("Failed to cast context graph_context to GraphContext: %+v", p.Context.Value("graph_context"))
        }

        signal_map, ok := p.Args["signal"].(map[string]interface{})
        if ok == false {
          return nil, fmt.Errorf("Failed to cast arg signal to GraphSignal: %+v", p.Args["signal"])
        }
        var signal GraphSignal = nil
        if signal_map["Direction"] == "up" {
          signal = NewSignal(server, signal_map["Type"].(string))
        } else if signal_map["Direction"] == "down" {
          signal = NewDownSignal(server, signal_map["Type"].(string))
        } else if signal_map["Direction"] == "direct" {
          signal = NewDirectSignal(server, signal_map["Type"].(string))
        } else {
          return nil, fmt.Errorf("Bad direction: %d", signal_map["Direction"])
        }

        id , ok := p.Args["id"].(string)
        if ok == false {
          return nil, fmt.Errorf("Failed to cast arg id to string")
        }

        var node GraphNode = nil
        err := UseStates(ctx, []GraphNode{server}, func(states NodeStateMap) (error){
          node = FindChild(ctx, server, NodeID(id), states)
          if node == nil {
            return fmt.Errorf("Failed to find ID: %s as child of server thread", id)
          }
          SendUpdate(ctx, node, signal, states)
          return nil
        })
        if err != nil {
          return nil, err
        }

        return signal, nil
      },
    }
  }

  return gql_mutation_send_update
}

var gql_query_self *graphql.Field = nil
func GQLQuerySelf() *graphql.Field {
  if gql_query_self == nil {
    gql_query_self = &graphql.Field{
      Type: GQLTypeGQLThread(),
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        server, ok := p.Context.Value("gql_server").(*GQLThread)
        if ok == false {
          return nil, fmt.Errorf("failed to cast gql_server to GQLThread")
        }

        return server, nil
      },
    }
  }

  return gql_query_self
}
