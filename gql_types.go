package graphvent

import (
  "github.com/graphql-go/graphql"
  "reflect"
)

func NewField(init func()*graphql.Field) *graphql.Field {
  return init()
}

type Singleton[K graphql.Type] struct {
  Type K
  List *graphql.List
}

func NewSingleton[K graphql.Type](init func() K, post_init func(K, *graphql.List)) *Singleton[K] {
  val := init()
  list := graphql.NewList(val)
  if post_init != nil {
    post_init(val, list)
  }
  return &Singleton[K]{
    Type: val,
    List: list,
  }
}

var GQLInterfaceNode = NewSingleton(func() *graphql.Interface {
  i := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Node",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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
        return ctx.GQL.BaseNodeType
      }

      return nil
    },
    Fields: graphql.Fields{},
  })
  i.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
  })
  return i
}, nil)

var GQLInterfaceThread = NewSingleton(func() *graphql.Interface {
  gql_interface_thread := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Thread",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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
        return ctx.GQL.BaseThreadType
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

  gql_interface_thread.AddFieldConfig("State", &graphql.Field{
    Type: graphql.String,
  })

  gql_interface_thread.AddFieldConfig("Requirements", &graphql.Field{
    Type: GQLInterfaceLockable.List,
  })

  gql_interface_thread.AddFieldConfig("Dependencies", &graphql.Field{
    Type: GQLInterfaceLockable.List,
  })

  gql_interface_thread.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceLockable.Type,
  })

  return gql_interface_thread
}, func(thread *graphql.Interface, thread_list *graphql.List) {
  thread.AddFieldConfig("Children", &graphql.Field{
    Type: thread_list,
  })

  thread.AddFieldConfig("Parent", &graphql.Field{
    Type: thread,
  })

})

var GQLInterfaceLockable = NewSingleton(func() *graphql.Interface {
  gql_interface_lockable := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Lockable",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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
        return ctx.GQL.BaseThreadType
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
  return gql_interface_lockable
}, func(lockable *graphql.Interface, lockable_list *graphql.List) {
  lockable.AddFieldConfig("Requirements", &graphql.Field{
    Type: lockable_list,
  })

  lockable.AddFieldConfig("Dependencies", &graphql.Field{
    Type: lockable_list,
  })

  lockable.AddFieldConfig("Owner", &graphql.Field{
    Type: lockable,
  })
})

var GQLTypeUser = NewSingleton(func() *graphql.Object {
  gql_type_user := graphql.NewObject(graphql.ObjectConfig{
    Name: "User",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceLockable.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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

  gql_type_user.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  gql_type_user.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLLockableName,
  })

  gql_type_user.AddFieldConfig("Requirements", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableRequirements,
  })

  gql_type_user.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceLockable.Type,
    Resolve: GQLLockableOwner,
  })

  gql_type_user.AddFieldConfig("Dependencies", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableDependencies,
  })
  return gql_type_user
}, nil)

var GQLTypeGQLThread = NewSingleton(func() *graphql.Object {
  gql_type_gql_thread := graphql.NewObject(graphql.ObjectConfig{
    Name: "GQLThread",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceThread.Type,
      GQLInterfaceLockable.Type,
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
    Resolve: GQLLockableName,
  })

  gql_type_gql_thread.AddFieldConfig("State", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLThreadState,
  })

  gql_type_gql_thread.AddFieldConfig("Children", &graphql.Field{
    Type: GQLInterfaceThread.List,
    Resolve: GQLThreadChildren,
  })

  gql_type_gql_thread.AddFieldConfig("Parent", &graphql.Field{
    Type: GQLInterfaceThread.Type,
    Resolve: GQLThreadParent,
  })

  gql_type_gql_thread.AddFieldConfig("Listen", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLThreadListen,
  })

  gql_type_gql_thread.AddFieldConfig("Requirements", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableRequirements,
  })

  gql_type_gql_thread.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceLockable.Type,
    Resolve: GQLLockableOwner,
  })

  gql_type_gql_thread.AddFieldConfig("Dependencies", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableDependencies,
  })

  gql_type_gql_thread.AddFieldConfig("Users", &graphql.Field{
    Type: GQLTypeUser.List,
    Resolve: GQLThreadUsers,
  })
  return gql_type_gql_thread
}, nil)

var GQLTypeSimpleThread = NewSingleton(func() *graphql.Object {
  gql_type_simple_thread := graphql.NewObject(graphql.ObjectConfig{
    Name: "SimpleThread",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceThread.Type,
      GQLInterfaceLockable.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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
  gql_type_simple_thread.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  gql_type_simple_thread.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLLockableName,
  })

  gql_type_simple_thread.AddFieldConfig("State", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLThreadState,
  })

  gql_type_simple_thread.AddFieldConfig("Children", &graphql.Field{
    Type: GQLInterfaceThread.List,
    Resolve: GQLThreadChildren,
  })

  gql_type_simple_thread.AddFieldConfig("Parent", &graphql.Field{
    Type: GQLInterfaceThread.Type,
    Resolve: GQLThreadParent,
  })

  gql_type_simple_thread.AddFieldConfig("Requirements", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableRequirements,
  })

  gql_type_simple_thread.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceLockable.Type,
    Resolve: GQLLockableOwner,
  })

  gql_type_simple_thread.AddFieldConfig("Dependencies", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableDependencies,
  })

  return gql_type_simple_thread
}, nil)

var GQLTypeSimpleLockable = NewSingleton(func() *graphql.Object {
  gql_type_simple_lockable := graphql.NewObject(graphql.ObjectConfig{
    Name: "SimpleLockable",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceLockable.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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

  gql_type_simple_lockable.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  gql_type_simple_lockable.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLLockableName,
  })

  gql_type_simple_lockable.AddFieldConfig("Requirements", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableRequirements,
  })

  gql_type_simple_lockable.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceLockable.Type,
    Resolve: GQLLockableOwner,
  })

  gql_type_simple_lockable.AddFieldConfig("Dependencies", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableDependencies,
  })

  return gql_type_simple_lockable
}, nil)

var GQLTypeGraphNode = NewSingleton(func() *graphql.Object {
  object := graphql.NewObject(graphql.ObjectConfig{
    Name: "GraphNode",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      ctx, ok := p.Context.Value("graph_context").(*Context)
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

  object.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  object.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLLockableName,
  })

  return object
}, nil)

var GQLTypeSignal = NewSingleton(func() *graphql.Object {
  gql_type_signal := graphql.NewObject(graphql.ObjectConfig{
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
  return gql_type_signal
}, nil)

var GQLTypeSignalInput = NewSingleton(func()*graphql.InputObject {
  gql_type_signal_input := graphql.NewInputObject(graphql.InputObjectConfig{
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

  return gql_type_signal_input
}, nil)

