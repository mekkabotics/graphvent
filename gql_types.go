package graphvent

import (
  "github.com/graphql-go/graphql"
)

func AddNodeFields(obj *graphql.Object) {
  obj.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })
}

func AddLockableFields(obj *graphql.Object) {
  AddNodeFields(obj)

  obj.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLLockableName,
  })

  obj.AddFieldConfig("Requirements", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableRequirements,
  })

  obj.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceLockable.Type,
    Resolve: GQLLockableOwner,
  })

  obj.AddFieldConfig("Dependencies", &graphql.Field{
    Type: GQLInterfaceLockable.List,
    Resolve: GQLLockableDependencies,
  })
}

func AddThreadFields(obj *graphql.Object) {
  AddLockableFields(obj)

  obj.AddFieldConfig("State", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLThreadState,
  })

  obj.AddFieldConfig("Children", &graphql.Field{
    Type: GQLInterfaceThread.List,
    Resolve: GQLThreadChildren,
  })

  obj.AddFieldConfig("Parent", &graphql.Field{
    Type: GQLInterfaceThread.Type,
    Resolve: GQLThreadParent,
  })
}

var GQLTypeUser = NewSingleton(func() *graphql.Object {
  gql_type_user := graphql.NewObject(graphql.ObjectConfig{
    Name: "User",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceLockable.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      _, ok := p.Value.(*User)
      return ok
    },
    Fields: graphql.Fields{},
  })

  AddLockableFields(gql_type_user)

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

  AddThreadFields(gql_type_gql_thread)

  gql_type_gql_thread.AddFieldConfig("Users", &graphql.Field{
    Type: GQLTypeUser.List,
    Resolve: GQLUserNodeUsers,
  })

  gql_type_gql_thread.AddFieldConfig("Listen", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLThreadListen,
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
      _, ok := p.Value.(Thread)
      return ok
    },
    Fields: graphql.Fields{},
  })

  AddThreadFields(gql_type_simple_thread)

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
      _, ok := p.Value.(Lockable)
      return ok
    },
    Fields: graphql.Fields{},
  })

  AddLockableFields(gql_type_simple_lockable)

  return gql_type_simple_lockable
}, nil)

var GQLTypeSimpleNode = NewSingleton(func() *graphql.Object {
  object := graphql.NewObject(graphql.ObjectConfig{
    Name: "SimpleNode",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      _, ok := p.Value.(Node)
      return ok
    },
    Fields: graphql.Fields{},
  })

  AddNodeFields(object)

  return object
}, nil)

var GQLTypeSignal = NewSingleton(func() *graphql.Object {
  gql_type_signal := graphql.NewObject(graphql.ObjectConfig{
    Name: "Signal",
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

