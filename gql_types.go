package graphvent

import (
  "github.com/graphql-go/graphql"
)

func AddNodeFields(obj *graphql.Object) {
  obj.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  obj.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeTypeHash,
  })
}

func AddLockableFields(obj *graphql.Object) {
  AddNodeFields(obj)

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
  AddNodeFields(obj)

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

var GQLTypeBaseThread = NewSingleton(func() *graphql.Object {
  gql_type_simple_thread := graphql.NewObject(graphql.ObjectConfig{
    Name: "SimpleThread",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceThread.Type,
      GQLInterfaceLockable.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      node, ok := p.Value.(*Node)
      if ok == false {
        return false
      }

      _, err := GetExt[*ThreadExt](node)
      return err == nil
    },
    Fields: graphql.Fields{},
  })

  AddThreadFields(gql_type_simple_thread)

  return gql_type_simple_thread
}, nil)

var GQLTypeBaseLockable = NewSingleton(func() *graphql.Object {
  gql_type_simple_lockable := graphql.NewObject(graphql.ObjectConfig{
    Name: "SimpleLockable",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
      GQLInterfaceLockable.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      node, ok := p.Value.(*Node)
      if ok == false {
        return false
      }

      _, err := GetExt[*LockableExt](node)
      return err == nil
    },
    Fields: graphql.Fields{},
  })

  AddLockableFields(gql_type_simple_lockable)

  return gql_type_simple_lockable
}, nil)

var GQLTypeBaseNode = NewSingleton(func() *graphql.Object {
  object := graphql.NewObject(graphql.ObjectConfig{
    Name: "SimpleNode",
    Interfaces: []*graphql.Interface{
      GQLInterfaceNode.Type,
    },
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      _, ok := p.Value.(*Node)
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

