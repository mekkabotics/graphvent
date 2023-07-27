package graphvent

import (
  "github.com/graphql-go/graphql"
)

func AddNodeFields(object *graphql.Object) {
  object.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  object.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeTypeHash,
  })
}

func AddLockableFields(object *graphql.Object) {
  addLockableFields(object, GQLInterfaceLockable.Interface, GQLInterfaceLockable.List)
}

func addLockableFields(object *graphql.Object, lockable_interface *graphql.Interface, lockable_list *graphql.List) {
  AddNodeFields(object)
  object.AddFieldConfig("Requirements", &graphql.Field{
    Type: lockable_list,
    Resolve: GQLLockableRequirements,
  })

  object.AddFieldConfig("Owner", &graphql.Field{
    Type: lockable_interface,
    Resolve: GQLLockableOwner,
  })

  object.AddFieldConfig("Dependencies", &graphql.Field{
    Type: lockable_list,
    Resolve: GQLLockableDependencies,
  })
}

var GQLNodeInterfaces = []*graphql.Interface{GQLInterfaceNode.Interface}
var GQLLockableInterfaces = append(GQLNodeInterfaces, GQLInterfaceLockable.Interface)

var GQLTypeGQLNode = NewGQLNodeType(GQLNodeType, GQLNodeInterfaces, func(gql *GQLType) {
  AddNodeFields(gql.Type)

  gql.Type.AddFieldConfig("Listen", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeListen,
  })
})

var GQLTypeSignal = NewSingleton(func() *graphql.Object {
  gql_type_signal := graphql.NewObject(graphql.ObjectConfig{
    Name: "Signal",
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      _, ok := p.Value.(Signal)
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

