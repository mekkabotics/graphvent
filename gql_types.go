package graphvent

import (
  "github.com/graphql-go/graphql"
)

func AddNodeFields(object *graphql.Object) {
  object.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: ResolveNodeID,
  })

  object.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
    Resolve: ResolveNodeTypeHash,
  })
}

func AddLockableFields(object *graphql.Object) {
  addLockableFields(object, InterfaceLockable.Interface, InterfaceLockable.List)
}

func addLockableFields(object *graphql.Object, lockable_interface *graphql.Interface, lockable_list *graphql.List) {
  AddNodeFields(object)
  object.AddFieldConfig("Requirements", &graphql.Field{
    Type: lockable_list,
    Resolve: ResolveRequirements,
  })

  object.AddFieldConfig("Owner", &graphql.Field{
    Type: lockable_interface,
    Resolve: ResolveOwner,
  })

  object.AddFieldConfig("Dependencies", &graphql.Field{
    Type: lockable_list,
    Resolve: ResolveDependencies,
  })
}

var GQLNodeInterfaces = []*graphql.Interface{InterfaceNode.Interface}
var GQLLockableInterfaces = append(GQLNodeInterfaces, InterfaceLockable.Interface)

var TypeGQLNode = NewGQLNodeType(GQLNodeType, GQLNodeInterfaces, func(gql *Type) {
  AddNodeFields(gql.Type)

  gql.Type.AddFieldConfig("Listen", &graphql.Field{
    Type: graphql.String,
    Resolve: ResolveListen,
  })
})

var TypeSignal = NewSingleton(func() *graphql.Object {
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

