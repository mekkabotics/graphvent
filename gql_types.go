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

var NodeInterfaces = []*Interface{InterfaceNode}
var LockableInterfaces = append(NodeInterfaces, InterfaceLockable)

func NewGQLNodeType(node_type NodeType, interfaces []*graphql.Interface, init func(*Type)) *Type {
  var gql Type
  gql.Type = graphql.NewObject(graphql.ObjectConfig{
    Name: string(node_type),
    Interfaces: interfaces,
    IsTypeOf: func(p graphql.IsTypeOfParams) bool {
      node, ok := p.Value.(NodeResult)
      if ok == false {
        return false
      }
      return node.Result.NodeType == node_type
    },
    Fields: graphql.Fields{},
  })
  gql.List = graphql.NewList(gql.Type)

  init(&gql)
  return &gql
}

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

