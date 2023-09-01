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

func NewGQLNodeType(gql_name string, node_type NodeType, interfaces []*graphql.Interface, init func(*Type)) *Type {
  var gql Type
  gql.Type = graphql.NewObject(graphql.ObjectConfig{
    Name: gql_name,
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
