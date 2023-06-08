package main

import (
  "github.com/graphql-go/graphql"
  "reflect"
)

func GQLVexTypes() map[reflect.Type]*graphql.Object {
  types := map[reflect.Type]*graphql.Object{}
  types[reflect.TypeOf((*Match)(nil))] = GQLVexTypeMatch()

  return types
}

var gql_vex_type_match * graphql.Object = nil
func GQLVexTypeMatch() * graphql.Object {
  if gql_vex_type_match == nil {
    gql_vex_type_match = graphql.NewObject(graphql.ObjectConfig{
      Name: "Match",
      Interfaces: []*graphql.Interface{
        GQLInterfaceEvent(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*Match)
        return ok
      },
      Fields: graphql.Fields{},
    })

    gql_vex_type_match.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventID,
    })

    gql_vex_type_match.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventName,
    })

    gql_vex_type_match.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLEventDescription,
    })

    gql_vex_type_match.AddFieldConfig("Children", &graphql.Field{
      Type: GQLListEvent(),
      Resolve: GQLEventChildren,
    })
  }

  return gql_vex_type_match
}
