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

var gql_vex_list_team * graphql.List = nil
func GQLVexListTeam() * graphql.List {
  if gql_vex_list_team == nil {
    gql_vex_list_team = graphql.NewList(GQLVexTypeTeam())
  }
  return gql_vex_list_team
}

var gql_vex_list_alliance * graphql.List = nil
func GQLVexListAlliance() * graphql.List {
  if gql_vex_list_alliance == nil {
    gql_vex_list_alliance = graphql.NewList(GQLVexTypeAlliance())
  }
  return gql_vex_list_alliance
}

func GQLVexMatchAlliances(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams) (interface{}, error) {
    //TODO improve
    return event.(*Match).alliances, nil
  })
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

    gql_vex_type_match.AddFieldConfig("Alliances", &graphql.Field{
      Type: GQLVexListAlliance(),
      Resolve: GQLVexMatchAlliances,
    })
  }

  return gql_vex_type_match
}

var gql_vex_type_alliance * graphql.Object = nil
func GQLVexTypeAlliance() * graphql.Object {
  if gql_vex_type_alliance == nil {
    gql_vex_type_alliance = graphql.NewObject(graphql.ObjectConfig{
      Name: "Alliance",
      Interfaces: []*graphql.Interface{
        GQLInterfaceResource(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*Alliance)
        return ok
      },
      Fields: graphql.Fields{},
    })
  }

  gql_vex_type_alliance.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLResourceID,
  })

  gql_vex_type_alliance.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLResourceName,
  })

  gql_vex_type_alliance.AddFieldConfig("Description", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLResourceDescription,
  })

  gql_vex_type_alliance.AddFieldConfig("Parents", &graphql.Field{
    Type: GQLListResource(),
    Resolve: GQLResourceParents,
  })

  return gql_vex_type_alliance
}

var gql_vex_type_team * graphql.Object = nil
func GQLVexTypeTeam() * graphql.Object {
  if gql_vex_type_team == nil {
    gql_vex_type_team = graphql.NewObject(graphql.ObjectConfig{
      Name: "Team",
      Interfaces: []*graphql.Interface{
        GQLInterfaceResource(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(*Team)
        return ok
      },
      Fields: graphql.Fields{},
    })
  }

  gql_vex_type_team.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLResourceID,
  })

  gql_vex_type_team.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLResourceName,
  })

  gql_vex_type_team.AddFieldConfig("Description", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLResourceDescription,
  })

  gql_vex_type_team.AddFieldConfig("Parents", &graphql.Field{
    Type: GQLListResource(),
    Resolve: GQLResourceParents,
  })

  return gql_vex_type_team
}
