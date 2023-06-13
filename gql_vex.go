package main

import (
  "github.com/graphql-go/graphql"
  "reflect"
  "fmt"
)

func GQLVexTypes() map[reflect.Type]*graphql.Object {
  types := map[reflect.Type]*graphql.Object{}
  types[reflect.TypeOf((*Match)(nil))] = GQLVexTypeMatch()

  return types
}

func GQLVexMutations() map[string]*graphql.Field {
  mutations := map[string]*graphql.Field{}
  return mutations
}

func GQLVexQueries() map[string]*graphql.Field {
  queries := map[string]*graphql.Field{}
  queries["Arenas"] = GQLVexQueryArenas()
  return queries
}

func FindResources(event Event, resource_type reflect.Type) []Resource {
  resources := event.RequiredResources()
  found := []Resource{}
  for _, resource := range(resources) {
    if reflect.TypeOf(resource) == resource_type {
      found = append(found, resource)
    }
  }

  for _, child := range(event.Children()) {
    found = append(found, FindResources(child, resource_type)...)
  }

  m := map[string]Resource{}
  for _, resource := range(found) {
    m[resource.ID()] = resource
  }
  ret := []Resource{}
  for _, resource := range(m) {
    ret = append(ret, resource)
  }
  return ret
}

var gql_vex_query_arenas *graphql.Field = nil
func GQLVexQueryArenas() *graphql.Field {
  if gql_vex_query_arenas == nil {
    gql_vex_query_arenas = &graphql.Field{
      Type: GQLVexListArena(),
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        server, ok := p.Context.Value("gql_server").(*GQLServer)
        if ok == false {
          panic("Failed to get/cast gql_server from context")
        }

        owner, is_event := server.Owner().(Event)
        if is_event == false {
          return nil, fmt.Errorf("Can't enumerate arenas when server is attached to resource")
        }
        return FindResources(owner, reflect.TypeOf((*VirtualArena)(nil))), nil
      },
    }
  }
  return gql_vex_query_arenas
}

var gql_vex_list_arena * graphql.List = nil
func GQLVexListArena() * graphql.List {
  if gql_vex_list_arena == nil {
    gql_vex_list_arena = graphql.NewList(GQLVexTypeArena())
  }
  return gql_vex_list_arena
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
    return event.(*Match).alliances, nil
  })
}

func GQLVexMatchArena(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams) (interface{}, error) {
    return event.(*Match).arena, nil
  })
}

func GQLVexMatchControl(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams) (interface{}, error) {
    return event.(*Match).control, nil
  })
}

func GQLVexMatchState(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams) (interface{}, error) {
    return event.(*Match).state, nil
  })
}

func GQLVexAllianceTeams(p graphql.ResolveParams) (interface{}, error) {
  return GQLResourceFn(p, func(resource Resource, p graphql.ResolveParams) (interface{}, error) {
    return resource.(*Alliance).teams, nil
  })
}

var gql_vex_type_arena * graphql.Object = nil
func GQLVexTypeArena() * graphql.Object {
  if gql_vex_type_arena == nil {
    gql_vex_type_arena = graphql.NewObject(graphql.ObjectConfig{
      Name: "Arena",
      Interfaces: []*graphql.Interface{
        GQLInterfaceResource(),
      },
      IsTypeOf: func(p graphql.IsTypeOfParams) bool {
        _, ok := p.Value.(Arena)
        return ok
      },
      Fields: graphql.Fields{},
    })

    gql_vex_type_arena.AddFieldConfig("ID", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceID,
    })

    gql_vex_type_arena.AddFieldConfig("Name", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceName,
    })

    gql_vex_type_arena.AddFieldConfig("Description", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLResourceDescription,
    })

    gql_vex_type_arena.AddFieldConfig("Parents", &graphql.Field{
      Type: GQLListResource(),
      Resolve: GQLResourceParents,
    })
  }

  return gql_vex_type_arena
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

    gql_vex_type_match.AddFieldConfig("Arena", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLVexMatchArena,
    })

    gql_vex_type_match.AddFieldConfig("Control", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLVexMatchControl,
    })

    gql_vex_type_match.AddFieldConfig("State", &graphql.Field{
      Type: graphql.String,
      Resolve: GQLVexMatchState,
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

    gql_vex_type_alliance.AddFieldConfig("Teams", &graphql.Field{
      Type: GQLVexListTeam(),
      Resolve: GQLVexAllianceTeams,
    })
  }

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
