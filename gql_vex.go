package main

import (
  "github.com/graphql-go/graphql"
  "reflect"
  "fmt"
  "errors"
)

func GQLVexTypes() map[reflect.Type]*graphql.Object {
  types := map[reflect.Type]*graphql.Object{}
  types[reflect.TypeOf((*Match)(nil))] = GQLVexTypeMatch()

  return types
}

func GQLVexMutations() map[string]*graphql.Field {
  mutations := map[string]*graphql.Field{}
  mutations["setMatchState"] = GQLVexMutationSetMatchState()
  return mutations
}

func GQLVexQueries() map[string]*graphql.Field {
  queries := map[string]*graphql.Field{}
  queries["Arenas"] = GQLVexQueryArenas()
  return queries
}

func GQLVexSubscriptions() map[string]*graphql.Field {
  subs := map[string]*graphql.Field{}
  subs["Arena"] = GQLVexSubscriptionArena()
  return subs
}

var gql_vex_subscription_arena *graphql.Field = nil
func GQLVexSubscriptionArena() *graphql.Field {
  if gql_vex_subscription_arena == nil {
    gql_vex_subscription_arena = &graphql.Field{
      Type: GQLVexTypeArena(),
      Args: graphql.FieldConfigArgument{
        "arena_id": &graphql.ArgumentConfig{
          Type: graphql.String,
        },
      },
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        return p.Source, nil
      },
      Subscribe: GQLVexSubscribeArena,
    }
  }
  return gql_vex_subscription_arena
}

func GQLVexSubscribeArena(p graphql.ResolveParams)(interface{}, error) {
  server, ok := p.Context.Value("gql_server").(*GQLServer)
  if ok == false {
    return nil, fmt.Errorf("Failed to get gql_Server from context and cast to GQLServer")
  }

  c := make(chan interface{})
  arena_id, ok := p.Args["arena_id"].(string)
  if ok == false {
    return nil, fmt.Errorf("Failed to get arena_id arg")
  }
  owner, ok := server.Owner().(Event)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast owner to event")
  }
  resource := FindRequiredResource(owner, arena_id)
  if resource == nil {
    return nil, fmt.Errorf("Failed to find resource under owner")
  }
  arena, ok := resource.(Arena)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast resource to arena")
  }

  sig_c := arena.UpdateChannel()
  go func(c chan interface{}, sig_c chan GraphSignal, arena Arena) {
    c <- arena
    for {
      _, ok := <- sig_c
      if ok == false {
        return
      }
      c <- arena
    }
  }(c, sig_c, arena)
  return c, nil
}

var gql_vex_mutation_set_match_state *graphql.Field= nil
func GQLVexMutationSetMatchState() *graphql.Field {
  if gql_vex_mutation_set_match_state == nil {
    gql_vex_mutation_set_match_state = &graphql.Field{
      Type: GQLTypeSignal(),
      Args: graphql.FieldConfigArgument{
        "id": &graphql.ArgumentConfig{
          Type: graphql.String,
        },
        "state": &graphql.ArgumentConfig{
          Type: graphql.String,
        },
        "time": &graphql.ArgumentConfig{
          Type: graphql.DateTime,
        },
      },
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        server, ok := p.Context.Value("gql_server").(*GQLServer)
        if ok == false {
          return nil, fmt.Errorf("Failed to cast context gql_server to GQLServer: %+v", p.Context.Value("gql_server"))
        }

        id, ok := p.Args["id"].(string)
        if ok == false {
          return nil, errors.New("Failed to cast arg id to string")
        }

        state, ok := p.Args["state"].(string)
        if ok == false {
          return nil, errors.New("Failed to cast arg state to string")
        }

        signal := NewDownSignal(server, state)

        owner := server.Owner()
        if owner == nil {
          return nil, errors.New("Cannot send update without owner")
        }

        root_event, ok := owner.(Event)
        if ok == false {
          return nil, errors.New("Cannot send update to Event unless owned by an Event")
        }

        node := FindChild(root_event, id)
        if node == nil {
          return nil, errors.New("Failed to find id in event tree from server")
        }

        SendUpdate(node, signal)

        return signal, nil
      },
    }
  }
  return gql_vex_mutation_set_match_state
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

func GQLVexMatchStateStart(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams) (interface{}, error) {
    return event.(*Match).control_start, nil
  })
}

func GQLVexMatchStateDuration(p graphql.ResolveParams) (interface{}, error) {
  return GQLEventFn(p, func(event Event, p graphql.ResolveParams) (interface{}, error) {
    return event.(*Match).control_duration, nil
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
        GQLInterfaceNode(),
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

    gql_vex_type_arena.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceNode(),
      Resolve: GQLResourceOwner,
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
        GQLInterfaceNode(),
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

    gql_vex_type_match.AddFieldConfig("StateStart", &graphql.Field{
      Type: graphql.DateTime,
      Resolve: GQLVexMatchStateStart,
    })

    gql_vex_type_match.AddFieldConfig("StateDuration", &graphql.Field{
      Type: graphql.Int,
      Resolve: GQLVexMatchStateDuration,
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
        GQLInterfaceNode(),
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

    gql_vex_type_alliance.AddFieldConfig("Owner", &graphql.Field{
      Type: GQLInterfaceNode(),
      Resolve: GQLResourceOwner,
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
        GQLInterfaceNode(),
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

  gql_vex_type_team.AddFieldConfig("Owner", &graphql.Field{
    Type: GQLInterfaceNode(),
    Resolve: GQLResourceOwner,
  })

  return gql_vex_type_team
}
