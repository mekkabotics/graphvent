package graphvent

import (
  "github.com/graphql-go/graphql"
)

func NewField(init func()*graphql.Field) *graphql.Field {
  return init()
}

type Singleton[K graphql.Type] struct {
  Type K
  List *graphql.List
}

func NewSingleton[K graphql.Type](init func() K, post_init func(K, *graphql.List)) *Singleton[K] {
  val := init()
  list := graphql.NewList(val)
  if post_init != nil {
    post_init(val, list)
  }
  return &Singleton[K]{
    Type: val,
    List: list,
  }
}

func NodeInterfaceDefaultIsType(extensions []ExtType) func(graphql.IsTypeOfParams) bool {
  return func(p graphql.IsTypeOfParams) bool {
    node, ok := p.Value.(NodeResult)
    if ok == false {
      return false
    }

    for _, ext := range(extensions) {
      _, has := node.Result.Extensions[ext]
      if has == false {
        return false
      }
    }

    return true
  }
}

func NodeInterfaceResolveType(required_extensions []ExtType, default_type **graphql.Object)func(graphql.ResolveTypeParams) *graphql.Object {
  return func(p graphql.ResolveTypeParams) *graphql.Object {
    ctx, ok := p.Context.Value("resolve").(*ResolveContext)
    if ok == false {
      return nil
    }

    node, ok := p.Value.(NodeResult)
    if ok == false {
      return nil
    }

    gql_type, exists := ctx.GQLContext.NodeTypes[node.Result.NodeType]
    ctx.Context.Log.Logf("gql", "GQL_INTERFACE_RESOLVE_TYPE(%+v): %+v - %t - %+v - %+v", node, gql_type, exists, required_extensions, *default_type)
    if exists == false {
      node_type_def, exists := ctx.Context.Types[Hash(node.Result.NodeType)]
      if exists == false {
        return nil
      } else {
        for _, ext := range(required_extensions) {
          found := false
          for _, e := range(node_type_def.Extensions) {
            if e == ext {
              found = true
              break
            }
          }
          if found == false {
            return nil
          }
        }
      }
      return *default_type
    }

    return gql_type
  }
}
