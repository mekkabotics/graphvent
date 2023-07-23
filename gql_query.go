package graphvent
import (
  "github.com/graphql-go/graphql"
)

var GQLQuerySelf = &graphql.Field{
  Type: GQLTypeGQLThread.Type,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    err = ctx.Server.Allowed("read", "", ctx.User)
    if err != nil {
      return nil, err
    }

    return ctx.Server, nil
  },
}

var GQLQueryUser = &graphql.Field{
  Type: GQLTypeUser.Type,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    err = ctx.User.Allowed("read", "", ctx.User)
    if err != nil {
      return nil, err
    }

    return ctx.User, nil
  },
}
