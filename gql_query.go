package graphvent
import (
  "github.com/graphql-go/graphql"
)

var GQLQuerySelf = &graphql.Field{
  Type: GQLTypeGQLThread.Type,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    _, server, user, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    err = server.Allowed("enumerate", "self", user)
    if err != nil {
      return nil, err
    }

    return server, nil
  },
}

var GQLQueryUser = &graphql.Field{
  Type: GQLTypeUser.Type,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    _, _, user, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    err = user.Allowed("enumerate", "self", user)
    if err != nil {
      return nil, err
    }

    return user, nil
  },
}
