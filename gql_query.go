package graphvent
import (
  "fmt"
  "github.com/graphql-go/graphql"
)

var gql_query_self *graphql.Field = nil
func GQLQuerySelf() *graphql.Field {
  if gql_query_self == nil {
    gql_query_self = &graphql.Field{
      Type: GQLTypeGQLThread(),
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        _, server, user, err := PrepResolve(p)

        if err != nil {
          return nil, err
        }

        err = server.Allowed("enumerate", "self", user)
        if err != nil {
          return nil, fmt.Errorf("User %s is not allowed to perform self.enumerate on %s", user.ID(), server.ID())
        }

        return server, nil
      },
    }
  }

  return gql_query_self
}
