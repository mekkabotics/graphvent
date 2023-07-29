package graphvent
import (
  "github.com/graphql-go/graphql"
)

var GQLQueryNode = &graphql.Field{
  Type: GQLInterfaceNode.Interface,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }
    ctx.Context.Log.Logf("gql", "FieldASTs: %+v", p.Info.FieldASTs)
    // Get a list of fields that will be written
    // Send the read signal
    // Wait for the response, returning an error on timeout

    return nil, nil
  },
}

var GQLQuerySelf = &graphql.Field{
  Type: GQLInterfaceNode.Default,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
   _, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    return nil, nil
  },
}

