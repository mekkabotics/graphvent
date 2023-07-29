package graphvent
import (
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/ast"
)

func GetFieldNames(p graphql.ResolveParams) []string {
  names := []string{}

  for _, node := range(p.Info.FieldASTs) {
    for _, sel := range(node.SelectionSet.Selections) {
      names = append(names, sel.(*ast.Field).Name.Value)
    }
  }

  return names
}

var QueryNode = &graphql.Field{
  Type: InterfaceNode.Interface,
  Args: graphql.FieldConfigArgument{
    "id": &graphql.ArgumentConfig{
      Type: graphql.String,
    },
  },
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }
    ctx.Context.Log.Logf("gql", "FIELDS: %+v", GetFieldNames(p))
    // Get a list of fields that will be written
    // Send the read signal
    // Wait for the response, returning an error on timeout

    return nil, nil
  },
}

var QuerySelf = &graphql.Field{
  Type: InterfaceNode.Default,
  Resolve: func(p graphql.ResolveParams) (interface{}, error) {
    ctx, err := PrepResolve(p)
    if err != nil {
      return nil, err
    }

    ctx.Context.Log.Logf("gql", "FIELDS: %+v", GetFieldNames(p))

    return nil, nil
  },
}

