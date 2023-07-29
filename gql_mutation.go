package graphvent
import (
  "github.com/graphql-go/graphql"
)

var MutationStop = NewField(func()*graphql.Field {
  mutation_stop := &graphql.Field{
    Type: TypeSignal.Type,
    Args: graphql.FieldConfigArgument{
      "id": &graphql.ArgumentConfig{
        Type: graphql.String,
      },
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      return StopSignal, nil
    },
  }

  return mutation_stop
})

