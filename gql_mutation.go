package graphvent
import (
  "fmt"
  "github.com/graphql-go/graphql"
)

var GQLMutationAbort = NewField(func()*graphql.Field {
  gql_mutation_abort := &graphql.Field{
    Type: GQLTypeSignal.Type,
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

      err = ctx.Server.Allowed("signal", "", ctx.User)
      if err != nil {
        return nil, err
      }

      id, err := ExtractID(p, "id")
      if err != nil {
        return nil, err
      }

      var node Node = nil
      err = UseStates(ctx.Context, ctx.User, NewLockMap(
        NewLockInfo(ctx.Server, []string{"children"}),
      ), func(context *ReadContext) (error){
        node = FindChild(ctx.Context, ctx.User, ctx.Server, id, locked)
        if node == nil {
          return fmt.Errorf("Failed to find ID: %s as child of server thread", id)
        }
        return UseMoreStates(ctx.Context, locked, ctx.User, NewLockInfo(node, []string{"signal"}), func(locked NodeLockMap) error {
          return node.Signal(ctx.Context, AbortSignal, locked)
        })
      })
      if err != nil {
        return nil, err
      }

      return AbortSignal, nil
    },
  }

  return gql_mutation_abort
})

var GQLMutationStartChild = NewField(func()*graphql.Field{
  gql_mutation_start_child := &graphql.Field{
    Type: GQLTypeSignal.Type,
    Args: graphql.FieldConfigArgument{
      "parent_id": &graphql.ArgumentConfig{
        Type: graphql.String,
      },
      "child_id": &graphql.ArgumentConfig{
        Type: graphql.String,
      },
      "action": &graphql.ArgumentConfig{
        Type: graphql.String,
        DefaultValue: "start",
      },
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      parent_id, err := ExtractID(p, "parent_id")
      if err != nil {
        return nil, err
      }

      child_id, err := ExtractID(p, "child_id")
      if err != nil {
        return nil, err
      }

      action, err := ExtractParam[string](p, "action")
      if err != nil {
        return nil, err
      }

      var signal GraphSignal
      err = UseStates(ctx.Context, ctx.User, NewLockMap(
        NewLockInfo(ctx.Server, []string{"children"}),
      ), func(context *ReadContext) error {
        node := FindChild(ctx.Context, ctx.User, ctx.Server, parent_id, locked)
        if node == nil {
          return fmt.Errorf("Failed to find ID: %s as child of server thread", parent_id)
        }

        err := node.Allowed("signal", "", ctx.User)
        if err != nil {
          return err
        }

        return UseMoreStates(ctx.Context, locked, ctx.User, NewLockInfo(node, []string{"start_child", "signal"}), func(locked NodeLockMap) error {
          signal = NewStartChildSignal(child_id, action)
          return node.Signal(ctx.Context, signal, locked)
        })
      })
      if err != nil {
        return nil, err
      }

      // TODO: wait for the result of the signal to send back instead of just the signal
      return signal, nil
    },
  }

  return gql_mutation_start_child
})

