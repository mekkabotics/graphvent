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
      _, ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      id, err := ExtractID(p, "id")
      if err != nil {
        return nil, err
      }

      var node *Node = nil
      context := NewReadContext(ctx.Context)
      err = UseStates(context, ctx.User, NewACLMap(
        NewACLInfo(ctx.Server, []string{"children"}),
      ), func(context *StateContext) (error){
        node, err = FindChild(context, ctx.User, ctx.Server, id)
        if err != nil {
          return err
        }
        if node == nil {
          return fmt.Errorf("Failed to find ID: %s as child of server thread", id)
        }
        return node.Process(context, ctx.User.ID, AbortSignal)
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
      _, ctx, err := PrepResolve(p)
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

      var signal Signal
      context := NewWriteContext(ctx.Context)
      err = UseStates(context, ctx.User, NewACLMap(
        NewACLInfo(ctx.Server, []string{"children"}),
      ), func(context *StateContext) error {
        parent, err := FindChild(context, ctx.User, ctx.Server, parent_id)
        if err != nil {
          return err
        }
        if parent == nil {
          return fmt.Errorf("%s is not a child of %s", parent_id, ctx.Server.ID)
        }

        signal = NewStartChildSignal(child_id, action)
        return parent.Process(context, ctx.User.ID, signal)
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

