package graphvent
import (
  "fmt"
  "github.com/graphql-go/graphql"
)

var GQLMutationSendUpdate = NewField(func()*graphql.Field {
  gql_mutation_send_update := &graphql.Field{
    Type: GQLTypeSignal.Type,
    Args: graphql.FieldConfigArgument{
      "id": &graphql.ArgumentConfig{
        Type: graphql.String,
      },
      "signal": &graphql.ArgumentConfig{
        Type: GQLTypeSignalInput.Type,
      },
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      ctx, err := PrepResolve(p)
      if err != nil {
        return nil, err
      }

      err = ctx.Server.Allowed("signal", "self", ctx.User)
      if err != nil {
        return nil, err
      }

      signal_map, err := ExtractParam[map[string]interface{}](p, "signal")
      if err != nil {
        return nil, err
      }

      var signal GraphSignal = nil
      if signal_map["Direction"] == "up" {
        signal = NewSignal(ctx.Server, signal_map["Type"].(string))
      } else if signal_map["Direction"] == "down" {
        signal = NewDownSignal(ctx.Server, signal_map["Type"].(string))
      } else if signal_map["Direction"] == "direct" {
        signal = NewDirectSignal(ctx.Server, signal_map["Type"].(string))
      } else {
        return nil, fmt.Errorf("Bad direction: %d", signal_map["Direction"])
      }

      id, err := ExtractID(p, "id")
      if err != nil {
        return nil, err
      }

      var node Node = nil
      err = UseStates(ctx.Context, []Node{ctx.Server}, func(nodes NodeMap) (error){
        node = FindChild(ctx.Context, ctx.Server, id, nodes)
        if node == nil {
          return fmt.Errorf("Failed to find ID: %s as child of server thread", id)
        }
        node.Signal(ctx.Context, signal, nodes)
        return nil
      })
      if err != nil {
        return nil, err
      }

      return signal, nil
    },
  }

  return gql_mutation_send_update
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

      err = ctx.Server.Allowed("start_child", "self", ctx.User)
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
      err = UseStates(ctx.Context, []Node{ctx.Server}, func(nodes NodeMap) (error){
        node := FindChild(ctx.Context, ctx.Server, parent_id, nodes)
        if node == nil {
          return fmt.Errorf("Failed to find ID: %s as child of server thread", parent_id)
        }
        return UseMoreStates(ctx.Context, []Node{node}, nodes, func(NodeMap) error {
          signal = NewStartChildSignal(ctx.Server,  child_id, action)
          return node.Signal(ctx.Context, signal, nodes)
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

