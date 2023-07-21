package graphvent
import (
  "fmt"
  "github.com/graphql-go/graphql"
)

var gql_mutation_send_update *graphql.Field = nil
func GQLMutationSendUpdate() *graphql.Field {
  if gql_mutation_send_update == nil {
    gql_mutation_send_update = &graphql.Field{
      Type: GQLTypeSignal(),
      Args: graphql.FieldConfigArgument{
        "id": &graphql.ArgumentConfig{
          Type: graphql.String,
        },
        "signal": &graphql.ArgumentConfig{
          Type: GQLTypeSignalInput(),
        },
      },
      Resolve: func(p graphql.ResolveParams) (interface{}, error) {
        ctx, server, user, err := PrepResolve(p)
        if err != nil {
          return nil, err
        }

        err = server.Allowed("signal", "self", user)
        if err != nil {
          return nil, err
        }

        signal_map, err := ExtractParam[map[string]interface{}](p, "signal")
        if err != nil {
          return nil, err
        }

        var signal GraphSignal = nil
        if signal_map["Direction"] == "up" {
          signal = NewSignal(server, signal_map["Type"].(string))
        } else if signal_map["Direction"] == "down" {
          signal = NewDownSignal(server, signal_map["Type"].(string))
        } else if signal_map["Direction"] == "direct" {
          signal = NewDirectSignal(server, signal_map["Type"].(string))
        } else {
          return nil, fmt.Errorf("Bad direction: %d", signal_map["Direction"])
        }

        id, err := ExtractID(p, "id")
        if err != nil {
          return nil, err
        }

        var node Node = nil
        err = UseStates(ctx, []Node{server}, func(nodes NodeMap) (error){
          node = FindChild(ctx, server, id, nodes)
          if node == nil {
            return fmt.Errorf("Failed to find ID: %s as child of server thread", id)
          }
          node.Signal(ctx, signal, nodes)
          return nil
        })
        if err != nil {
          return nil, err
        }

        return signal, nil
      },
    }
  }

  return gql_mutation_send_update
}

var gql_mutation_start_child *graphql.Field = nil
func GQLMutationStartChild() *graphql.Field {
  if gql_mutation_start_child == nil {
    gql_mutation_start_child = &graphql.Field{
      Type: GQLTypeSignal(),
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
        ctx, server, user, err := PrepResolve(p)
        if err != nil {
          return nil, err
        }

        err = server.Allowed("start_child", "self", user)
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
        err = UseStates(ctx, []Node{server}, func(nodes NodeMap) (error){
          node := FindChild(ctx, server, parent_id, nodes)
          if node == nil {
            return fmt.Errorf("Failed to find ID: %s as child of server thread", parent_id)
          }
          return UseMoreStates(ctx, []Node{node}, nodes, func(NodeMap) error {
            signal = NewStartChildSignal(server,  child_id, action)
            return node.Signal(ctx, signal, nodes)
          })
        })
        if err != nil {
          return nil, err
        }

        // TODO: wait for the result of the signal to send back instead of just the signal
        return signal, nil
      },
    }
  }

  return gql_mutation_start_child
}
