package graphvent
import (
  "time"
  "reflect"
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/ast"
  "github.com/google/uuid"
)

func GetFieldNames(ctx *Context, selection_set *ast.SelectionSet) []string {
  names := []string{}
  if selection_set == nil {
    return names
  }

  for _, sel := range(selection_set.Selections) {
    switch field := sel.(type) {
    case *ast.Field:
      names = append(names, field.Name.Value)
    case *ast.InlineFragment:
      names = append(names, GetFieldNames(ctx, field.SelectionSet)...)
    default:
      ctx.Log.Logf("gql", "Unknown selection type: %s", reflect.TypeOf(field))
    }
  }

  return names
}

func GetResolveFields(ctx *Context, p graphql.ResolveParams) []string {
  names := []string{}
  for _, field := range(p.Info.FieldASTs) {
    names = append(names, GetFieldNames(ctx, field.SelectionSet)...)
  }

  return names
}

func ResolveNodes(ctx *ResolveContext, p graphql.ResolveParams, ids []NodeID) ([]NodeResult, error) {
  fields := GetResolveFields(ctx.Context, p)
  ctx.Context.Log.Logf("gql", "RESOLVE_NODES(%+v): %+v", ids, fields)

  read_signals := map[NodeID]uuid.UUID{}
  for _, id := range(ids) {
    // Get a list of fields that will be written
    ext_fields, err := ctx.GQLContext.GetACLFields(p.Info.FieldName, fields)
    if err != nil {
      return nil, err
    }
    // Create a read signal, send it to the specified node, and add the wait to the response map if the send returns no error
    read_signal := NewReadSignal(ext_fields)

    ctx.Ext.resolver_response_lock.Lock()
    ctx.Ext.resolver_response[read_signal.UUID] = ctx.ID
    ctx.Ext.resolver_response_lock.Unlock()

    err = ctx.Context.Send(ctx.Server.ID, id, read_signal)
    read_signals[id] = read_signal.UUID
    if err != nil {
      ctx.Ext.resolver_response_lock.Lock()
      delete(ctx.Ext.resolver_response, read_signal.UUID)
      ctx.Ext.resolver_response_lock.Unlock()
      return nil, err
    }
  }

  responses := []NodeResult{}
  for node_id, sig_id := range(read_signals) {
    // Wait for the response, returning an error on timeout
    response, err := WaitForResult(ctx.Chan, time.Millisecond*100, sig_id)
    if err != nil {
      return nil, err
    }
    responses = append(responses, NodeResult{node_id, response.(*ReadResultSignal)})
  }

  return responses, nil
}
