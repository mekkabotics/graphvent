package graphvent
import (
  "time"
  "reflect"
  "fmt"
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

  resp_channels := map[uuid.UUID]chan Signal{}
  node_ids := map[uuid.UUID]NodeID{}
  for _, id := range(ids) {
    // Get a list of fields that will be written
    ext_fields, err := ctx.GQLContext.GetACLFields(p.Info.FieldName, fields)
    if err != nil {
      return nil, err
    }
    // Create a read signal, send it to the specified node, and add the wait to the response map if the send returns no error
    read_signal := NewReadSignal(ext_fields)
    auth_signal, err := NewAuthorizedSignal(ctx.Key, &read_signal)
    if err != nil {
      return nil, err
    }


    response_chan := ctx.Ext.GetResponseChannel(read_signal.ID())
    resp_channels[read_signal.ID()] = response_chan
    node_ids[read_signal.ID()] = id

    err = ctx.Context.Send(ctx.Server.ID, id, &auth_signal)
    if err != nil {
      ctx.Ext.FreeResponseChannel(read_signal.ID())
      return nil, err
    }
  }

  responses := []NodeResult{}
  for sig_id, response_chan := range(resp_channels) {
    // Wait for the response, returning an error on timeout
    response, err := WaitForResult(response_chan, time.Millisecond*100, sig_id)
    if err != nil {
      return nil, err
    }
    switch resp := response.(type) {
    case *ReadResultSignal:
      responses = append(responses, NodeResult{node_ids[sig_id], resp})
    case *ErrorSignal:
      return nil, fmt.Errorf(resp.Str)
    default:
      return nil, fmt.Errorf("BAD_TYPE: %s", reflect.TypeOf(resp))
    }
  }

  return responses, nil
}
