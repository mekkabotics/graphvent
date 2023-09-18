package graphvent
import (
  "time"
  "reflect"
  "fmt"
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/ast"
  "github.com/google/uuid"
)

func ResolveNodeID(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(NodeResult)
  if ok == false {
    return nil, fmt.Errorf("Can't get NodeID from %+v", reflect.TypeOf(p.Source))
  }

  return node.NodeID, nil
}

func ResolveNodeTypeHash(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(NodeResult)
  if ok == false {
    return nil, fmt.Errorf("Can't get TypeHash from %+v", reflect.TypeOf(p.Source))
  }

  return uint64(node.NodeType), nil
}

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
  indices := map[uuid.UUID]int{}

  // Get a list of fields that will be written
  ext_fields, err := ctx.GQLContext.GetACLFields(p.Info.FieldName, fields)
  if err != nil {
    return nil, err
  }

  responses := make([]NodeResult, len(ids))

  for i, id := range(ids) {
    var read_signal *ReadSignal = nil 

    node, cached := ctx.NodeCache[id]
    if cached == true {
      resolve := false
      missing_exts := map[ExtType][]string{}
      for ext_type, fields := range(ext_fields) {
        cached_ext, exists := node.Data[ext_type]
        if exists == true {
          missing_fields := []string{}
          for _, field_name := range(fields) {
            _, found := cached_ext[field_name]
            if found == false {
              missing_fields = append(missing_fields, field_name)
            }
          }
          if len(missing_fields) > 0 {
            missing_exts[ext_type] = missing_fields
            resolve = true
          }
        } else {
          missing_exts[ext_type] = fields
          resolve = true
        }
      }

      if resolve == true {
        read_signal = NewReadSignal(missing_exts)
      } else {
        ctx.Context.Log.Logf("gql_subscribe", "Using cached response for %+v(%d)", id, i)
        responses[i] = node
        continue
      }
    } else {
      read_signal = NewReadSignal(ext_fields)
    }
    // Create a read signal, send it to the specified node, and add the wait to the response map if the send returns no error
    msgs := Messages{}
    msgs = msgs.Add(ctx.Context, ctx.Server.ID, ctx.Key, read_signal, id)

    response_chan := ctx.Ext.GetResponseChannel(read_signal.ID)
    resp_channels[read_signal.ID] = response_chan
    indices[read_signal.ID] = i

    // TODO: Send all at once instead of creating Messages for each
    err = ctx.Context.Send(msgs)
    if err != nil {
      ctx.Ext.FreeResponseChannel(read_signal.ID)
      return nil, err
    }
  }

  for sig_id, response_chan := range(resp_channels) {
    // Wait for the response, returning an error on timeout
    response, err := WaitForSignal(response_chan, time.Millisecond*100, func(sig *ReadResultSignal)bool{
      return sig.ReqID == sig_id
    })
    if err != nil {
      return nil, err
    }

    idx := indices[sig_id]
    responses[idx] = NodeResult{
      response.NodeID,
      response.NodeType,
      response.Extensions,
    }

    cache, exists := ctx.NodeCache[response.NodeID]
    if exists == true {
      for ext_type, fields := range(response.Extensions) {
        cached_fields, exists := cache.Data[ext_type]
        if exists == true {
          for field_name, field_value := range(fields) {
            cached_fields[field_name] = field_value
          }
        }
      }
      ctx.Context.Log.Logf("gql_subscribe", "CACHED_EXISTING_NODE: %+v", response.NodeID)
    } else {
      ctx.NodeCache[response.NodeID] = responses[idx]
      ctx.Context.Log.Logf("gql_subscribe", "CACHED_NEW_NODE: %+v", response.NodeID)
    }

  }
  ctx.Context.Log.Logf("gql", "RESOLVED_NODES %+v - %+v", ids, responses)

  return responses, nil
}
