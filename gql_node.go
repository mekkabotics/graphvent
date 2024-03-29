package graphvent
import (
  "reflect"
  "fmt"
  "time"
  "github.com/graphql-go/graphql"
  "github.com/graphql-go/graphql/language/ast"
)

func ResolveNodeID(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(NodeResult)
  if ok == false {
    return nil, fmt.Errorf("Can't get NodeID from %+v", reflect.TypeOf(p.Source))
  }

  return node.NodeID, nil
}

func ResolveNodeType(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(NodeResult)
  if ok == false {
    return nil, fmt.Errorf("Can't get TypeHash from %+v", reflect.TypeOf(p.Source))
  }

  return uint64(node.NodeType), nil
}

type FieldIndex struct {
  Extension ExtType
  Tag string
}

func GetFields(selection_set *ast.SelectionSet) []string {
  names := []string{}
  if selection_set == nil {
    return names
  }

  for _, sel := range(selection_set.Selections) {
    switch field := sel.(type) {
    case *ast.Field:
      if field.Name.Value == "ID" || field.Name.Value == "Type" {
        continue
      }
      names = append(names, field.Name.Value)
    case *ast.InlineFragment:
      names = append(names, GetFields(field.SelectionSet)...)
    }
  }

  return names
}

// Returns the fields that need to be resolved
func GetResolveFields(p graphql.ResolveParams) []string {
  fields := []string{}
  for _, field := range(p.Info.FieldASTs) {
    fields = append(fields, GetFields(field.SelectionSet)...)
  }

  return fields
}

func ResolveNode(id NodeID, p graphql.ResolveParams) (NodeResult, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return NodeResult{}, err
  }

  switch source := p.Source.(type) {
  case *StatusSignal:
    cached_node, cached := ctx.NodeCache[source.Source]
    if cached {
      for _, field_name := range(source.Fields) {
        _, cached := cached_node.Data[field_name]
        if cached {
          delete(cached_node.Data, field_name)
        }
      }
      ctx.NodeCache[source.Source] = cached_node
    }
  }

  cache, node_cached := ctx.NodeCache[id]
  fields := GetResolveFields(p) 
  var not_cached []string
  if node_cached {
    not_cached = []string{}
    for _, field := range(fields) {
      if node_cached {
        _, field_cached := cache.Data[field]
        if field_cached {
          continue
        }
      }

      not_cached = append(not_cached, field)
    }
  } else {
    not_cached = fields
  }

  if (len(not_cached) == 0) && (node_cached == true) {
    ctx.Context.Log.Logf("gql", "No new fields to resolve for %s", id)
    return cache, nil
  } else {
    ctx.Context.Log.Logf("gql", "Resolving fields %+v on node %s", not_cached, id)

    signal := NewReadSignal(not_cached)
    response_chan := ctx.Ext.GetResponseChannel(signal.ID())
    // TODO: TIMEOUT DURATION
    err = ctx.Context.Send(ctx.Server, []SendMsg{{
      Dest: id,
      Signal: signal,
    }})
    if err != nil {
      ctx.Ext.FreeResponseChannel(signal.ID())
      return NodeResult{}, err
    }

    response, _, err := WaitForResponse(response_chan, 100*time.Millisecond, signal.ID())
    ctx.Ext.FreeResponseChannel(signal.ID())
    if err != nil {
      return NodeResult{}, err
    }

    switch response := response.(type) {
    case *ReadResultSignal:
      if node_cached == false {
        cache = NodeResult{
          NodeID: id,
          NodeType: response.NodeType,
          Data: response.Fields,
        }
      } else {
        for field_name, field_value := range(response.Fields) {
          cache.Data[field_name] = field_value
        }
      }

      ctx.NodeCache[id] = cache
      return ctx.NodeCache[id], nil
    default:
      return NodeResult{}, fmt.Errorf("Bad read response: %+v", response)
    }
  }
}
