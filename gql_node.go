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
    default:
      ctx.Log.Logf("gql", "Unknown selection type: %s", reflect.TypeOf(field))
    }
  }

  return names
}

// Returns the fields that need to be resolved
func GetResolveFields(id NodeID, ctx *ResolveContext, p graphql.ResolveParams) (map[ExtType][]string, error) {
  node_info, mapped := ctx.Context.NodeTypes[p.Info.ReturnType.Name()]
  if mapped == false {
    return nil, fmt.Errorf("No NodeType %s", p.Info.ReturnType.Name())
  }

  fields := map[ExtType][]string{}
  names := []string{}
  for _, field := range(p.Info.FieldASTs) {
    names = append(names, GetFieldNames(ctx.Context, field.SelectionSet)...)
  }

  cache, node_cached := ctx.NodeCache[id]
  for _, name := range(names) {
    if name == "ID" || name == "Type" {
      continue
    }

    ext_type, field_mapped := node_info.Fields[name]
    if field_mapped == false {
      return nil, fmt.Errorf("NodeType %s does not have field %s", p.Info.ReturnType.Name(), name)
    }

    ext_fields, exists := fields[ext_type]
    if exists == false {
      ext_fields = []string{}
    }

    if node_cached {
      ext_cache, ext_cached := cache.Data[ext_type]
      if ext_cached {
        _, field_cached := ext_cache[name]
        if field_cached {
          continue
        }
      }
    }

    fields[ext_type] = append(ext_fields, name)
  }
  
  return fields, nil
}

func ResolveNode(id NodeID, p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  fields, err := GetResolveFields(id, ctx, p)
  if err != nil {
    return nil, err
  }
  
  ctx.Context.Log.Logf("gql", "Resolving fields %+v on node %s", fields, id)

  signal := NewReadSignal(fields)
  response_chan := ctx.Ext.GetResponseChannel(signal.ID())
  // TODO: TIMEOUT DURATION
  err = ctx.Context.Send(ctx.Server, []SendMsg{{
    Dest: id,
    Signal: signal,
  }})
  if err != nil {
    ctx.Ext.FreeResponseChannel(signal.ID())
    return nil, err
  }

  response, _, err := WaitForResponse(response_chan, 100*time.Millisecond, signal.ID())
  ctx.Ext.FreeResponseChannel(signal.ID())
  if err != nil {
    return nil, err
  }

  switch response := response.(type) {
  case *ReadResultSignal:
    cache, node_cached := ctx.NodeCache[id]
    if node_cached == false {
      cache = NodeResult{
        NodeID: id,
        NodeType: response.NodeType,
        Data: response.Extensions,
      }
    } else {
      for ext_type, ext_data := range(response.Extensions) {
        cached_ext, ext_cached := cache.Data[ext_type]
        if ext_cached {
          for field_name, field := range(ext_data) {
            cache.Data[ext_type][field_name] = field
          }
        } else {
          cache.Data[ext_type] = ext_data
        }

        cache.Data[ext_type] = cached_ext
      }
    }

    ctx.NodeCache[id] = cache
    return ctx.NodeCache[id], nil
  default:
    return nil, fmt.Errorf("Bad read response: %+v", response)
  }
}
