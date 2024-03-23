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

func GetFields(ctx *Context, node_type string, selection_set *ast.SelectionSet) []FieldIndex {
  names := []FieldIndex{}
  if selection_set == nil {
    return names
  }

  node_info, mapped := ctx.NodeTypes[node_type]
  if mapped == false {
    return nil
  }

  for _, sel := range(selection_set.Selections) {
    switch field := sel.(type) {
    case *ast.Field:
      if field.Name.Value == "ID" || field.Name.Value == "Type" {
        continue
      }

      extension, mapped := node_info.Fields[field.Name.Value]
      if mapped == false {
        continue
      }
      names = append(names, FieldIndex{extension, field.Name.Value})
    case *ast.InlineFragment:
      names = append(names, GetFields(ctx, field.TypeCondition.Name.Value, field.SelectionSet)...)
    default:
      ctx.Log.Logf("gql", "Unknown selection type: %s", reflect.TypeOf(field))
    }
  }

  return names
}

// Returns the fields that need to be resolved
func GetResolveFields(id NodeID, ctx *ResolveContext, p graphql.ResolveParams) (map[ExtType][]string, error) {
  m := map[ExtType][]string{}
  fields := []FieldIndex{}
  for _, field := range(p.Info.FieldASTs) {
    fields = append(fields, GetFields(ctx.Context, p.Info.ReturnType.Name(), field.SelectionSet)...)
  }

  cache, node_cached := ctx.NodeCache[id]
  for _, field := range(fields) {
    ext_fields, exists := m[field.Extension]
    if exists == false {
      ext_fields = []string{}
    }

    if node_cached {
      ext_cache, ext_cached := cache.Data[field.Extension]
      if ext_cached {
        _, field_cached := ext_cache[field.Tag]
        if field_cached {
          continue
        }
      }
    }

    m[field.Extension] = append(ext_fields, field.Tag)
  }
  
  return m, nil
}

// TODO: instead of doing the read right away, check if any new fields need to be read
func ResolveNode(id NodeID, p graphql.ResolveParams) (NodeResult, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return NodeResult{}, err
  }

  fields, err := GetResolveFields(id, ctx, p)
  if err != nil {
    return NodeResult{}, err
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
    return NodeResult{}, err
  }

  response, _, err := WaitForResponse(response_chan, 100*time.Millisecond, signal.ID())
  ctx.Ext.FreeResponseChannel(signal.ID())
  if err != nil {
    return NodeResult{}, err
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
    return NodeResult{}, fmt.Errorf("Bad read response: %+v", response)
  }
}
