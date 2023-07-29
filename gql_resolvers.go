package graphvent
import (
  "fmt"
  "reflect"
  "github.com/graphql-go/graphql"
)

func PrepResolve(p graphql.ResolveParams) (*ResolveContext, error) {
  resolve_context, ok := p.Context.Value("resolve").(*ResolveContext)
  if ok == false {
    return nil, fmt.Errorf("Bad resolve in params context")
  }

  return resolve_context, nil
}

// TODO: Make composabe by checkinf if K is a slice, then recursing in the same way that ExtractList does
func ExtractParam[K interface{}](p graphql.ResolveParams, name string) (K, error) {
  var zero K
  arg_if, ok := p.Args[name]
  if ok == false {
    return zero, fmt.Errorf("No Arg of name %s", name)
  }

  arg, ok := arg_if.(K)
  if ok == false {
    return zero, fmt.Errorf("Failed to cast arg %s(%+v) to %+v", name, arg_if, reflect.TypeOf(zero))
  }

  return arg, nil
}

func ExtractList[K interface{}](p graphql.ResolveParams, name string) ([]K, error) {
  var zero K

  arg_list, err := ExtractParam[[]interface{}](p, name)
  if err != nil {
    return nil, err
  }

  ret := make([]K, len(arg_list))
  for i, val := range(arg_list) {
    val_conv, ok := arg_list[i].(K)
    if ok == false {
      return nil, fmt.Errorf("Failed to cast arg %s[%d](%+v) to %+v", name, i, val, reflect.TypeOf(zero))
    }
    ret[i] = val_conv
  }

  return ret, nil
}

func ExtractID(p graphql.ResolveParams, name string) (NodeID, error) {
  id_str, err := ExtractParam[string](p, name)
  if err != nil {
    return ZeroID, err
  }

  id, err := ParseID(id_str)
  if err != nil {
    return ZeroID, err
  }

  return id, nil
}

func ResolveNodeResult(p graphql.ResolveParams, resolve_fn func(NodeResult)(interface{}, error)) (interface{}, error) {
  node, ok := p.Source.(NodeResult)
  if ok == false {
    return nil, fmt.Errorf("p.Value is not NodeResult")
  }

  return resolve_fn(node)
}

func ResolveNodeID(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResult(p, func(node NodeResult) (interface{}, error) {
    return node.ID, nil
  })
}

func ResolveNodeTypeHash(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResult(p, func(node NodeResult) (interface{}, error) {
    return Hash(node.Result.NodeType), nil
  })
}

func ResolveNodeResultExt[T any](p graphql.ResolveParams, ext_type ExtType, field string, resolve_fn func(T)(interface{}, error)) (interface{}, error) {
  return ResolveNodeResult(p, func(result NodeResult) (interface{}, error) {
    ext, exists := result.Result.Extensions[ext_type]
    if exists == false {
      return nil, fmt.Errorf("%s is not in the extensions of the result", ext_type)
    }

    val_if, exists := ext[field]
    if exists == false {
      return nil, fmt.Errorf("%s is not in the fields of %s in the result", field, ext_type)
    }

    var zero T
    val, ok := val_if.(T)
    if ok == false {
      return nil, fmt.Errorf("%s.%s is not %s", ext_type, field, reflect.TypeOf(zero))
    }

    return resolve_fn(val)
  })
}

func ResolveListen(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResultExt(p, GQLExtType, "listen", func(listen string) (interface{}, error) {
    return listen, nil
  })
}

func ResolveRequirements(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResultExt(p, LockableExtType, "requirements", func(requirements []NodeID) (interface{}, error) {
    res := make([]string, len(requirements))
    for i, id := range(requirements) {
      res[i] = id.String()
    }
    return res, nil
  })
}

func ResolveDependencies(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResultExt(p, LockableExtType, "dependencies", func(dependencies []NodeID) (interface{}, error) {
    res := make([]string, len(dependencies))
    for i, id := range(dependencies) {
      res[i] = id.String()
    }
    return res, nil
  })
}

func ResolveOwner(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResultExt(p, LockableExtType, "owner", func(owner NodeID) (interface{}, error) {
    return owner.String(), nil
  })
}

func ResolveMembers(p graphql.ResolveParams) (interface{}, error) {
  return ResolveNodeResultExt(p, GroupExtType, "members", func(members []NodeID) (interface{}, error) {
    res := make([]string, len(members))
    for i, id := range(members) {
      res[i] = id.String()
    }
    return res, nil
  })
}

func GQLSignalFn(p graphql.ResolveParams, fn func(Signal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
    if signal, ok := p.Source.(Signal); ok {
      return fn(signal, p)
    }
    return nil, fmt.Errorf("Failed to cast source to event")
}

func GQLSignalType(p graphql.ResolveParams) (interface{}, error) {
  return GQLSignalFn(p, func(signal Signal, p graphql.ResolveParams)(interface{}, error){
    return signal.Type(), nil
  })
}

func GQLSignalDirection(p graphql.ResolveParams) (interface{}, error) {
    return GQLSignalFn(p, func(signal Signal, p graphql.ResolveParams)(interface{}, error){
      direction := signal.Direction()
      if direction == Up {
        return "up", nil
      } else if direction == Down {
        return "down", nil
      } else if direction == Direct {
        return "direct", nil
      }
      return nil, fmt.Errorf("Invalid direction: %+v", direction)
    })
}

func GQLSignalString(p graphql.ResolveParams) (interface{}, error) {
    return GQLSignalFn(p, func(signal Signal, p graphql.ResolveParams)(interface{}, error){
      ser, err := signal.Serialize()
      return string(ser), err
    })
}
