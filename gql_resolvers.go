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

// TODO: think about what permissions should be needed to read ID, and if there's ever a case where they haven't already been granted
func GQLNodeID(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLNodeTypeHash(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLNodeListen(p graphql.ResolveParams) (interface{}, error) {
  // TODO figure out how nodes can read eachother
  return "", nil
}

func GQLThreadParent(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLThreadState(p graphql.ResolveParams) (interface{}, error) {
  return "", nil
}

func GQLThreadChildren(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLLockableRequirements(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLLockableDependencies(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLLockableOwner(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
}

func GQLGroupMembers(p graphql.ResolveParams) (interface{}, error) {
  return nil, nil
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
