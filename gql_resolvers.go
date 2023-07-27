package graphvent
import (
  "fmt"
  "reflect"
  "github.com/graphql-go/graphql"
)

func PrepResolve(p graphql.ResolveParams) (*Node, *ResolveContext, error) {
  resolve_context, ok := p.Context.Value("resolve").(*ResolveContext)
  if ok == false {
    return nil, nil, fmt.Errorf("Bad resolve in params context")
  }

  node, ok := p.Source.(*Node)
  if ok == false {
    return nil, nil, fmt.Errorf("Source is not a *Node in PrepResolve")
  }

  return node, resolve_context, nil
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
  node, _, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  return node.ID, nil
}

func GQLNodeTypeHash(p graphql.ResolveParams) (interface{}, error) {
  node, _, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  return string(node.Type), nil
}

func GQLNodeListen(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  gql_ext, err := GetExt[*GQLExt](node)
  if err != nil {
    return nil, err
  }

  listen := ""
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"listen"}), func(context *StateContext) error {
    listen = gql_ext.Listen
    return nil
  })

  if err != nil {
    return nil, err
  }

  return listen, nil
}

func GQLThreadParent(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  thread_ext, err := GetExt[*ThreadExt](node)
  if err != nil {
    return nil, err
  }

  var parent *Node = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"parent"}), func(context *StateContext) error {
    parent = thread_ext.Parent
    return nil
  })

  if err != nil {
    return nil, err
  }

  return parent, nil
}

func GQLThreadState(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  thread_ext, err := GetExt[*ThreadExt](node)
  if err != nil {
    return nil, err
  }

  var state string
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"state"}), func(context *StateContext) error {
    state = thread_ext.State
    return nil
  })

  if err != nil {
    return nil, err
  }

  return state, nil
}

func GQLThreadChildren(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  thread_ext, err := GetExt[*ThreadExt](node)
  if err != nil {
    return nil, err
  }

  var children []*Node = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"children"}), func(context *StateContext) error {
    children = thread_ext.ChildList()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return children, nil
}

func GQLLockableRequirements(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  lockable_ext, err := GetExt[*LockableExt](node)
  if err != nil {
    return nil, err
  }

  var requirements []*Node = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"requirements"}), func(context *StateContext) error {
    requirements = make([]*Node, len(lockable_ext.Requirements))
    i := 0
    for _, req := range(lockable_ext.Requirements) {
      requirements[i] = req
      i += 1
    }
    return nil
  })

  if err != nil {
    return nil, err
  }

  return requirements, nil
}

func GQLLockableDependencies(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  lockable_ext, err := GetExt[*LockableExt](node)
  if err != nil {
    return nil, err
  }

  var dependencies []*Node = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"dependencies"}), func(context *StateContext) error {
    dependencies = make([]*Node, len(lockable_ext.Dependencies))
    i := 0
    for _, dep := range(lockable_ext.Dependencies) {
      dependencies[i] = dep
      i += 1
    }
    return nil
  })

  if err != nil {
    return nil, err
  }

  return dependencies, nil
}

func GQLLockableOwner(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  lockable_ext, err := GetExt[*LockableExt](node)
  if err != nil {
    return nil, err
  }

  var owner *Node = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"owner"}), func(context *StateContext) error {
    owner = lockable_ext.Owner
    return nil
  })

  if err != nil {
    return nil, err
  }

  return owner, nil
}

func GQLGroupMembers(p graphql.ResolveParams) (interface{}, error) {
  node, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  group_ext, err := GetExt[*GroupExt](node)
  if err != nil {
    return nil, err
  }

  var members []*Node
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewACLInfo(node, []string{"users"}), func(context *StateContext) error {
    members = make([]*Node, len(group_ext.Members))
    i := 0
    for _, member := range(group_ext.Members) {
      members[i] = member
      i += 1
    }
    return nil
  })

  if err != nil {
    return nil, err
  }

  return members, nil
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
