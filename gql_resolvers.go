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
  node, ok := p.Source.(Node)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Node")
  }

  return node.ID(), nil
}

func GQLThreadListen(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(*GQLThread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GQLThread")
  }


  listen := ""
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"listen"}), func(context *StateContext) error {
    listen = node.Listen
    return nil
  })

  if err != nil {
    return nil, err
  }

  return listen, nil
}

func GQLThreadParent(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(*Thread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  var parent ThreadNode = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"parent"}), func(context *StateContext) error {
    parent = node.ThreadHandle().Parent
    return nil
  })

  if err != nil {
    return nil, err
  }

  return parent, nil
}

func GQLThreadState(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(ThreadNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  var state string
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"state"}), func(context *StateContext) error {
    state = node.ThreadHandle().StateName
    return nil
  })

  if err != nil {
    return nil, err
  }

  return state, nil
}

func GQLThreadChildren(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(ThreadNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  var children []ThreadNode = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"children"}), func(context *StateContext) error {
    children = make([]ThreadNode, len(node.ThreadHandle().Children))
    i := 0
    for _, info := range(node.ThreadHandle().Children) {
      children[i] = info.Child
      i += 1
    }
    return nil
  })

  if err != nil {
    return nil, err
  }

  return children, nil
}

func GQLLockableName(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(LockableNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  name := ""
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"name"}), func(context *StateContext) error {
    name = node.LockableHandle().Name
    return nil
  })

  if err != nil {
    return nil, err
  }

  return name, nil
}

func GQLLockableRequirements(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(LockableNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  var requirements []LockableNode = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"requirements"}), func(context *StateContext) error {
    requirements = make([]LockableNode, len(node.LockableHandle().Requirements))
    i := 0
    for _, req := range(node.LockableHandle().Requirements) {
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
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(LockableNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  var dependencies []LockableNode = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"dependencies"}), func(context *StateContext) error {
    dependencies = make([]LockableNode, len(node.LockableHandle().Dependencies))
    i := 0
    for _, dep := range(node.LockableHandle().Dependencies) {
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
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(LockableNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  var owner Node = nil
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"owner"}), func(context *StateContext) error {
    owner = node.LockableHandle().Owner
    return nil
  })

  if err != nil {
    return nil, err
  }

  return owner, nil
}

func GQLGroupNodeUsers(p graphql.ResolveParams) (interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  node, ok := p.Source.(GroupNode)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GQLThread")
  }

  var users []*User
  context := NewReadContext(ctx.Context)
  err = UseStates(context, ctx.User, NewLockInfo(node, []string{"users"}), func(context *StateContext) error {
    users = make([]*User, len(node.Users()))
    i := 0
    for _, user := range(node.Users()) {
      users[i] = user
      i += 1
    }
    return nil
  })

  if err != nil {
    return nil, err
  }

  return users, nil
}

func GQLSignalFn(p graphql.ResolveParams, fn func(GraphSignal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
    if signal, ok := p.Source.(GraphSignal); ok {
      return fn(signal, p)
    }
    return nil, fmt.Errorf("Failed to cast source to event")
}

func GQLSignalType(p graphql.ResolveParams) (interface{}, error) {
  return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
    return signal.Type(), nil
  })
}

func GQLSignalDirection(p graphql.ResolveParams) (interface{}, error) {
    return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
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
    return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
      return signal.String(), nil
    })
}
