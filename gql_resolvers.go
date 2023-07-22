package graphvent
import (
  "fmt"
  "reflect"
  "github.com/graphql-go/graphql"
)

func GetResolveContext(p graphql.ResolveParams) (*ResolveContext, error) {
  resolve_context, ok := p.Context.Value("resolve").(*ResolveContext)
  if ok == false {
    return nil, fmt.Errorf("Bad resolve in params context")
  }
  return resolve_context, nil
}

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

func GQLNodeID(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Node)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Node")
  }
  return node.ID(), nil
}

func GQLThreadListen(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(*GQLThread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GQLThread")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  listen := ""
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    listen = node.Listen
    return nil
  })

  if err != nil {
    return nil, err
  }

  return listen, nil
}

func GQLThreadParent(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Thread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var parent Thread = nil
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    parent = node.Parent()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return parent, nil
}

func GQLThreadState(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Thread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var state string
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    state = node.State()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return state, nil
}

func GQLThreadChildren(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Thread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Thread")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var children []Thread = nil
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    children = node.Children()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return children, nil
}

func GQLLockableName(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  name := ""
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) error {
    name = node.Name()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return name, nil
}

func GQLLockableRequirements(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var requirements []Lockable = nil
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    requirements = node.Requirements()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return requirements, nil
}

func GQLLockableDependencies(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var dependencies []Lockable = nil
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    dependencies = node.Dependencies()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return dependencies, nil
}

func GQLLockableOwner(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(Lockable)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to Lockable")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var owner Node = nil
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) (error) {
    owner = node.Owner()
    return nil
  })

  if err != nil {
    return nil, err
  }

  return owner, nil
}

func GQLThreadUsers(p graphql.ResolveParams) (interface{}, error) {
  node, ok := p.Source.(*GQLThread)
  if ok == false || node == nil {
    return nil, fmt.Errorf("Failed to cast source to GQLThread")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to cast context graph_context to Context")
  }

  var users []*User
  err := UseStates(ctx, []Node{node}, func(nodes NodeMap) error {
    users = make([]*User, len(node.Users))
    i := 0
    for _, user := range(node.Users) {
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

func GQLSignalSource(p graphql.ResolveParams) (interface{}, error) {
  return GQLSignalFn(p, func(signal GraphSignal, p graphql.ResolveParams)(interface{}, error){
    return signal.Source(), nil
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
