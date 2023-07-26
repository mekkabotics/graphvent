package graphvent

import (
  "github.com/graphql-go/graphql"
  "reflect"
  "fmt"
)

func NewField(init func()*graphql.Field) *graphql.Field {
  return init()
}

type Singleton[K graphql.Type] struct {
  Type K
  List *graphql.List
}

func NewSingleton[K graphql.Type](init func() K, post_init func(K, *graphql.List)) *Singleton[K] {
  val := init()
  list := graphql.NewList(val)
  if post_init != nil {
    post_init(val, list)
  }
  return &Singleton[K]{
    Type: val,
    List: list,
  }
}

func AddNodeInterfaceFields(i *graphql.Interface) {
  i.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
  })

  i.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
  })
}

func PrepTypeResolve(p graphql.ResolveTypeParams) (*ResolveContext, error) {
  resolve_context, ok := p.Context.Value("resolve").(*ResolveContext)
  if ok == false {
    return nil, fmt.Errorf("Bad resolve in params context")
  }
  return resolve_context, nil
}

var GQLInterfaceNode = NewSingleton(func() *graphql.Interface {
  i := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Node",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, err := PrepTypeResolve(p)
      if err != nil {
        return nil
      }

      valid_nodes := ctx.GQLContext.ValidNodes
      p_type := reflect.TypeOf(p.Value)

      for key, value := range(valid_nodes) {
        if p_type == key {
          return value
        }
      }

      _, ok := p.Value.(Node)
      if ok == true {
        return ctx.GQLContext.BaseNodeType
      }

      return nil
    },
    Fields: graphql.Fields{},
  })

  AddNodeInterfaceFields(i)

  return i
}, nil)

var GQLInterfaceLockable = NewSingleton(func() *graphql.Interface {
  gql_interface_lockable := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Lockable",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, err := PrepTypeResolve(p)
      if err != nil {
        return nil
      }

      valid_lockables := ctx.GQLContext.ValidLockables
      p_type := reflect.TypeOf(p.Value)

      for key, value := range(valid_lockables) {
        if p_type == key {
          return value
        }
      }

      _, ok := p.Value.(*Node)
      if ok == false {
        return ctx.GQLContext.BaseLockableType
      }
      return nil
    },
    Fields: graphql.Fields{},
  })

  return gql_interface_lockable
}, func(lockable *graphql.Interface, lockable_list *graphql.List) {
  lockable.AddFieldConfig("Requirements", &graphql.Field{
    Type: lockable_list,
  })

  lockable.AddFieldConfig("Dependencies", &graphql.Field{
    Type: lockable_list,
  })

  lockable.AddFieldConfig("Owner", &graphql.Field{
    Type: lockable,
  })
  AddNodeInterfaceFields(lockable)
})

var GQLInterfaceThread = NewSingleton(func() *graphql.Interface {
  gql_interface_thread := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Thread",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, err := PrepTypeResolve(p)
      if err != nil {
        return nil
      }

      valid_threads := ctx.GQLContext.ValidThreads
      p_type := reflect.TypeOf(p.Value)

      for key, value := range(valid_threads) {
        if p_type == key {
          return value
        }
      }

      node, ok := p.Value.(*Node)
      if ok == false {
        return nil
      }

      _, err = GetExt[*ThreadExt](node)
      if err == nil {
        return ctx.GQLContext.BaseThreadType
      }

      return nil
    },
    Fields: graphql.Fields{},
  })

  return gql_interface_thread
}, func(thread *graphql.Interface, thread_list *graphql.List) {
  thread.AddFieldConfig("Children", &graphql.Field{
    Type: thread_list,
  })

  thread.AddFieldConfig("Parent", &graphql.Field{
    Type: thread,
  })

  thread.AddFieldConfig("State", &graphql.Field{
    Type: graphql.String,
  })

  AddNodeInterfaceFields(thread)
})
