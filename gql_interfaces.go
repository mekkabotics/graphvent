package graphvent

import (
  "github.com/graphql-go/graphql"
  "reflect"
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

func addNodeInterfaceFields(i *graphql.Interface) {
  i.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
  })
}

var GQLInterfaceNode = NewSingleton(func() *graphql.Interface {
  i := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Node",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("graph_context").(*Context)
      if ok == false {
        return nil
      }

      valid_nodes := ctx.GQL.ValidNodes
      p_type := reflect.TypeOf(p.Value)

      for key, value := range(valid_nodes) {
        if p_type == key {
          return value
        }
      }

      _, ok = p.Value.(Node)
      if ok == true {
        return ctx.GQL.BaseNodeType
      }

      return nil
    },
    Fields: graphql.Fields{},
  })

  addNodeInterfaceFields(i)

  return i
}, nil)

func addLockableInterfaceFields(i *graphql.Interface, lockable *graphql.Interface, list *graphql.List) {
  addNodeInterfaceFields(i)

  i.AddFieldConfig("Name", &graphql.Field{
    Type: graphql.String,
  })

  i.AddFieldConfig("Requirements", &graphql.Field{
    Type: list,
  })

  i.AddFieldConfig("Dependencies", &graphql.Field{
    Type: list,
  })

  i.AddFieldConfig("Owner", &graphql.Field{
    Type: lockable,
  })
}

var GQLInterfaceLockable = NewSingleton(func() *graphql.Interface {
  gql_interface_lockable := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Lockable",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("graph_context").(*Context)
      if ok == false {
        return nil
      }

      valid_lockables := ctx.GQL.ValidLockables
      p_type := reflect.TypeOf(p.Value)

      for key, value := range(valid_lockables) {
        if p_type == key {
          return value
        }
      }

      _, ok = p.Value.(Lockable)
      if ok == true {
        return ctx.GQL.BaseLockableType
      }
      return nil
    },
    Fields: graphql.Fields{},
  })

  return gql_interface_lockable
}, func(lockable *graphql.Interface, lockable_list *graphql.List) {
  addLockableInterfaceFields(lockable, lockable, lockable_list)
})

func addThreadInterfaceFields(i *graphql.Interface, thread *graphql.Interface, list *graphql.List) {
  addLockableInterfaceFields(i, GQLInterfaceLockable.Type, GQLInterfaceLockable.List)

  i.AddFieldConfig("Children", &graphql.Field{
    Type: list,
  })

  i.AddFieldConfig("Parent", &graphql.Field{
    Type: thread,
  })
}

var GQLInterfaceThread = NewSingleton(func() *graphql.Interface {
  gql_interface_thread := graphql.NewInterface(graphql.InterfaceConfig{
    Name: "Thread",
    ResolveType: func(p graphql.ResolveTypeParams) *graphql.Object {
      ctx, ok := p.Context.Value("graph_context").(*Context)
      if ok == false {
        return nil
      }

      valid_threads := ctx.GQL.ValidThreads
      p_type := reflect.TypeOf(p.Value)

      for key, value := range(valid_threads) {
        if p_type == key {
          return value
        }
      }

      _, ok = p.Value.(Thread)
      if ok == true {
        return ctx.GQL.BaseThreadType
      }

      return nil
    },
    Fields: graphql.Fields{},
  })

  return gql_interface_thread
}, func(thread *graphql.Interface, thread_list *graphql.List) {
  addThreadInterfaceFields(thread, thread, thread_list)
})
