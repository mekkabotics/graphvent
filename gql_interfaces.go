package graphvent

import (
  "github.com/graphql-go/graphql"
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

func AddNodeInterfaceFields(gql *Interface) {
  gql.Interface.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
  })

  gql.Interface.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
  })
}

func AddLockableInterfaceFields(gql *Interface) {
  addLockableInterfaceFields(gql, InterfaceLockable)
}

func addLockableInterfaceFields(gql *Interface, gql_lockable *Interface) {
  AddNodeInterfaceFields(gql)

  gql.Interface.AddFieldConfig("Requirements", &graphql.Field{
    Type: gql_lockable.List,
  })

  gql.Interface.AddFieldConfig("Dependencies", &graphql.Field{
    Type: gql_lockable.List,
  })

  gql.Interface.AddFieldConfig("Owner", &graphql.Field{
    Type: gql_lockable.Interface,
  })
}

func NodeIsTypeResolver(extensions []ExtType) func(graphql.IsTypeOfParams) bool {
  return func(p graphql.IsTypeOfParams) bool {
    node, ok := p.Value.(NodeResult)
    if ok == false {
      return false
    }

    for _, ext := range(extensions) {
      _, has := node.Result.Extensions[ext]
      if has == false {
        return false
      }
    }

    return true
  }
}

func NodeTypeResolver(required_extensions []ExtType, default_type **graphql.Object)func(graphql.ResolveTypeParams) *graphql.Object {
  return func(p graphql.ResolveTypeParams) *graphql.Object {
    ctx, ok := p.Context.Value("resolve").(*ResolveContext)
    if ok == false {
      return nil
    }

    node, ok := p.Value.(NodeResult)
    if ok == false {
      return nil
    }

    gql_type, exists := ctx.GQLContext.NodeTypes[node.Result.NodeType]
    if exists == false {
      for _, ext := range(required_extensions) {
        _, exists := node.Result.Extensions[ext]
        if exists == false {
          return nil
        }
      }
      return *default_type
    }

    return gql_type
  }
}

type NodeResult struct {
  ID NodeID
  Result *ReadResultSignal
}

func NewInterface(if_name string, default_name string, interfaces []*graphql.Interface, extensions []ExtType, init_1 func(*Interface), init_2 func(*Interface)) *Interface {
  var gql Interface
  gql.Extensions = extensions
  gql.Interface = graphql.NewInterface(graphql.InterfaceConfig{
    Name: if_name,
    ResolveType: NodeTypeResolver([]ExtType{}, &gql.Default),
    Fields: graphql.Fields{},
  })
  gql.List = graphql.NewList(gql.Interface)

  init_1(&gql)

  gql.Default = graphql.NewObject(graphql.ObjectConfig{
    Name: default_name,
    Interfaces: append(interfaces, gql.Interface),
    IsTypeOf: NodeIsTypeResolver([]ExtType{}),
    Fields: graphql.Fields{},
  })

  init_2(&gql)

  return &gql
}


var InterfaceNode = NewInterface("Node", "DefaultNode", []*graphql.Interface{}, []ExtType{}, func(gql *Interface) {
  AddNodeInterfaceFields(gql)
}, func(gql *Interface) {
  AddNodeFields(gql.Default)
})

var InterfaceLockable = NewInterface("Lockable", "DefaultLockable", []*graphql.Interface{InterfaceNode.Interface}, []ExtType{LockableExtType}, func(gql *Interface) {
  addLockableInterfaceFields(gql, gql)
}, func(gql *Interface) {
  addLockableFields(gql.Default, gql.Interface, gql.List)
})

