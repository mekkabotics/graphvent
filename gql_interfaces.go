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

func AddNodeInterfaceFields(gql *GQLInterface) {
  gql.Interface.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
  })

  gql.Interface.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
  })
}

func AddLockableInterfaceFields(gql *GQLInterface) {
  addLockableInterfaceFields(gql, GQLInterfaceLockable)
}

func addLockableInterfaceFields(gql *GQLInterface, gql_lockable *GQLInterface) {
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

func AddThreadInterfaceFields(gql *GQLInterface) {
  addThreadInterfaceFields(gql, GQLInterfaceThread)
}

func addThreadInterfaceFields(gql *GQLInterface, gql_thread *GQLInterface) {
  AddLockableInterfaceFields(gql)

  gql.Interface.AddFieldConfig("Children", &graphql.Field{
    Type: gql_thread.List,
  })

  gql.Interface.AddFieldConfig("Parent", &graphql.Field{
    Type: gql_thread.Interface,
  })
}

func NodeHasExtensions(node *Node, extensions []ExtType) bool {
  if node == nil {
    return false
  }

  for _, ext := range(extensions) {
    _, has := node.Extensions[ext]
    if has == false {
      return false
    }
  }

  return true
}

func GQLNodeHasExtensions(extensions []ExtType) func(graphql.IsTypeOfParams) bool {
  return func(p graphql.IsTypeOfParams) bool {
    node, ok := p.Value.(*Node)
    if ok == false {
      return false
    }

    return NodeHasExtensions(node, extensions)
  }
}

func NodeResolver(required_extensions []ExtType, default_type **graphql.Object)func(graphql.ResolveTypeParams) *graphql.Object {
  return func(p graphql.ResolveTypeParams) *graphql.Object {
    ctx, ok := p.Context.Value("resolve").(*ResolveContext)
    if ok == false {
      return nil
    }

    node, ok := p.Value.(*Node)
    if ok == false {
      return nil
    }

    gql_type, exists := ctx.GQLContext.NodeTypes[node.Type]
    if exists == false {
      for _, ext := range(required_extensions) {
        _, exists := node.Extensions[ext]
        if exists == false {
          return nil
        }
      }
      return *default_type
    }

    return gql_type
  }
}

var GQLInterfaceNode = NewGQLInterface("Node", "DefaultNode", []*graphql.Interface{}, []ExtType{}, func(gql *GQLInterface) {
  AddNodeInterfaceFields(gql)
}, func(gql *GQLInterface) {
  AddNodeFields(gql.Default)
})

var GQLInterfaceLockable = NewGQLInterface("Lockable", "DefaultLockable", []*graphql.Interface{GQLInterfaceNode.Interface}, []ExtType{LockableExtType}, func(gql *GQLInterface) {
  addLockableInterfaceFields(gql, gql)
}, func(gql *GQLInterface) {
  addLockableFields(gql.Default, gql.Interface, gql.List)
})

var GQLInterfaceThread = NewGQLInterface("Thread", "DefaultThread", []*graphql.Interface{GQLInterfaceNode.Interface, }, []ExtType{ThreadExtType, LockableExtType}, func(gql *GQLInterface){
  addThreadInterfaceFields(gql, gql)
}, func(gql *GQLInterface) {
  addThreadFields(gql.Default, gql.Interface, gql.List)
})
