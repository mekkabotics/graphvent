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

func AddNodeFields(gql *GQLInterface) {
  gql.Default.AddFieldConfig("ID", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeID,
  })

  gql.Default.AddFieldConfig("TypeHash", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLNodeTypeHash,
  })
}

func LockableInterfaceFields(gql *GQLInterface) {
  AddLockableInterfaceFields(gql, gql)
}

func AddLockableInterfaceFields(gql *GQLInterface, gql_lockable *GQLInterface) {
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

func LockableFields(gql *GQLInterface) {
  AddLockableFields(gql, gql)
}

func AddLockableFields(gql *GQLInterface, gql_lockable *GQLInterface) {
  AddNodeFields(gql)

  gql.Default.AddFieldConfig("Requirements", &graphql.Field{
    Type: gql_lockable.List,
    Resolve: GQLLockableRequirements,
  })

  gql.Default.AddFieldConfig("Owner", &graphql.Field{
    Type: gql_lockable.Interface,
    Resolve: GQLLockableOwner,
  })

  gql.Default.AddFieldConfig("Dependencies", &graphql.Field{
    Type: gql_lockable.List,
    Resolve: GQLLockableDependencies,
  })
}

func ThreadInterfaceFields(gql *GQLInterface) {
  AddThreadInterfaceFields(gql, GQLInterfaceLockable, gql)
}

func AddThreadInterfaceFields(gql *GQLInterface, gql_lockable *GQLInterface, gql_thread *GQLInterface) {
  AddLockableInterfaceFields(gql, gql_lockable)

  gql.Interface.AddFieldConfig("Children", &graphql.Field{
    Type: gql_thread.List,
  })

  gql.Interface.AddFieldConfig("Parent", &graphql.Field{
    Type: gql_thread.Interface,
  })
}

func ThreadFields(gql *GQLInterface) {
  AddThreadFields(gql, GQLInterfaceLockable, gql)
}

func AddThreadFields(gql *GQLInterface, gql_lockable *GQLInterface, gql_thread *GQLInterface) {
  AddLockableFields(gql, gql_lockable)

  gql.Default.AddFieldConfig("State", &graphql.Field{
    Type: graphql.String,
    Resolve: GQLThreadState,
  })

  gql.Default.AddFieldConfig("Children", &graphql.Field{
    Type: gql_thread.List,
    Resolve: GQLThreadChildren,
  })

  gql.Default.AddFieldConfig("Parent", &graphql.Field{
    Type: gql_thread.Interface,
    Resolve: GQLThreadParent,
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

var GQLInterfaceNode = NewGQLInterface("Node", "DefaultNode", []*graphql.Interface{}, []ExtType{}, AddNodeInterfaceFields, AddNodeFields)

var GQLInterfaceLockable = NewGQLInterface("Lockable", "DefaultLockable", []*graphql.Interface{GQLInterfaceNode.Interface}, []ExtType{LockableExtType}, LockableInterfaceFields, LockableFields)

var GQLInterfaceThread = NewGQLInterface("Thread", "DefaultThread", []*graphql.Interface{GQLInterfaceNode.Interface, }, []ExtType{ThreadExtType, LockableExtType}, ThreadInterfaceFields, ThreadFields)
