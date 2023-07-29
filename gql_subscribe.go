package graphvent
import (
  "github.com/graphql-go/graphql"
)

func SubscribeNode(p graphql.ResolveParams) (interface{}, error) {
  return SubscribeFn(p, false, func(ctx *Context, server *Node, ext *GQLExt, signal Signal, p graphql.ResolveParams)(interface{}, error) {
    return nil, nil
  })
}

func SubscribeSelf(p graphql.ResolveParams) (interface{}, error) {
  return SubscribeFn(p, true, func(ctx *Context, server *Node, ext *GQLExt, signal Signal, p graphql.ResolveParams)(interface{}, error) {
    return server, nil
  })
}

func SubscribeFn(p graphql.ResolveParams, send_nil bool, fn func(*Context, *Node, *GQLExt, Signal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
  ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  c := make(chan interface{})
  go func(c chan interface{}, ext *GQLExt, server *Node) {
    ctx.Context.Log.Logf("gqlws", "GQL_SUBSCRIBE_THREAD_START")
    sig_c := make(chan Signal, 1)
    if send_nil == true {
      sig_c <- nil
    }
    for {
      val, ok := <- sig_c
      if ok == false {
        return
      }
      ret, err := fn(ctx.Context, server, ext, val, p)
      if err != nil {
        ctx.Context.Log.Logf("gqlws", "type convertor error %s", err)
        return
      }
      c <- ret
    }
  }(c, ctx.Ext, ctx.Server)
  return c, nil
}

var SubscriptionSelf = NewField(func()*graphql.Field{
  subscription_self := &graphql.Field{
    Type: InterfaceNode.Default,
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      return p.Source, nil
    },
    Subscribe: SubscribeSelf,
  }

  return subscription_self
})

var SubscriptionNode = NewField(func()*graphql.Field{
  subscription_node := &graphql.Field{
    Type: InterfaceNode.Default,
    Args: graphql.FieldConfigArgument{
    "id": &graphql.ArgumentConfig{
        Type: graphql.String,
      },
    },
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      return p.Source, nil
    },
    Subscribe: SubscribeNode,
  }

  return subscription_node
})

