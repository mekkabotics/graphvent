package graphvent
import (
  "github.com/graphql-go/graphql"
)

func GQLSubscribeSignal(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, false, func(ctx *Context, server *Node, ext *GQLExt, signal Signal, p graphql.ResolveParams)(interface{}, error) {
    return signal, nil
  })
}

func GQLSubscribeSelf(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, true, func(ctx *Context, server *Node, ext *GQLExt, signal Signal, p graphql.ResolveParams)(interface{}, error) {
    return server, nil
  })
}

func GQLSubscribeFn(p graphql.ResolveParams, send_nil bool, fn func(*Context, *Node, *GQLExt, Signal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
  _, ctx, err := PrepResolve(p)
  if err != nil {
    return nil, err
  }

  c := make(chan interface{})
  go func(c chan interface{}, ext *GQLExt, server *Node) {
    ctx.Context.Log.Logf("gqlws", "GQL_SUBSCRIBE_THREAD_START")
    sig_c := ext.NewSubscriptionChannel(1)
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

var GQLSubscriptionSelf = NewField(func()*graphql.Field{
  gql_subscription_self := &graphql.Field{
    Type: GQLInterfaceThread.Default,
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      return p.Source, nil
    },
    Subscribe: GQLSubscribeSelf,
  }

  return gql_subscription_self
})

var GQLSubscriptionUpdate = NewField(func()*graphql.Field{
  gql_subscription_update := &graphql.Field{
    Type: GQLTypeSignal.Type,
    Resolve: func(p graphql.ResolveParams) (interface{}, error) {
      return p.Source, nil
    },
    Subscribe: GQLSubscribeSignal,
  }
  return gql_subscription_update
})

