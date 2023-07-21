package graphvent
import (
  "fmt"
  "github.com/graphql-go/graphql"
)

func GQLSubscribeSignal(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, false, func(ctx *Context, server *GQLThread, signal GraphSignal, p graphql.ResolveParams)(interface{}, error) {
    return signal, nil
  })
}

func GQLSubscribeSelf(p graphql.ResolveParams) (interface{}, error) {
  return GQLSubscribeFn(p, true, func(ctx *Context, server *GQLThread, signal GraphSignal, p graphql.ResolveParams)(interface{}, error) {
    return server, nil
  })
}

func GQLSubscribeFn(p graphql.ResolveParams, send_nil bool, fn func(*Context, *GQLThread, GraphSignal, graphql.ResolveParams)(interface{}, error))(interface{}, error) {
  server, ok := p.Context.Value("gql_server").(*GQLThread)
  if ok == false {
    return nil, fmt.Errorf("Failed to get gql_server from context and cast to GQLServer")
  }

  ctx, ok := p.Context.Value("graph_context").(*Context)
  if ok == false {
    return nil, fmt.Errorf("Failed to get graph_context from context and cast to Context")
  }

  c := make(chan interface{})
  go func(c chan interface{}, server *GQLThread) {
    ctx.Log.Logf("gqlws", "GQL_SUBSCRIBE_THREAD_START")
    sig_c := UpdateChannel(server, 1, RandID())
    if send_nil == true {
      sig_c <- nil
    }
    for {
      val, ok := <- sig_c
      if ok == false {
        return
      }
      ret, err := fn(ctx, server, val, p)
      if err != nil {
        ctx.Log.Logf("gqlws", "type convertor error %s", err)
        return
      }
      c <- ret
    }
  }(c, server)
  return c, nil
}

var GQLSubscriptionSelf = NewField(func()*graphql.Field{
  gql_subscription_self := &graphql.Field{
    Type: GQLTypeGQLThread.Type,
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

