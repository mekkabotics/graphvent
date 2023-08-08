package graphvent

import (
  "testing"
  "runtime/debug"
  badger "github.com/dgraph-io/badger/v3"
)

const SimpleListenerNodeType = NodeType("SIMPLE_LISTENER")

func NewSimpleListener(ctx *Context, buffer int) (*Node, *ListenerExt) {
  listener_extension := NewListenerExt(buffer)
  listener := NewNode(ctx,
                      nil,
                      SimpleListenerNodeType,
                      10,
                      nil,
                      listener_extension,
                      NewLockableExt())

  return listener, listener_extension
}

func logTestContext(t * testing.T, components []string) *Context {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  ctx, err := NewContext(db, NewConsoleLogger(components))
  fatalErr(t, err)

  err = ctx.RegisterNodeType(SimpleListenerNodeType, []ExtType{ListenerExtType, LockableExtType})
  fatalErr(t, err)

  return ctx
}

func testContext(t * testing.T) * Context {
  return logTestContext(t, []string{})
}

func fatalErr(t * testing.T, err error) {
  if err != nil {
    debug.PrintStack()
    t.Fatal(err)
  }
}
