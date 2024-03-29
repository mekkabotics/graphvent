package graphvent

import (
  "testing"
  "runtime/debug"
  "time"
  badger "github.com/dgraph-io/badger/v3"
)

func NewSimpleListener(ctx *Context, buffer int) (*Node, *ListenerExt, error) {
  listener_extension := NewListenerExt(buffer)
  listener, err := NewNode(ctx,
                      nil,
                      "LockableNode",
                      10,
                      nil,
                      listener_extension,
                      NewLockableExt(nil))

  return listener, listener_extension, err
}

func logTestContext(t * testing.T, components []string) *Context {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  ctx, err := NewContext(db, NewConsoleLogger(components))
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

func testSend(t *testing.T, ctx *Context, signal Signal, source, destination *Node) (ResponseSignal, []Signal) {
  source_listener, err := GetExt[ListenerExt](source)
  fatalErr(t, err)

  messages := []SendMsg{{destination.ID, signal}}
  fatalErr(t, ctx.Send(source, messages))

  response, signals, err := WaitForResponse(source_listener.Chan, time.Millisecond*10, signal.ID())
  fatalErr(t, err)

  return response, signals
}
