package graphvent

import (
  "testing"
  "fmt"
  "time"
  "runtime/pprof"
  "runtime/debug"
  "os"
  badger "github.com/dgraph-io/badger/v3"
)

type GraphTester testing.T
const listner_timeout = 50 * time.Millisecond

func (t * GraphTester) WaitForStatus(ctx * Context, listener chan Signal, status string, timeout time.Duration, str string) Signal {
  timeout_channel := time.After(timeout)
  for true {
    select {
    case signal := <- listener:
      if signal == nil {
        ctx.Log.Logf("test", "SIGNAL_CHANNEL_CLOSED: %s", listener)
        t.Fatal(str)
      }
      if signal.Type() == "status" {
        sig, ok := signal.(StatusSignal)
        if ok == true {
          ctx.Log.Logf("test", "Status received: %s", sig.Status)
          if sig.Status == status {
            return signal
          }
        } else {
          ctx.Log.Logf("test", "Failed to cast status to StatusSignal: %+v", signal)
        }
      }
    case <-timeout_channel:
      pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
      t.Fatal(str)
      return nil
    }
  }
  return nil
}

func (t * GraphTester) CheckForNone(listener chan Signal, str string) {
  timeout := time.After(listner_timeout)
  select {
  case sig := <- listener:
    pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
    t.Fatal(fmt.Sprintf("%s : %+v", str, sig))
  case <-timeout:
  }
}

const SimpleListenerNodeType = NodeType("SIMPLE_LISTENER")

func NewSimpleListener(ctx *Context, buffer int) (*Node, *ListenerExt) {
  policy := NewAllNodesPolicy([]string{"signal.status", "requirements.write", "requirements.read", "dependencies.write", "dependencies.read", "owner.read", "owner.write"})
  listener_extension := NewListenerExt(buffer)
  listener := NewNode(ctx,
                      RandID(),
                      SimpleListenerNodeType,
                      listener_extension,
                      NewACLExt(&policy),
                      NewLockableExt(nil, nil, nil, nil))

  return listener, listener_extension
}

func logTestContext(t * testing.T, components []string) *Context {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  ctx, err := NewContext(db, NewConsoleLogger(components))
  fatalErr(t, err)

  err = ctx.RegisterNodeType(SimpleListenerNodeType, []ExtType{ACLExtType, ListenerExtType, LockableExtType})
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
