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

func (t * GraphTester) WaitForStatus(ctx * Context, listener chan GraphSignal, status string, timeout time.Duration, str string) GraphSignal {
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
          if sig.Status == status {
            return signal
          }
          ctx.Log.Logf("test", "Different status received: %s", sig.Status)
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

func (t * GraphTester) CheckForNone(listener chan GraphSignal, str string) {
  timeout := time.After(listner_timeout)
  select {
  case sig := <- listener:
    pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
    t.Fatal(fmt.Sprintf("%s : %+v", str, sig))
  case <-timeout:
  }
}

func logTestContext(t * testing.T, components []string) * Context {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  return NewContext(db, NewConsoleLogger(components))
}

func testContext(t * testing.T) * Context {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  return NewContext(db, NewConsoleLogger([]string{}))
}

func fatalErr(t * testing.T, err error) {
  if err != nil {
    debug.PrintStack()
    t.Fatal(err)
  }
}
