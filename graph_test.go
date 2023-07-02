package graphvent

import (
  "testing"
  "fmt"
  "time"
  "runtime/pprof"
  "os"
  badger "github.com/dgraph-io/badger/v3"
)

type GraphTester testing.T
const listner_timeout = 50 * time.Millisecond

func (t * GraphTester) WaitForValue(ctx * GraphContext, listener chan GraphSignal, signal_type string, source GraphNode, timeout time.Duration, str string) GraphSignal {
  timeout_channel := time.After(timeout)
  for true {
    select {
    case signal := <- listener:
      if signal == nil {
        ctx.Log.Logf("test", "SIGNAL_CHANNEL_CLOSED: %s", listener)
        t.Fatal(str)
      }
      if signal.Type() == signal_type {
        ctx.Log.Logf("test", "SIGNAL_TYPE_FOUND: %s - %s %+v\n", signal.Type(), signal.Source(), listener)
        if source == nil {
          if signal.Source() == "" {
            return signal
          }
        } else {
          if signal.Source() == source.ID() {
            return signal
          }
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

func logTestContext(t * testing.T, components []string) * GraphContext {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  return NewGraphContext(db, NewConsoleLogger(components), StateLoadMap{}, NodeLoadMap{}, TypeList{}, ObjTypeMap{}, FieldMap{}, FieldMap{}, FieldMap{})
}

func testContext(t * testing.T) * GraphContext {
  db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
  if err != nil {
    t.Fatal(err)
  }

  return NewGraphContext(db, NewConsoleLogger([]string{}), StateLoadMap{}, NodeLoadMap{}, TypeList{}, ObjTypeMap{}, FieldMap{}, FieldMap{}, FieldMap{})
}

func fatalErr(t * testing.T, err error) {
  if err != nil {
    t.Fatal(err)
  }
}
