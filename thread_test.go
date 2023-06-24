package graphvent

import (
  "testing"
  "time"
  "encoding/json"
  "fmt"
)

func TestNewEvent(t * testing.T) {
  ctx := testContext(t)

  t1, err := NewThread(ctx, "Test thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  go func(thread Thread) {
    time.Sleep(10*time.Millisecond)
    SendUpdate(ctx, t1, CancelSignal(nil))
  }(t1)

  err = RunThread(ctx, t1)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{t1}, func(states []NodeState) (interface{}, error) {
    ser, err := json.MarshalIndent(states, "", "  ")
    fatalErr(t, err)

    fmt.Printf("\n%s\n", ser)

    return nil, nil
  })
}
