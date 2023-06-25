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

func TestEventWithRequirement(t * testing.T) {
  ctx := logTestContext(t, []string{"lockable", "thread"})

  l1, err := NewLockable(ctx, "Test Lockable 1", []Lockable{})
  fatalErr(t, err)

  t1, err := NewThread(ctx, "Test Thread 1", []Lockable{l1}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  go func (thread Thread) {
    time.Sleep(10*time.Millisecond)
    _, err := UseStates(ctx, []GraphNode{l1}, func(states []NodeState) (interface{}, error) {
      ser, err := json.MarshalIndent(states[0], "", "  ")
      fatalErr(t, err)

      fmt.Printf("\n%s\n", ser)
      return nil, nil
    })
    fatalErr(t, err)
    SendUpdate(ctx, t1, CancelSignal(nil))
  }(t1)
  fatalErr(t, err)

  err = RunThread(ctx, t1)
  fatalErr(t, err)

  _, err = UseStates(ctx, []GraphNode{l1}, func(states []NodeState) (interface{}, error) {
    ser, err := json.MarshalIndent(states[0], "", "  ")
    fatalErr(t, err)

    fmt.Printf("\n%s\n", ser)
    return nil, nil
  })
  fatalErr(t, err)
}
