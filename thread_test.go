package graphvent

import (
  "testing"
  "time"
)

func TestNewEvent(t * testing.T) {
  ctx := testContext(t)

  t1, err := NewThread(ctx, "Test thread 1", []Lockable{}, ThreadActions{}, ThreadHandlers{})
  fatalErr(t, err)

  go func(thread Thread) {
    time.Sleep(1*time.Second)
    SendUpdate(ctx, t1, CancelSignal(nil))
  }(t1)

  err = RunThread(ctx, t1)
  fatalErr(t, err)
}
