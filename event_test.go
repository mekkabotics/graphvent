package graphvent

import (
  "testing"
)

func TestEvent(t *testing.T) {
  ctx := logTestContext(t, []string{"event", "listener"})
  event_listener := NewListenerExt(100)
  _, err := NewNode(ctx, nil, BaseNodeType, 100, nil, NewEventExt(nil, "Test Event"), event_listener)
  fatalErr(t, err)

}
