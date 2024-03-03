package graphvent

import (
  "reflect"
)

// A Listener extension provides a channel that can receive signals on a different thread
type ListenerExt struct {
  Buffer int `gv:"buffer"`
  Chan chan Signal
}

func (ext *ListenerExt) PostDeserialize(ctx *Context) error {
  ext.Chan = make(chan Signal, ext.Buffer)
  return nil
}

// Create a new listener extension with a given buffer size
func NewListenerExt(buffer int) *ListenerExt {
  return &ListenerExt{
    Buffer: buffer,
    Chan: make(chan Signal, buffer),
  }
}

// Send the signal to the channel, logging an overflow if it occurs
func (ext *ListenerExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  ctx.Log.Logf("listener", "%s - %+v", node.ID, reflect.TypeOf(signal))
  ctx.Log.Logf("listener_debug", "%s->%s - %+v", source, node.ID, signal)
  select {
  case ext.Chan <- signal:
  default:
    ctx.Log.Logf("listener", "LISTENER_OVERFLOW: %s", node.ID)
  }
  switch sig := signal.(type) {
  case *StatusSignal:
    ctx.Log.Logf("listener_status", "%s - %+v", sig.Source, sig.Changes)
  }
  return nil, nil
}
