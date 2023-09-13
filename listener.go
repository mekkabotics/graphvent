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

func (listener *ListenerExt) Type() ExtType {
  return ListenerExtType
}

// Send the signal to the channel, logging an overflow if it occurs
func (ext *ListenerExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  ctx.Log.Logf("listener", "LISTENER_PROCESS: %s - %+v", node.ID, reflect.TypeOf(signal))
  select {
  case ext.Chan <- signal:
  default:
    ctx.Log.Logf("listener", "LISTENER_OVERFLOW: %s", node.ID)
  }
  return nil
}
