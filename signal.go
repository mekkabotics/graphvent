package graphvent

import (
  "encoding/json"
)

type SignalDirection int
const (
  Up SignalDirection = iota
  Down
  Direct
)

// GraphSignals are passed around the event tree/resource DAG and cast by Type()
type GraphSignal interface {
  // How to propogate the signal
  Direction() SignalDirection
  Source() NodeID
  Type() string
  String() string
}

// BaseSignal is the most basic type of signal, it has no additional data
type BaseSignal struct {
  FDirection SignalDirection `json:"direction"`
  FSource NodeID `json:"source"`
  FType string `json:"type"`
}

func (state BaseSignal) String() string {
  ser, err := json.Marshal(state)
  if err != nil {
    return "STATE_SER_ERR"
  }
  return string(ser)
}

func (signal BaseSignal) Direction() SignalDirection {
  return signal.FDirection
}

func (signal BaseSignal) Source() NodeID {
  return signal.FSource
}

func (signal BaseSignal) Type() string {
  return signal.FType
}

func NewBaseSignal(source Node, _type string, direction SignalDirection) BaseSignal {
  var source_id NodeID = NodeID{}
  if source != nil {
    source_id = source.ID()
  }

  signal := BaseSignal{
    FDirection: direction,
    FSource: source_id,
    FType: _type,
  }
  return signal
}

func NewDownSignal(source Node, _type string) BaseSignal {
  return NewBaseSignal(source, _type, Down)
}

func NewSignal(source Node, _type string) BaseSignal {
  return NewBaseSignal(source, _type, Up)
}

func NewDirectSignal(source Node, _type string) BaseSignal {
  return NewBaseSignal(source, _type, Direct)
}

func AbortSignal(source Node) BaseSignal {
  return NewBaseSignal(source, "abort", Down)
}

func CancelSignal(source Node) BaseSignal {
  return NewBaseSignal(source, "cancel", Down)
}
