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
  Type() string
  String() string
}

// BaseSignal is the most basic type of signal, it has no additional data
type BaseSignal struct {
  FDirection SignalDirection `json:"direction"`
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

func (signal BaseSignal) Type() string {
  return signal.FType
}

func NewBaseSignal(_type string, direction SignalDirection) BaseSignal {
  signal := BaseSignal{
    FDirection: direction,
    FType: _type,
  }
  return signal
}

func NewDownSignal(_type string) BaseSignal {
  return NewBaseSignal(_type, Down)
}

func NewSignal(_type string) BaseSignal {
  return NewBaseSignal(_type, Up)
}

func NewDirectSignal(_type string) BaseSignal {
  return NewBaseSignal(_type, Direct)
}

var AbortSignal = NewBaseSignal("abort", Down)
var StopSignal = NewBaseSignal("stop", Down)

type IDSignal struct {
  BaseSignal
  ID NodeID `json:"id"`
}

func NewIDSignal(_type string, direction SignalDirection, id NodeID) IDSignal {
  return IDSignal{
    BaseSignal: NewBaseSignal(_type, direction),
    ID: id,
  }
}

type StatusSignal struct {
  IDSignal
  Status string
}

func NewStatusSignal(status string, source NodeID) StatusSignal {
  return StatusSignal{
    IDSignal: NewIDSignal("status", Up, source),
    Status: status,
  }
}

type StartChildSignal struct {
  IDSignal
  Action string `json:"action"`
}

func NewStartChildSignal(child_id NodeID, action string) StartChildSignal {
  return StartChildSignal{
    IDSignal: NewIDSignal("start_child", Direct, child_id),
    Action: action,
  }
}
