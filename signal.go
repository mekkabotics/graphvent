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

type SignalType string

type Signal interface {
  Serializable[SignalType]
  Direction() SignalDirection
}

type BaseSignal struct {
  SignalDirection SignalDirection `json:"direction"`
  SignalType SignalType `json:"type"`
}

func (signal BaseSignal) Type() SignalType {
  return signal.SignalType
}

func (signal BaseSignal) Direction() SignalDirection {
  return signal.SignalDirection
}

func (signal BaseSignal) Serialize() ([]byte, error) {
  return json.MarshalIndent(signal, "", "  ")
}

func NewBaseSignal(signal_type SignalType, direction SignalDirection) BaseSignal {
  signal := BaseSignal{
    SignalDirection: direction,
    SignalType: signal_type,
  }
  return signal
}

func NewDownSignal(signal_type SignalType) BaseSignal {
  return NewBaseSignal(signal_type, Down)
}

func NewUpSignal(signal_type SignalType) BaseSignal {
  return NewBaseSignal(signal_type, Up)
}

func NewDirectSignal(signal_type SignalType) BaseSignal {
  return NewBaseSignal(signal_type, Direct)
}

const StopSignalType = SignalType("STOP")
var StopSignal = NewDownSignal(StopSignalType)

type IDSignal struct {
  BaseSignal
  ID NodeID `json:"id"`
}

func (signal IDSignal) String() string {
  ser, err := json.Marshal(signal)
  if err != nil {
    return "STATE_SER_ERR"
  }
  return string(ser)
}

func NewIDSignal(signal_type SignalType, direction SignalDirection, id NodeID) IDSignal {
  return IDSignal{
    BaseSignal: NewBaseSignal(signal_type, direction),
    ID: id,
  }
}

type StatusSignal struct {
  IDSignal
  Status string `json:"status"`
}

func (signal StatusSignal) String() string {
  ser, err := json.Marshal(signal)
  if err != nil {
    return "STATE_SER_ERR"
  }
  return string(ser)
}

func NewStatusSignal(status string, source NodeID) StatusSignal {
  return StatusSignal{
    IDSignal: NewIDSignal("status", Up, source),
    Status: status,
  }
}

const LinkSignalType = SignalType("LINK")
type LinkSignal struct {
  BaseSignal
  State string `json:"state"`
}

func (signal LinkSignal) Serialize() ([]byte, error) {
  return json.MarshalIndent(signal, "", "  ")
}

func (signal LinkSignal) String() string {
  ser, _ := signal.Serialize()
  return string(ser)
}

func NewLinkSignal(state string) LinkSignal {
  return LinkSignal{
    BaseSignal: NewDirectSignal(LinkSignalType),
    State: state,
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
