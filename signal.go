package graphvent

import (
  "encoding/json"
)

const (
  StopSignalType = SignalType("STOP")
  StatusSignalType = SignalType("STATUS")
  LinkSignalType = SignalType("LINK")
  LockSignalType = SignalType("LOCK")
  ReadSignalType = SignalType("READ")
  ReadResultSignalType = SignalType("READ_RESULT")
)

type SignalDirection int
const (
  Up SignalDirection = iota
  Down
  Direct
)

type SignalType string
func (signal_type SignalType) String() string {
  return string(signal_type)
}

type Signal interface {
  Serializable[SignalType]
  Direction() SignalDirection
  Permission() Action
}

type BaseSignal struct {
  SignalDirection SignalDirection `json:"direction"`
  SignalType SignalType `json:"type"`
}

func (signal BaseSignal) Type() SignalType {
  return signal.SignalType
}

func (signal BaseSignal) Permission() Action {
  return MakeAction(signal.Type())
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
    IDSignal: NewIDSignal(StatusSignalType, Up, source),
    Status: status,
  }
}

type StateSignal struct {
  BaseSignal
  State string `json:"state"`
}

func (signal StateSignal) Serialize() ([]byte, error) {
  return json.MarshalIndent(signal, "", "  ")
}

func (signal StateSignal) String() string {
  ser, _ := signal.Serialize()
  return string(ser)
}

func NewLinkSignal(state string) StateSignal {
  return StateSignal{
    BaseSignal: NewDirectSignal(LinkSignalType),
    State: state,
  }
}

func NewLockSignal(state string) StateSignal {
  return StateSignal{
    BaseSignal: NewDirectSignal(LockSignalType),
    State: state,
  }
}

func (signal StateSignal) Permission() Action {
  return MakeAction(signal.Type(), signal.State)
}

type ReadSignal struct {
  BaseSignal
  Extensions map[ExtType][]string `json:"extensions"`
}

func NewReadSignal(exts map[ExtType][]string) ReadSignal {
  return ReadSignal{
    BaseSignal: NewDirectSignal(ReadSignalType),
    Extensions: exts,
  }
}

type ReadResultSignal struct {
  BaseSignal
  Extensions map[ExtType]map[string]interface{} `json:"extensions"`
}

func NewReadResultSignal(exts map[ExtType]map[string]interface{}) ReadResultSignal {
  return ReadResultSignal{
    BaseSignal: NewDirectSignal(ReadResultSignalType),
    Extensions: exts,
  }
}
