package graphvent

import (
  "fmt"
  "time"

 "github.com/google/uuid"
)

type SignalDirection uint8
const (
  Up SignalDirection = iota
  Down
  Direct
)

type SignalHeader struct {
  Direction SignalDirection `gv:"direction"`
  ID uuid.UUID `gv:"id"`
  ReqID uuid.UUID `gv:"req_id"`
}

type Signal interface {
  Header() *SignalHeader
  Permission() Tree
}

func WaitForResponse(listener chan Signal, timeout time.Duration, req_id uuid.UUID) (Signal, error) {
  var timeout_channel <- chan time.Time
  if timeout > 0 {
    timeout_channel = time.After(timeout)
  }

  for true {
    select {
    case signal := <- listener:
      if signal == nil {
        return nil, fmt.Errorf("LISTENER_CLOSED")
      }
      if signal.Header().ReqID == req_id {
        return signal, nil
      }
    case <-timeout_channel:
      return nil, fmt.Errorf("LISTENER_TIMEOUT")
    }
  }
  return nil, fmt.Errorf("UNREACHABLE")
}

func WaitForSignal[S Signal](listener chan Signal, timeout time.Duration, check func(S)bool) (S, error) {
  var zero S
  var timeout_channel <- chan time.Time
  if timeout > 0 {
    timeout_channel = time.After(timeout)
  }
  for true {
    select {
    case signal := <- listener:
      if signal == nil {
        return zero, fmt.Errorf("LISTENER_CLOSED")
      }
      sig, ok := signal.(S)
      if ok == true {
        if check(sig) == true {
          return sig, nil
        }
      }
    case <-timeout_channel:
      return zero, fmt.Errorf("LISTENER_TIMEOUT")
    }
  }
  return zero, fmt.Errorf("LOOP_ENDED")
}

func NewSignalHeader(direction SignalDirection) SignalHeader {
  id := uuid.New()
  header := SignalHeader{
    ID: id,
    ReqID: id,
    Direction: direction,
  }
  return header
}

func NewRespHeader(req_id uuid.UUID, direction SignalDirection) SignalHeader {
  header := SignalHeader{
    ID: uuid.New(),
    ReqID: req_id,
    Direction: direction,
  }
  return header
}

type CreateSignal struct {
  SignalHeader
}

func (signal CreateSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal CreateSignal) Permission() Tree {
  return Tree{
    SerializedType(CreateSignalType): nil,
  }
}

func NewCreateSignal() *CreateSignal {
  return &CreateSignal{
    NewSignalHeader(Direct),
  }
}

type StartSignal struct {
  SignalHeader
}
func (signal StartSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal StartSignal) Permission() Tree {
  return Tree{
    SerializedType(StartSignalType): nil,
  }
}
func NewStartSignal() *StartSignal {
  return &StartSignal{
    NewSignalHeader(Direct),
  }
}

type StopSignal struct {
  SignalHeader
}
func (signal StopSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal StopSignal) Permission() Tree {
  return Tree{
    SerializedType(StopSignalType): nil,
  }
}
func NewStopSignal() *StopSignal {
  return &StopSignal{
    NewSignalHeader(Direct),
  }
}

type ErrorSignal struct {
  SignalHeader
  Error string
}
func (signal ErrorSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal ErrorSignal) Permission() Tree {
  return Tree{
    SerializedType(ErrorSignalType): nil,
  }
}
func NewErrorSignal(req_id uuid.UUID, fmt_string string, args ...interface{}) Signal {
  return &ErrorSignal{
    NewRespHeader(req_id, Direct),
    fmt.Sprintf(fmt_string, args...),
  }
}

type ACLTimeoutSignal struct {
  SignalHeader
}
func (signal ACLTimeoutSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal ACLTimeoutSignal) Permission() Tree {
  return Tree{
    SerializedType(ACLTimeoutSignalType): nil,
  }
}
func NewACLTimeoutSignal(req_id uuid.UUID) *ACLTimeoutSignal {
  sig := &ACLTimeoutSignal{
    NewRespHeader(req_id, Direct),
  }
  return sig
}

type StatusSignal struct {
  SignalHeader
  Source NodeID `gv:"source"`
  Status string `gv:"status"`
}
func (signal StatusSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal StatusSignal) Permission() Tree {
  return Tree{
    SerializedType(StatusSignalType): nil,
  }
}
func NewStatusSignal(source NodeID, status string) *StatusSignal {
  return &StatusSignal{
    NewSignalHeader(Up),
    source,
    status,
  }
}

type LinkSignal struct {
  SignalHeader
  NodeID
  Action string
}
func (signal LinkSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}

const (
  LinkActionBase = "LINK_ACTION"
  LinkActionAdd = "ADD"
)

func (signal LinkSignal) Permission() Tree {
  return Tree{
    SerializedType(LinkSignalType): Tree{
      Hash(LinkActionBase, signal.Action): nil,
    },
  }
}
func NewLinkSignal(action string, id NodeID) Signal {
  return &LinkSignal{
    NewSignalHeader(Direct),
    id,
    action,
  }
}

type LockSignal struct {
  SignalHeader
  State string
}
func (signal LockSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}

const (
  LockStateBase = "LOCK_STATE"
)

func (signal LockSignal) Permission() Tree {
  return Tree{
    SerializedType(LockSignalType): Tree{
      Hash(LockStateBase, signal.State): nil,
    },
  }
}

func NewLockSignal(state string) *LockSignal {
  return &LockSignal{
    NewSignalHeader(Direct),
    state,
  }
}

type ReadSignal struct {
  SignalHeader
  Extensions map[ExtType][]string `json:"extensions"`
}
func (signal ReadSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}

func (signal ReadSignal) Permission() Tree {
  ret := Tree{}
  for ext, fields := range(signal.Extensions) {
    field_tree := Tree{}
    for _, field := range(fields) {
      field_tree[Hash(FieldNameBase, field)]  = nil
    }
    ret[SerializedType(ext)] = field_tree
  }
  return Tree{SerializedType(ReadSignalType): ret}
}
func NewReadSignal(exts map[ExtType][]string) *ReadSignal {
  return &ReadSignal{
    NewSignalHeader(Direct),
    exts,
  }
}

type ReadResultSignal struct {
  SignalHeader
  NodeID NodeID
  NodeType NodeType
  Extensions map[ExtType]map[string]SerializedValue
}
func (signal ReadResultSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal ReadResultSignal) Permission() Tree {
  return Tree{
    SerializedType(ReadResultSignalType): nil,
  }
}
func NewReadResultSignal(req_id uuid.UUID, node_id NodeID, node_type NodeType, exts map[ExtType]map[string]SerializedValue) *ReadResultSignal {
  return &ReadResultSignal{
    NewRespHeader(req_id, Direct),
    node_id,
    node_type,
    exts,
  }
}

