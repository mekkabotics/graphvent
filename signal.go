package graphvent

import (
  "fmt"
  "time"

 "github.com/google/uuid"
)

type TimeoutSignal struct {
  ResponseHeader
}

func NewTimeoutSignal(req_id uuid.UUID) *TimeoutSignal {
  return &TimeoutSignal{
    NewResponseHeader(req_id),
  }
}

func (signal TimeoutSignal) String() string {
  return fmt.Sprintf("TimeoutSignal(%s)", &signal.ResponseHeader)
}

type SignalHeader struct {
  Id uuid.UUID `gv:"id"`
}

func (signal SignalHeader) ID() uuid.UUID {
  return signal.Id
}

func (header SignalHeader) String() string {
   return fmt.Sprintf("SignalHeader(%s)", header.Id)
}

type ResponseSignal interface {
  Signal
  ResponseID() uuid.UUID
}

type ResponseHeader struct {
  SignalHeader
  ReqID uuid.UUID `gv:"req_id"`
}

func (header ResponseHeader) ResponseID() uuid.UUID {
  return header.ReqID
}

func (header ResponseHeader) String() string {
   return fmt.Sprintf("ResponseHeader(%s, %s)", header.Id, header.ReqID)
}

type Signal interface {
  fmt.Stringer
  ID() uuid.UUID
}

func WaitForResponse(listener chan Signal, timeout time.Duration, req_id uuid.UUID) (ResponseSignal, []Signal, error) {
  signals := []Signal{}
  var timeout_channel <- chan time.Time
  if timeout > 0 {
    timeout_channel = time.After(timeout)
  }

  for true {
    select {
    case signal := <- listener:
      if signal == nil {
        return nil, signals, fmt.Errorf("LISTENER_CLOSED")
      }

      resp_signal, ok := signal.(ResponseSignal)
      if ok == true && resp_signal.ResponseID() == req_id {
        return resp_signal, signals, nil
      } else {
        signals = append(signals, signal)
      }

    case <-timeout_channel:
      return nil, signals, fmt.Errorf("LISTENER_TIMEOUT")
    }
  }
  return nil, signals, fmt.Errorf("UNREACHABLE")
}

//TODO: Add []Signal return as well for other signals
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

func NewSignalHeader() SignalHeader {
  return SignalHeader{
    uuid.New(),
  }
}

func NewResponseHeader(req_id uuid.UUID) ResponseHeader {
  return ResponseHeader{
    NewSignalHeader(),
    req_id,
  }
}

type SuccessSignal struct {
  ResponseHeader
}

func (signal SuccessSignal) String() string {
  return fmt.Sprintf("SuccessSignal(%s)", signal.ResponseHeader)
}

func NewSuccessSignal(req_id uuid.UUID) *SuccessSignal {
  return &SuccessSignal{
    NewResponseHeader(req_id),
  }
}

type ErrorSignal struct {
  ResponseHeader
  Error string
}
func (signal ErrorSignal) String() string {
  return fmt.Sprintf("ErrorSignal(%s, %s)", signal.ResponseHeader, signal.Error)
}
func NewErrorSignal(req_id uuid.UUID, fmt_string string, args ...interface{}) *ErrorSignal {
  return &ErrorSignal{
    NewResponseHeader(req_id),
    fmt.Sprintf(fmt_string, args...),
  }
}

type ACLTimeoutSignal struct {
  ResponseHeader
}
func NewACLTimeoutSignal(req_id uuid.UUID) *ACLTimeoutSignal {
  sig := &ACLTimeoutSignal{
    NewResponseHeader(req_id),
  }
  return sig
}

type StatusSignal struct {
  SignalHeader
  Source NodeID `gv:"source"`
  Changes map[ExtType]Changes `gv:"changes"`
}
func (signal StatusSignal) String() string {
  return fmt.Sprintf("StatusSignal(%s, %+v)", signal.SignalHeader, signal.Changes)
}
func NewStatusSignal(source NodeID, changes map[ExtType]Changes) *StatusSignal {
  return &StatusSignal{
    NewSignalHeader(),
    source,
    changes,
  }
}

type LinkSignal struct {
  SignalHeader
  NodeID NodeID
  Action string
}

const (
  LinkActionBase = "LINK_ACTION"
  LinkActionAdd = "ADD"
)

func NewLinkSignal(action string, id NodeID) Signal {
  return &LinkSignal{
    NewSignalHeader(),
    id,
    action,
  }
}

type LockSignal struct {
  SignalHeader
}
func (signal LockSignal) String() string {
  return fmt.Sprintf("LockSignal(%s)", signal.SignalHeader)
}

func NewLockSignal() *LockSignal {
  return &LockSignal{
    NewSignalHeader(),
  }
}

type UnlockSignal struct {
  SignalHeader
}
func (signal UnlockSignal) String() string {
  return fmt.Sprintf("UnlockSignal(%s)", signal.SignalHeader)
}

func NewUnlockSignal() *UnlockSignal {
  return &UnlockSignal{
    NewSignalHeader(),
  }
}


type ReadSignal struct {
  SignalHeader
  Extensions map[ExtType][]string `json:"extensions"`
}

func (signal ReadSignal) String() string {
  return fmt.Sprintf("ReadSignal(%s, %+v)", signal.SignalHeader, signal.Extensions)
}

func NewReadSignal(exts map[ExtType][]string) *ReadSignal {
  return &ReadSignal{
    NewSignalHeader(),
    exts,
  }
}

type ReadResultSignal struct {
  ResponseHeader
  NodeID NodeID
  NodeType NodeType
  Extensions map[ExtType]map[string]any
}

func (signal ReadResultSignal) String() string {
  return fmt.Sprintf("ReadResultSignal(%s, %s, %+v)", signal.ResponseHeader, signal.NodeID, signal.Extensions)
}

func NewReadResultSignal(req_id uuid.UUID, node_id NodeID, node_type NodeType, exts map[ExtType]map[string]any) *ReadResultSignal {
  return &ReadResultSignal{
    NewResponseHeader(req_id),
    node_id,
    node_type,
    exts,
  }
}

