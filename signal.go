package graphvent

import (
  "fmt"
  "time"

  "capnproto.org/go/capnp/v3"
  "github.com/google/uuid"
  schema "github.com/mekkanized/graphvent/signal"
)

type SignalDirection int
const (
  Up SignalDirection = iota
  Down
  Direct
)

type SignalHeader struct {
  Direction SignalDirection `gv:"0"`
  ID uuid.UUID `gv:"1"`
  ReqID uuid.UUID `gv:"2"`
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

func SerializeHeader(header SignalHeader, root schema.SignalHeader) error {
  root.SetDirection(uint8(header.Direction))
  id_ser, err := header.ID.MarshalBinary()
  if err != nil {
    return err
  }
  root.SetId(id_ser)

  req_id_ser, err := header.ReqID.MarshalBinary()
  if err != nil {
    return err
  }
  root.SetReqID(req_id_ser)
  return nil
}

func DeserializeHeader(header schema.SignalHeader) (SignalHeader, error) {
  id_ser, err := header.Id()
  if err != nil {
    return SignalHeader{}, err
  }
  id, err := uuid.FromBytes(id_ser)
  if err != nil {
    return SignalHeader{}, err
  }

  req_id_ser, err := header.ReqID()
  if err != nil {
    return SignalHeader{}, err
  }
  req_id, err := uuid.FromBytes(req_id_ser)
  if err != nil {
    return SignalHeader{}, err
  }

  return SignalHeader{
    ID: id,
    ReqID: req_id,
    Direction: SignalDirection(header.Direction()),
  }, nil
}

type CreateSignal struct {
  SignalHeader
}

func (signal *CreateSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *CreateSignal) Permission() Tree {
  return Tree{
    uint64(CreateSignalType): nil,
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
func (signal *StartSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *StartSignal) Permission() Tree {
  return Tree{
    uint64(StartSignalType): nil,
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
func (signal *StopSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *StopSignal) Permission() Tree {
  return Tree{
    uint64(StopSignalType): nil,
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
func (signal *ErrorSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *ErrorSignal) MarshalBinary() ([]byte, error) {
  arena := capnp.SingleSegment(nil)
  msg, seg, err := capnp.NewMessage(arena)
  if err != nil {
    return nil, err
  }

  root, err := schema.NewRootErrorSignal(seg)
  if err != nil {
    return nil, err
  }

  root.SetError(signal.Error)

  header, err := root.NewHeader()
  if err != nil {
    return nil, err
  }

  err = SerializeHeader(signal.SignalHeader, header)
  if err != nil {
    return nil, err
  }

  return msg.Marshal()
}
func (signal *ErrorSignal) Deserialize(ctx *Context, data []byte) error {
  msg, err := capnp.Unmarshal(data)
  if err != nil {
    return err
  }

  root, err := schema.ReadRootErrorSignal(msg)
  if err != nil {
    return err
  }

  header, err := root.Header()
  if err != nil {
    return err
  }

  signal.Error, err = root.Error()
  if err != nil {
    return err
  }
  signal.SignalHeader, err = DeserializeHeader(header)
  if err != nil {
    return err
  }

  return nil
}
func (signal *ErrorSignal) Permission() Tree {
  return Tree{
    uint64(ErrorSignalType): nil,
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
func (signal *ACLTimeoutSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *ACLTimeoutSignal) Permission() Tree {
  return Tree{
    uint64(ACLTimeoutSignalType): nil,
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
  Source NodeID
  Status string
}
func (signal *StatusSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *StatusSignal) Permission() Tree {
  return Tree{
    uint64(StatusSignalType): nil,
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
func (signal *LinkSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}

const (
  LinkActionBase = "LINK_ACTION"
  LinkActionAdd = "ADD"
)

func (signal *LinkSignal) Permission() Tree {
  return Tree{
    uint64(LinkSignalType): Tree{
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
func (signal *LockSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}

const (
  LockStateBase = "LOCK_STATE"
)

func (signal *LockSignal) Permission() Tree {
  return Tree{
    uint64(LockSignalType): Tree{
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
func (signal *ReadSignal) MarshalBinary() ([]byte, error) {
  arena := capnp.SingleSegment(nil)
  msg, seg, err := capnp.NewMessage(arena)
  if err != nil {
    return nil, err
  }

  root, err := schema.NewRootReadSignal(seg)
  if err != nil {
    return nil, err
  }

  header, err := root.NewHeader()
  if err != nil {
    return nil, err
  }

  err = SerializeHeader(signal.SignalHeader, header)
  if err != nil {
    return nil, err
  }

  extensions, err := root.NewExtensions(int32(len(signal.Extensions)))
  if err != nil {
    return nil, err
  }

  i := 0
  for ext_type, fields := range(signal.Extensions) {
    extension := extensions.At(i)
    extension.SetType(uint64(ext_type))
    f, err := extension.NewFields(int32(len(fields)))
    if err != nil {
      return nil, err
    }

    for j, field := range(fields) {
      err := f.Set(j, field)
      if err != nil {
        return nil, err
      }
    }

    i += 1
  }

  return msg.Marshal()
}
func (signal *ReadSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}

func (signal *ReadSignal) Permission() Tree {
  ret := Tree{}
  for ext, fields := range(signal.Extensions) {
    field_tree := Tree{}
    for _, field := range(fields) {
      field_tree[Hash(FieldNameBase, field)]  = nil
    }
    ret[uint64(ext)] = field_tree
  }
  return Tree{uint64(ReadSignalType): ret}
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
func (signal *ReadResultSignal) Header() *SignalHeader {
  return &signal.SignalHeader
}
func (signal *ReadResultSignal) Permission() Tree {
  return Tree{
    uint64(ReadResultSignalType): nil,
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

