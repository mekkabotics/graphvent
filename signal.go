package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "crypto/sha512"
  "crypto/ecdsa"
  "crypto/ecdh"
  "crypto/rand"
)

type SignalDirection int
const (
  StopSignalType SignalType = "STOP"
  StatusSignalType          = "STATUS"
  LinkSignalType            = "LINK"
  LockSignalType            = "LOCK"
  ReadSignalType            = "READ"
  ReadResultSignalType      = "READ_RESULT"
  LinkStartSignalType       = "LINK_START"
  ECDHSignalType            = "ECDH"

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

func WaitForSignal[S Signal](ctx * Context, listener *ListenerExt, timeout time.Duration, signal_type SignalType, check func(S)bool) (S, error) {
  var zero S
  timeout_channel := time.After(timeout)
  for true {
    select {
    case signal := <- listener.Chan:
      if signal == nil {
        return zero, fmt.Errorf("LISTENER_CLOSED: %s", signal_type)
      }
      if signal.Type() == signal_type {
        sig, ok := signal.(S)
        if ok == true {
          ctx.Log.Logf("test", "received: %+v", sig)
          if check(sig) == true {
            return sig, nil
          }
        }
      }
    case <-timeout_channel:
      return zero, fmt.Errorf("LISTENER_TIMEOUT: %s", signal_type)
    }
  }
  return zero, fmt.Errorf("LOOP_ENDED")
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

func (signal *BaseSignal) MarshalJSON() ([]byte, error) {
  return json.Marshal(signal)
}

func (signal BaseSignal) Serialize() ([]byte, error) {
  return signal.MarshalJSON()
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

type StateSignal struct {
  BaseSignal
  State string `json:"state"`
}

type IDStateSignal struct {
  BaseSignal
  ID NodeID `json:"id"`
  State string `json:"status"`
}

func (signal IDStateSignal) String() string {
  ser, err := json.Marshal(signal)
  if err != nil {
    return "STATE_SER_ERR"
  }
  return string(ser)
}

func NewStatusSignal(status string, source NodeID) IDStateSignal {
  return IDStateSignal{
    BaseSignal: NewUpSignal(StatusSignalType),
    ID: source,
    State: status,
  }
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

type LinkStartSignal struct {
  IDSignal
  LinkType string `json:"link_type"`
}

func NewLinkStartSignal(link_type string, target NodeID) LinkStartSignal {
  return LinkStartSignal{
    IDSignal: NewIDSignal(LinkStartSignalType, Direct, target),
    LinkType: link_type,
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

type ECDHSignal struct {
  StateSignal
  Time time.Time
  ECDSA *ecdsa.PublicKey
  ECDH *ecdh.PublicKey
  Signature []byte
}

func keyHash(now time.Time, ec_key *ecdh.PublicKey) ([]byte, error) {
  time_bytes, err := now.MarshalJSON()
  if err != nil {
    return nil, err
  }

  sig_data := append(ec_key.Bytes(), time_bytes...)
  sig_hash := sha512.Sum512(sig_data)

  return sig_hash[:], nil
}

func NewECDHReqSignal(ctx *Context, node *Node) (ECDHSignal, *ecdh.PrivateKey, error) {
  ec_key, err := ctx.ECDH.GenerateKey(rand.Reader)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  now := time.Now()

  sig_hash, err := keyHash(now, ec_key.PublicKey())
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  sig, err := ecdsa.SignASN1(rand.Reader, node.Key, sig_hash)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  return ECDHSignal{
    StateSignal: StateSignal{
      BaseSignal: NewDirectSignal(ECDHSignalType),
      State: "req",
    },
    Time: now,
    ECDSA: &node.Key.PublicKey,
    ECDH: ec_key.PublicKey(),
    Signature: sig,
  }, ec_key, nil
}

const DEFAULT_ECDH_WINDOW = time.Second

func NewECDHRespSignal(ctx *Context, node *Node, req ECDHSignal) (ECDHSignal, []byte, error) {
  now := time.Now()

  err := VerifyECDHSignal(now, req, DEFAULT_ECDH_WINDOW)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  ec_key, err := ctx.ECDH.GenerateKey(rand.Reader)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  shared_secret, err := ec_key.ECDH(req.ECDH)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  key_hash, err := keyHash(now, ec_key.PublicKey())
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  sig, err := ecdsa.SignASN1(rand.Reader, node.Key, key_hash)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  return ECDHSignal{
    StateSignal: StateSignal{
      BaseSignal: NewDirectSignal(ECDHSignalType),
      State: "resp",
    },
    Time: now,
    ECDSA: &node.Key.PublicKey,
    ECDH: ec_key.PublicKey(),
    Signature: sig,
  }, shared_secret, nil
}

func VerifyECDHSignal(now time.Time, sig ECDHSignal, window time.Duration) error {
  earliest := now.Add(-window)
  latest := now.Add(window)

  if sig.Time.Compare(earliest) == -1 {
    return fmt.Errorf("TIME_TOO_LATE: %+v", sig.Time)
  } else if sig.Time.Compare(latest) == 1 {
    return fmt.Errorf("TIME_TOO_EARLY: %+v", sig.Time)
  }

  sig_hash, err := keyHash(sig.Time, sig.ECDH)
  if err != nil {
    return err
  }

  verified := ecdsa.VerifyASN1(sig.ECDSA, sig_hash, sig.Signature)
  if verified == false {
    return fmt.Errorf("VERIFY_FAIL")
  }

  return nil
}

