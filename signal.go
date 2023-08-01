package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "encoding/binary"
  "crypto/sha512"
  "crypto/ecdsa"
  "crypto/ecdh"
  "crypto/rand"
  "crypto/aes"
  "crypto/cipher"
  "github.com/google/uuid"
)

type SignalDirection int
const (
  StopSignalType SignalType = "STOP"
  StartSignalType           = "START"
  ErrorSignalType           = "ERROR"
  StatusSignalType          = "STATUS"
  LinkSignalType            = "LINK"
  LockSignalType            = "LOCK"
  ReadSignalType            = "READ"
  ReadResultSignalType      = "READ_RESULT"
  LinkStartSignalType       = "LINK_START"
  ECDHSignalType            = "ECDH"
  ECDHStateSignalType       = "ECDH_STATE"
  ECDHProxySignalType       = "ECDH_PROXY"
  GQLStateSignalType        = "GQL_STATE"

  Up SignalDirection = iota
  Down
  Direct
)

type SignalType string
func (signal_type SignalType) String() string { return string(signal_type) }
func (signal_type SignalType) Prefix() string { return "SIGNAL: " }

type Signal interface {
  Serializable[SignalType]
  Direction() SignalDirection
  ID() uuid.UUID
  Permission() Action
}

func WaitForResult(listener chan Signal, timeout time.Duration, id uuid.UUID) (Signal, error) {
  timeout_channel := time.After(timeout)
  var err error = nil
  var result Signal = nil
  run := true
  for run == true {
    select {
    case result=<-listener:
      if result.ID() == id {
        run = false
      }
    case <-timeout_channel:
      result = nil
      err = fmt.Errorf("timeout waiting for read response to %s", id)
      run = false
    }
  }
  return result, err
}

func WaitForSignal[S Signal](ctx * Context, listener *ListenerExt, timeout time.Duration, signal_type SignalType, check func(S)bool) (S, error) {
  var zero S
  var timeout_channel <- chan time.Time
  if timeout > 0 {
    timeout_channel = time.After(timeout)
  }
  for true {
    select {
    case signal := <- listener.Chan:
      if signal == nil {
        return zero, fmt.Errorf("LISTENER_CLOSED: %s", signal_type)
      }
      if signal.Type() == signal_type {
        sig, ok := signal.(S)
        if ok == true {
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
  uuid.UUID `json:"id"`
}

func (signal BaseSignal) ID() uuid.UUID {
  return signal.UUID
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
  return json.Marshal(signal)
}

func NewBaseSignal(signal_type SignalType, direction SignalDirection) BaseSignal {
  signal := BaseSignal{
    UUID: uuid.New(),
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

type ErrorSignal struct {
  BaseSignal
  Error error `json:"error"`
}

func (signal ErrorSignal) Permission() Action {
  return ErrorSignalAction
}

func NewErrorSignal(req_id uuid.UUID, err error) ErrorSignal {
  return ErrorSignal{
    BaseSignal: BaseSignal{
      Direct,
      ErrorSignalType,
      req_id,
    },
    Error: err,
  }
}

type IDSignal struct {
  BaseSignal
  NodeID `json:"id"`
}

func (signal IDSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
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
    NodeID: id,
  }
}

type StringSignal struct {
  BaseSignal
  Str string `json:"state"`
}

func (signal StringSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
}

type IDStringSignal struct {
  BaseSignal
  NodeID `json:"node_id"`
  Str string `json:"string"`
}

func (signal IDStringSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
}

func (signal IDStringSignal) String() string {
  ser, err := json.Marshal(signal)
  if err != nil {
    return "STATE_SER_ERR"
  }
  return string(ser)
}

func NewStatusSignal(status string, source NodeID) IDStringSignal {
  return IDStringSignal{
    BaseSignal: NewUpSignal(StatusSignalType),
    NodeID: source,
    Str: status,
  }
}

func NewLinkSignal(state string) StringSignal {
  return StringSignal{
    BaseSignal: NewDirectSignal(LinkSignalType),
    Str: state,
  }
}

func NewIDStringSignal(signal_type SignalType, direction SignalDirection, state string, id NodeID) IDStringSignal {
  return IDStringSignal{
    BaseSignal: NewBaseSignal(signal_type, direction),
    NodeID: id,
    Str: state,
  }
}

func NewLinkStartSignal(link_type string, target NodeID) IDStringSignal {
  return NewIDStringSignal(LinkStartSignalType, Direct, link_type, target)
}

func NewLockSignal(state string) StringSignal {
  return StringSignal{
    BaseSignal: NewDirectSignal(LockSignalType),
    Str: state,
  }
}

func (signal StringSignal) Permission() Action {
  return MakeAction(signal.Type(), signal.Str)
}

type ReadSignal struct {
  BaseSignal
  UUID uuid.UUID
  Extensions map[ExtType][]string `json:"extensions"`
}

func (signal ReadSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
}

func NewReadSignal(exts map[ExtType][]string) ReadSignal {
  return ReadSignal{
    UUID: uuid.New(),
    BaseSignal: NewDirectSignal(ReadSignalType),
    Extensions: exts,
  }
}

type ReadResultSignal struct {
  BaseSignal
  NodeType
  Extensions map[ExtType]map[string]interface{} `json:"extensions"`
}

func (signal ReadResultSignal) Permission() Action {
  return ReadResultSignalAction
}

func NewReadResultSignal(req_id uuid.UUID, node_type NodeType, exts map[ExtType]map[string]interface{}) ReadResultSignal {
  return ReadResultSignal{
    BaseSignal: BaseSignal{
      Direct,
      ReadResultSignalType,
      req_id,
    },
    NodeType: node_type,
    Extensions: exts,
  }
}

type ECDHSignal struct {
  StringSignal
  Time time.Time
  ECDSA *ecdsa.PublicKey
  ECDH *ecdh.PublicKey
  Signature []byte
}

type ECDHSignalJSON struct {
  StringSignal
  Time time.Time `json:"time"`
  ECDSA []byte `json:"ecdsa_pubkey"`
  ECDH []byte `json:"ecdh_pubkey"`
  Signature []byte `json:"signature"`
}

func (signal *ECDHSignal) MarshalJSON() ([]byte, error) {
  return json.Marshal(&ECDHSignalJSON{
    StringSignal: signal.StringSignal,
    Time: signal.Time,
    ECDH: signal.ECDH.Bytes(),
    ECDSA: signal.ECDH.Bytes(),
    Signature: signal.Signature,
  })
}

func (signal ECDHSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
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
    StringSignal: StringSignal{
      BaseSignal: NewDirectSignal(ECDHSignalType),
      Str: "req",
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
    StringSignal: StringSignal{
      BaseSignal: NewDirectSignal(ECDHSignalType),
      Str: "resp",
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

type ECDHProxySignal struct {
  BaseSignal
  Source NodeID
  Dest NodeID
  IV []byte
  Data []byte
}

func NewECDHProxySignal(source, dest NodeID, signal Signal, shared_secret []byte) (ECDHProxySignal, error) {
  if shared_secret == nil {
    return ECDHProxySignal{}, fmt.Errorf("need shared_secret")
  }

  aes_key, err := aes.NewCipher(shared_secret[:32])
  if err != nil {
    return ECDHProxySignal{}, err
  }

  ser, err := SerializeSignal(signal, aes_key.BlockSize())
  if err != nil {
    return ECDHProxySignal{}, err
  }

  iv := make([]byte, aes_key.BlockSize())
  n, err := rand.Reader.Read(iv)
  if err != nil {
    return ECDHProxySignal{}, err
  } else if n != len(iv) {
    return ECDHProxySignal{}, fmt.Errorf("Not enough bytes read for IV")
  }

  encrypter := cipher.NewCBCEncrypter(aes_key, iv)
  encrypter.CryptBlocks(ser, ser)

  return ECDHProxySignal{
    BaseSignal: NewDirectSignal(ECDHProxySignalType),
    Source: source,
    Dest: dest,
    IV: iv,
    Data: ser,
  }, nil
}

type SignalHeader struct {
  Magic uint32
  TypeHash uint64
  Length uint64
}

const SIGNAL_SER_MAGIC uint32 = 0x753a64de
const SIGNAL_SER_HEADER_LENGTH = 20
func SerializeSignal(signal Signal, block_size int) ([]byte, error) {
  signal_ser, err := signal.Serialize()
  if err != nil {
    return nil, err
  }

  pad_req := 0
  if block_size > 0 {
    pad := block_size - ((SIGNAL_SER_HEADER_LENGTH + len(signal_ser)) % block_size)
    if pad != block_size {
      pad_req = pad
    }
  }

  header := SignalHeader{
    Magic: SIGNAL_SER_MAGIC,
    TypeHash: Hash(signal.Type()),
    Length: uint64(len(signal_ser) + pad_req),
  }

  ser := make([]byte, SIGNAL_SER_HEADER_LENGTH + len(signal_ser) + pad_req)
  binary.BigEndian.PutUint32(ser[0:4], header.Magic)
  binary.BigEndian.PutUint64(ser[4:12], header.TypeHash)
  binary.BigEndian.PutUint64(ser[12:20], header.Length)

  copy(ser[SIGNAL_SER_HEADER_LENGTH:], signal_ser)

  return ser, nil
}

func ParseSignal(ctx *Context, data []byte) (Signal, error) {
  if len(data) < SIGNAL_SER_HEADER_LENGTH {
    return nil, fmt.Errorf("data shorter than header length")
  }

  header := SignalHeader{
    Magic: binary.BigEndian.Uint32(data[0:4]),
    TypeHash: binary.BigEndian.Uint64(data[4:12]),
    Length: binary.BigEndian.Uint64(data[12:20]),
  }

  if header.Magic != SIGNAL_SER_MAGIC {
    return nil, fmt.Errorf("signal magic mismatch 0x%x", header.Magic)
  }

  left := len(data) - SIGNAL_SER_HEADER_LENGTH
  if int(header.Length) != left {
    return nil, fmt.Errorf("signal length mismatch %d/%d", header.Length, left)
  }

  signal_def, exists := ctx.Signals[header.TypeHash]
  if exists == false {
    return nil, fmt.Errorf("0x%x is not a known signal type", header.TypeHash)
  }

  signal, err := signal_def.Load(ctx, data[SIGNAL_SER_HEADER_LENGTH:])
  if err != nil {
    return nil, err
  }

  return signal, nil
}

func ParseECDHProxySignal(ctx *Context, signal *ECDHProxySignal, shared_secret []byte) (Signal, error) {
  if shared_secret == nil {
    return nil, fmt.Errorf("need shared_secret")
  }

  aes_key, err := aes.NewCipher(shared_secret[:32])
  if err != nil {
    return nil, err
  }

  decrypter := cipher.NewCBCDecrypter(aes_key, signal.IV)
  decrypted := make([]byte, len(signal.Data))
  decrypter.CryptBlocks(decrypted, signal.Data)

  wrapped_signal, err := ParseSignal(ctx, decrypted)
  if err != nil {
    return nil, err
  }

  return wrapped_signal, nil
}
