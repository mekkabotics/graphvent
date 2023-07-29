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
  ECDHStateSignalType            = "ECDH_STATE"
  ECDHProxySignalType       = "ECDH_PROXY"

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

func (signal BaseSignal) Serialize() ([]byte, error) {
  return json.Marshal(signal)
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
    ID: id,
  }
}

type StateSignal struct {
  BaseSignal
  State string `json:"state"`
}

func (signal StateSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
}

type IDStateSignal struct {
  BaseSignal
  ID NodeID `json:"id"`
  State string `json:"state"`
}

func (signal IDStateSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
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

func (signal ReadSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
}

func NewReadSignal(exts map[ExtType][]string) ReadSignal {
  return ReadSignal{
    BaseSignal: NewDirectSignal(ReadSignalType),
    Extensions: exts,
  }
}

type ReadResultSignal struct {
  BaseSignal
  NodeType NodeType
  Extensions map[ExtType]map[string]interface{} `json:"extensions"`
}

func NewReadResultSignal(node_type NodeType, exts map[ExtType]map[string]interface{}) ReadResultSignal {
  return ReadResultSignal{
    BaseSignal: NewDirectSignal(ReadResultSignalType),
    NodeType: node_type,
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

type ECDHSignalJSON struct {
  StateSignal
  Time time.Time `json:"time"`
  ECDSA []byte `json:"ecdsa_pubkey"`
  ECDH []byte `json:"ecdh_pubkey"`
  Signature []byte `json:"signature"`
}

func (signal *ECDHSignal) MarshalJSON() ([]byte, error) {
  return json.Marshal(&ECDHSignalJSON{
    StateSignal: signal.StateSignal,
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
