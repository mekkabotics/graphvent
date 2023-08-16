package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "encoding/binary"
  "crypto"
  "crypto/ed25519"
  "crypto/ecdh"
  "crypto/rand"
  "crypto/aes"
  "crypto/cipher"
  "github.com/google/uuid"
)

type SignalDirection int
const (
  StopSignalType       = SignalType("STOP")
  NewSignalType        = SignalType("NEW")
  StartSignalType      = SignalType("START")
  ErrorSignalType      = SignalType("ERROR")
  StatusSignalType     = SignalType("STATUS")
  LinkSignalType       = SignalType("LINK")
  LockSignalType       = SignalType("LOCK")
  ReadSignalType       = SignalType("READ")
  ReadResultSignalType = SignalType("READ_RESULT")
  LinkStartSignalType  = SignalType("LINK_START")
  ECDHSignalType       = SignalType("ECDH")
  ECDHProxySignalType  = SignalType("ECDH_PROXY")
  ACLTimeoutSignalType = SignalType("ACL_TIMEOUT")

  Up SignalDirection = iota
  Down
  Direct
)

type SignalType string
func (signal_type SignalType) String() string { return string(signal_type) }
func (signal_type SignalType) Prefix() string { return "SIGNAL: " }

type Signal interface {
  Serializable[SignalType]
  String() string
  Direction() SignalDirection
  ID() uuid.UUID
  ReqID() uuid.UUID
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
      if signal.ReqID() == req_id {
        return signal, nil
      }
    case <-timeout_channel:
      return nil, fmt.Errorf("LISTENER_TIMEOUT")
    }
  }
  return nil, fmt.Errorf("UNREACHABLE")
}

func WaitForSignal[S Signal](listener chan Signal, timeout time.Duration, signal_type SignalType, check func(S)bool) (S, error) {
  var zero S
  var timeout_channel <- chan time.Time
  if timeout > 0 {
    timeout_channel = time.After(timeout)
  }
  for true {
    select {
    case signal := <- listener:
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
  UUID uuid.UUID `json:"id"`
  ReqUUID uuid.UUID `json:"req_uuid"`
}

func (signal *BaseSignal) ReqID() uuid.UUID {
  return signal.ReqUUID
}

func (signal *BaseSignal) String() string {
  ser, _ := json.Marshal(signal)
  return string(ser)
}

func (signal *BaseSignal) Deserialize(ctx *Context, data []byte) error {
  return json.Unmarshal(data, signal)
}

func (signal *BaseSignal) ID() uuid.UUID {
  return signal.UUID
}

func (signal *BaseSignal) Type() SignalType {
  return signal.SignalType
}

func (signal *BaseSignal) Permission() Tree {
  return Tree{
    string(signal.Type()): Tree{},
  }
}

func (signal *BaseSignal) Direction() SignalDirection {
  return signal.SignalDirection
}

func (signal *BaseSignal) Serialize() ([]byte, error) {
  return json.Marshal(signal)
}

func NewBaseSignal(signal_type SignalType, direction SignalDirection) BaseSignal {
  id := uuid.New()
  signal := BaseSignal{
    UUID: id,
    ReqUUID: id,
    SignalDirection: direction,
    SignalType: signal_type,
  }
  return signal
}

func NewRespSignal(id uuid.UUID, signal_type SignalType, direction SignalDirection) BaseSignal {
  signal := BaseSignal{
    UUID: uuid.New(),
    ReqUUID: id,
    SignalDirection: direction,
    SignalType: signal_type,
  }
  return signal
}

var NewSignal = NewBaseSignal(NewSignalType, Direct)
var StartSignal = NewBaseSignal(StartSignalType, Direct)
var StopSignal = NewBaseSignal(StopSignalType, Direct)

type IDSignal struct {
  BaseSignal
  NodeID `json:"id"`
}

func (signal *IDSignal) Serialize() ([]byte, error) {
  return json.Marshal(signal)
}

type StringSignal struct {
  BaseSignal
  Str string `json:"state"`
}

func (signal *StringSignal) String() string {
  ser, _ := json.Marshal(signal)
  return string(ser)
}

func (signal *StringSignal) Serialize() ([]byte, error) {
  return json.Marshal(&signal)
}

type ErrorSignal struct {
  BaseSignal
  Error string
}

func (signal *ErrorSignal) String() string {
  ser, _ := json.Marshal(signal)
  return string(ser)
}

func NewErrorSignal(req_id uuid.UUID, err string) Signal {
  return &ErrorSignal{
    NewRespSignal(req_id, ErrorSignalType, Direct),
    err,
  }
}

func NewACLTimeoutSignal(req_id uuid.UUID) Signal {
  sig := NewRespSignal(req_id, ACLTimeoutSignalType, Direct)
  return &sig
}

type IDStringSignal struct {
  BaseSignal
  NodeID NodeID `json:"node_id"`
  Str string `json:"string"`
}

func (signal *IDStringSignal) String() string {
  ser, _ := json.Marshal(signal)
  return string(ser)
}

func (signal *IDStringSignal) Serialize() ([]byte, error) {
  return json.Marshal(signal)
}

func NewStatusSignal(status string, source NodeID) Signal {
  return &IDStringSignal{
    BaseSignal: NewBaseSignal(StatusSignalType, Up),
    NodeID: source,
    Str: status,
  }
}

func NewLinkSignal(state string) Signal {
  return &StringSignal{
    BaseSignal: NewBaseSignal(LinkSignalType, Direct),
    Str: state,
  }
}

func NewLinkStartSignal(link_type string, target NodeID) Signal {
  return &IDStringSignal{
    NewBaseSignal(LinkStartSignalType, Direct),
    target,
    link_type,
  }
}

func NewLockSignal(state string) Signal {
  return &StringSignal{
    NewBaseSignal(LockSignalType, Direct),
    state,
  }
}

func (signal *StringSignal) Permission() Tree {
  return Tree{
    string(signal.Type()): Tree{
      signal.Str: Tree{},
    },
  }
}

type ReadSignal struct {
  BaseSignal
  Extensions map[ExtType][]string `json:"extensions"`
}

func (signal *ReadSignal) Serialize() ([]byte, error) {
  return json.Marshal(signal)
}

func NewReadSignal(exts map[ExtType][]string) *ReadSignal {
  return &ReadSignal{
    NewBaseSignal(ReadSignalType, Direct),
    exts,
  }
}

func (signal *ReadSignal) Permission() Tree {
  ret := Tree{}
  for ext, fields := range(signal.Extensions) {
    field_tree := Tree{}
    for _, field := range(fields) {
      field_tree[field] = Tree{}
    }
    ret[ext.String()] = field_tree
  }
  return Tree{ReadSignalType.String(): ret}
}

type ReadResultSignal struct {
  BaseSignal
  NodeID NodeID
  NodeType NodeType
  Extensions map[ExtType]map[string]interface{} `json:"extensions"`
}

func (signal *ReadResultSignal) Permission() Tree {
  return Tree{
    ReadResultSignalType.String(): Tree{},
  }
}

func NewReadResultSignal(req_id uuid.UUID, node_id NodeID, node_type NodeType, exts map[ExtType]map[string]interface{}) Signal {
  return &ReadResultSignal{
    NewRespSignal(req_id, ReadResultSignalType, Direct),
    node_id,
    node_type,
    exts,
  }
}

type ECDHSignal struct {
  StringSignal
  Time time.Time
  EDDSA ed25519.PublicKey
  ECDH *ecdh.PublicKey
  Signature []byte
}

type ECDHSignalJSON struct {
  StringSignal
  Time time.Time `json:"time"`
  EDDSA []byte `json:"ecdsa_pubkey"`
  ECDH []byte `json:"ecdh_pubkey"`
  Signature []byte `json:"signature"`
}

func (signal *ECDHSignal) MarshalJSON() ([]byte, error) {
  return json.Marshal(&ECDHSignalJSON{
    StringSignal: signal.StringSignal,
    Time: signal.Time,
    ECDH: signal.ECDH.Bytes(),
    EDDSA: signal.ECDH.Bytes(),
    Signature: signal.Signature,
  })
}

func (signal *ECDHSignal) Serialize() ([]byte, error) {
  return json.Marshal(signal)
}

func NewECDHReqSignal(node *Node) (Signal, *ecdh.PrivateKey, error) {
  ec_key, err := ECDH.GenerateKey(rand.Reader)
  if err != nil {
    return nil, nil, err
  }

  now := time.Now()
  time_bytes, err := now.MarshalJSON()
  if err != nil {
    return nil, nil, err
  }

  sig_data := append(ec_key.PublicKey().Bytes(), time_bytes...)

  sig, err := node.Key.Sign(rand.Reader, sig_data, crypto.Hash(0))
  if err != nil {
    return nil, nil, err
  }

  return &ECDHSignal{
    StringSignal: StringSignal{
      BaseSignal: NewBaseSignal(ECDHSignalType, Direct),
      Str: "req",
    },
    Time: now,
    EDDSA: node.Key.Public().(ed25519.PublicKey),
    ECDH: ec_key.PublicKey(),
    Signature: sig,
  }, ec_key, nil
}

const DEFAULT_ECDH_WINDOW = time.Second

func NewECDHRespSignal(node *Node, req *ECDHSignal) (ECDHSignal, []byte, error) {
  now := time.Now()

  err := VerifyECDHSignal(now, req, DEFAULT_ECDH_WINDOW)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  ec_key, err := ECDH.GenerateKey(rand.Reader)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  shared_secret, err := ec_key.ECDH(req.ECDH)
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  time_bytes, err := now.MarshalJSON()
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  sig_data := append(ec_key.PublicKey().Bytes(), time_bytes...)

  sig, err := node.Key.Sign(rand.Reader, sig_data, crypto.Hash(0))
  if err != nil {
    return ECDHSignal{}, nil, err
  }

  return ECDHSignal{
    StringSignal: StringSignal{
      BaseSignal: NewBaseSignal(ECDHSignalType, Direct),
      Str: "resp",
    },
    Time: now,
    EDDSA: node.Key.Public().(ed25519.PublicKey),
    ECDH: ec_key.PublicKey(),
    Signature: sig,
  }, shared_secret, nil
}

func VerifyECDHSignal(now time.Time, sig *ECDHSignal, window time.Duration) error {
  earliest := now.Add(-window)
  latest := now.Add(window)

  if sig.Time.Compare(earliest) == -1 {
    return fmt.Errorf("TIME_TOO_LATE: %+v", sig.Time)
  } else if sig.Time.Compare(latest) == 1 {
    return fmt.Errorf("TIME_TOO_EARLY: %+v", sig.Time)
  }

  time_bytes, err := sig.Time.MarshalJSON()
  if err != nil {
    return err
  }

  sig_data := append(sig.ECDH.Bytes(), time_bytes...)

  verified := ed25519.Verify(sig.EDDSA, sig_data, sig.Signature)
  if verified == false {
    return fmt.Errorf("Failed to verify signature")
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

func NewECDHProxySignal(source, dest NodeID, signal Signal, shared_secret []byte) (Signal, error) {
  if shared_secret == nil {
    return nil, fmt.Errorf("need shared_secret")
  }

  aes_key, err := aes.NewCipher(shared_secret[:32])
  if err != nil {
    return nil, err
  }

  ser, err := SerializeSignal(signal, aes_key.BlockSize())
  if err != nil {
    return nil, err
  }

  iv := make([]byte, aes_key.BlockSize())
  n, err := rand.Reader.Read(iv)
  if err != nil {
    return nil, err
  } else if n != len(iv) {
    return nil, fmt.Errorf("Not enough bytes read for IV")
  }

  encrypter := cipher.NewCBCEncrypter(aes_key, iv)
  encrypter.CryptBlocks(ser, ser)

  return &ECDHProxySignal{
    BaseSignal: NewBaseSignal(ECDHProxySignalType, Direct),
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
