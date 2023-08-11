package graphvent

import (
  "fmt"
  "time"
  "encoding/json"
  "crypto/ecdsa"
  "crypto/x509"
  "crypto/ecdh"
)

type ECDHState struct {
  ECKey *ecdh.PrivateKey
  SharedSecret []byte
}

type ECDHStateJSON struct {
  ECKey []byte `json:"ec_key"`
  SharedSecret []byte `json:"shared_secret"`
}

func (state *ECDHState) MarshalJSON() ([]byte, error) {
  var key_bytes []byte
  var err error
  if state.ECKey != nil {
    key_bytes, err = x509.MarshalPKCS8PrivateKey(state.ECKey)
    if err != nil {
      return nil, err
    }
  }

  return json.Marshal(&ECDHStateJSON{
    ECKey: key_bytes,
    SharedSecret: state.SharedSecret,
  })
}

func (state *ECDHState) UnmarshalJSON(data []byte) error {
  var j ECDHStateJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return err
  }

  state.SharedSecret = j.SharedSecret
  if len(j.ECKey) == 0 {
    state.ECKey = nil
  } else {
    tmp_key, err := x509.ParsePKCS8PrivateKey(j.ECKey)
    if err != nil {
      return err
    }

    ecdsa_key, ok := tmp_key.(*ecdsa.PrivateKey)
    if ok == false {
      return fmt.Errorf("Parsed wrong key type from DB for ECDHState")
    }

    state.ECKey, err = ecdsa_key.ECDH()
    if err != nil {
      return err
    }
  }

  return nil
}

type ECDHMap map[NodeID]ECDHState

func (m ECDHMap) MarshalJSON() ([]byte, error) {
  tmp := map[string]ECDHState{}
  for id, state := range(m) {
    tmp[id.String()] = state
  }

  return json.Marshal(tmp)
}

type ECDHExt struct {
  ECDHStates ECDHMap
}

func NewECDHExt() *ECDHExt {
  return &ECDHExt{
    ECDHStates: ECDHMap{},
  }
}

func ResolveFields[T Extension](t T, name string, field_funcs map[string]func(T)interface{})interface{} {
  var zero T
  field_func, ok := field_funcs[name]
  if ok == false {
    return fmt.Errorf("%s is not a field of %s", name, zero.Type())
  }
  return field_func(t)
}

func (ext *ECDHExt) Field(name string) interface{} {
  return ResolveFields(ext, name, map[string]func(*ECDHExt)interface{}{
    "ecdh_states": func(ext *ECDHExt) interface{} {
      return ext.ECDHStates
    },
  })
}

func (ext *ECDHExt) HandleECDHSignal(log Logger, node *Node, signal *ECDHSignal) Messages {
  source := KeyID(signal.EDDSA)

  messages := Messages{}
  switch signal.Str {
  case "req":
    state, exists := ext.ECDHStates[source]
    if exists == false {
      state = ECDHState{nil, nil}
    }
    resp, shared_secret, err := NewECDHRespSignal(node, signal)
    if err == nil {
      state.SharedSecret = shared_secret
      ext.ECDHStates[source] = state
      log.Logf("ecdh", "New shared secret for %s<->%s - %+v", node.ID, source, ext.ECDHStates[source].SharedSecret)
      messages = messages.Add(node.ID, node.Key, &resp, source)
    } else {
      log.Logf("ecdh", "ECDH_REQ_ERR: %s", err)
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), err.Error()), source)
    }
  case "resp":
    state, exists := ext.ECDHStates[source]
    if exists == false || state.ECKey == nil {
      messages = messages.Add(node.ID, node.Key, NewErrorSignal(signal.ID(), "no_req"), source)
    } else {
      err := VerifyECDHSignal(time.Now(), signal, DEFAULT_ECDH_WINDOW)
      if err == nil {
        shared_secret, err := state.ECKey.ECDH(signal.ECDH)
        if err == nil {
          state.SharedSecret = shared_secret
          state.ECKey = nil
          ext.ECDHStates[source] = state
          log.Logf("ecdh", "New shared secret for %s<->%s - %+v", node.ID, source, ext.ECDHStates[source].SharedSecret)
        }
      }
    }
  default:
    log.Logf("ecdh", "unknown echd state %s", signal.Str)
  }
  return messages
}

func (ext *ECDHExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  switch signal.Type() {
  case ECDHSignalType:
    sig := signal.(*ECDHSignal)
    return ext.HandleECDHSignal(ctx.Log, node, sig)
  }
  return nil
}

func (ext *ECDHExt) Type() ExtType {
  return ECDHExtType
}

func (ext *ECDHExt) Serialize() ([]byte, error) {
  return json.Marshal(ext)
}

func (ext *ECDHExt) Deserialize(ctx *Context, data []byte) error {
  return json.Unmarshal(data, &ext)
}
