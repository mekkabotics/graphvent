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

func (ext *ECDHExt) HandleECDHSignal(ctx *Context, source NodeID, node *Node, signal ECDHSignal) {
  ser, _ := signal.Serialize()
  ctx.Log.Logf("ecdh", "ECDH_SIGNAL: %s->%s - %s", source, node.ID, ser)
  switch signal.Str {
  case "req":
    state, exists := ext.ECDHStates[source]
    if exists == false {
      state = ECDHState{nil, nil}
    }
    resp, shared_secret, err := NewECDHRespSignal(ctx, node, signal)
    if err == nil {
      state.SharedSecret = shared_secret
      ext.ECDHStates[source] = state
      ctx.Log.Logf("ecdh", "New shared secret for %s<->%s - %+v", node.ID, source, ext.ECDHStates[source].SharedSecret)
      ctx.Send(node.ID, source, resp)
    } else {
      ctx.Log.Logf("ecdh", "ECDH_REQ_ERR: %s", err)
      // TODO: send error response
    }
  case "resp":
    state, exists := ext.ECDHStates[source]
    if exists == false || state.ECKey == nil {
      ctx.Send(node.ID, source, StringSignal{NewDirectSignal(ECDHStateSignalType), "no_req"})
    } else {
      err := VerifyECDHSignal(time.Now(), signal, DEFAULT_ECDH_WINDOW)
      if err == nil {
        shared_secret, err := state.ECKey.ECDH(signal.ECDH)
        if err == nil {
          state.SharedSecret = shared_secret
          state.ECKey = nil
          ext.ECDHStates[source] = state
          ctx.Log.Logf("ecdh", "New shared secret for %s<->%s - %+v", node.ID, source, ext.ECDHStates[source].SharedSecret)
        }
      }
    }
  default:
    ctx.Log.Logf("ecdh", "unknown echd state %s", signal.Str)
  }
}

func (ext *ECDHExt) HandleStateSignal(ctx *Context, source NodeID, node *Node, signal StringSignal) {
  ser, _ := signal.Serialize()
  ctx.Log.Logf("ecdh", "ECHD_STATE: %s->%s - %s", source, node.ID, ser)
}

func (ext *ECDHExt) HandleECDHProxySignal(ctx *Context, source NodeID, node *Node, signal ECDHProxySignal) {
  state, exists := ext.ECDHStates[source]
  if exists == false {
    ctx.Send(node.ID, source, StringSignal{NewDirectSignal(ECDHStateSignalType), "no_req"})
  } else if state.SharedSecret == nil {
    ctx.Send(node.ID, source, StringSignal{NewDirectSignal(ECDHStateSignalType), "no_shared"})
  } else {
    unwrapped_signal, err := ParseECDHProxySignal(ctx, &signal, state.SharedSecret)
    if err != nil {
      ctx.Send(node.ID, source, StringSignal{NewDirectSignal(ECDHStateSignalType), err.Error()})
    } else {
      //TODO: Figure out what I was trying to do here and fix it
      ctx.Send(signal.Source, signal.Dest, unwrapped_signal)
    }
  }
}

func (ext *ECDHExt) Process(ctx *Context, source NodeID, node *Node, signal Signal) {
  switch signal.Direction() {
  case Direct:
    switch signal.Type() {
    case ECDHProxySignalType:
      ecdh_signal := signal.(ECDHProxySignal)
      ext.HandleECDHProxySignal(ctx, source, node, ecdh_signal)
    case ECDHStateSignalType:
      ecdh_signal := signal.(StringSignal)
      ext.HandleStateSignal(ctx, source, node, ecdh_signal)
    case ECDHSignalType:
      ecdh_signal := signal.(ECDHSignal)
      ext.HandleECDHSignal(ctx, source, node, ecdh_signal)
    default:
    }
  default:
  }
}

func (ext *ECDHExt) Type() ExtType {
  return ECDHExtType
}

func (ext *ECDHExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(ext, "", "  ")
}

func LoadECDHExt(ctx *Context, data []byte) (Extension, error) {
  var ext ECDHExt
  err := json.Unmarshal(data, &ext)
  if err != nil {
    return nil, err
  }

  return &ext, nil
}
