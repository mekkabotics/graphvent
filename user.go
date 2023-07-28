package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "crypto/ecdsa"
  "crypto/x509"
)

type ECDHExt struct {
  Granted time.Time
  Pubkey *ecdsa.PublicKey
  Shared []byte
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
    "granted": func(ext *ECDHExt) interface{} {
      return ext.Granted
    },
    "pubkey": func(ext *ECDHExt) interface{} {
      return ext.Pubkey
    },
    "shared": func(ext *ECDHExt) interface{} {
      return ext.Shared
    },
  })
}

type ECDHExtJSON struct {
  Granted time.Time `json:"granted"`
  Pubkey []byte `json:"pubkey"`
  Shared []byte `json:"shared"`
}

func (ext *ECDHExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
  return
}

func (ext *ECDHExt) Type() ExtType {
  return ECDHExtType
}

func (ext *ECDHExt) Serialize() ([]byte, error) {
  pubkey, err := x509.MarshalPKIXPublicKey(ext.Pubkey)
  if err != nil {
    return nil, err
  }

  return json.MarshalIndent(&ECDHExtJSON{
    Granted: ext.Granted,
    Pubkey: pubkey,
    Shared: ext.Shared,
  }, "", "  ")
}

func LoadECDHExt(ctx *Context, data []byte) (Extension, error) {
  var j ECDHExtJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  pub, err := x509.ParsePKIXPublicKey(j.Pubkey)
  if err != nil {
    return nil, err
  }

  var pubkey *ecdsa.PublicKey
  switch pub.(type) {
  case *ecdsa.PublicKey:
    pubkey = pub.(*ecdsa.PublicKey)
  default:
    return nil, fmt.Errorf("Invalid key type: %+v", pub)
  }

  extension := ECDHExt{
    Granted: j.Granted,
    Pubkey: pubkey,
    Shared: j.Shared,
  }

  return &extension, nil
}

type GroupExt struct {
  Members map[NodeID]string
}

type GroupExtJSON struct {
  Members map[string]string `json:"members"`
}

func (ext *GroupExt) Type() ExtType {
  return GroupExtType
}

func (ext *GroupExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(&GroupExtJSON{
    Members: IDMap(ext.Members),
  }, "", "  ")
}

func (ext *GroupExt) Field(name string) interface{} {
  return ResolveFields(ext, name, map[string]func(*GroupExt)interface{}{
    "members": func(ext *GroupExt) interface{} {
      return ext.Members
    },
  })
}

func NewGroupExt(members map[NodeID]string) *GroupExt {
  if members == nil {
    members = map[NodeID]string{}
  }

  return &GroupExt{
    Members: members,
  }
}

func LoadGroupExt(ctx *Context, data []byte) (Extension, error) {
  var j GroupExtJSON
  err := json.Unmarshal(data, &j)

  members, err := LoadIDMap(j.Members)
  if err != nil {
    return nil, err
  }

  return &GroupExt{
    Members: members,
  }, nil
}

func (ext *GroupExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
  return
}

