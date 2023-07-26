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

type ECDHExtJSON struct {
  Granted time.Time `json:"granted"`
  Pubkey []byte `json:"pubkey"`
  Shared []byte `json:"shared"`
}

func (ext *ECDHExt) Process(context *StateContext, node *Node, signal Signal) error {
  return nil
}

const ECDHExtType = ExtType("ECDH")
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
  Members NodeMap
}

const GroupExtType = ExtType("GROUP")
func (ext *GroupExt) Type() ExtType {
  return GroupExtType
}

func (ext *GroupExt) Serialize() ([]byte, error) {
  return json.MarshalIndent(&struct{
    Members []string `json:"members"`
  }{
    Members: SaveNodeList(ext.Members),
  }, "", "  ")
}

func NewGroupExt(members NodeMap) *GroupExt {
  if members == nil {
    members = NodeMap{}
  }
  return &GroupExt{
    Members: members,
  }
}

func LoadGroupExt(ctx *Context, data []byte) (Extension, error) {
  var j struct {
    Members []string `json:"members"`
  }

  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  members, err := RestoreNodeList(ctx, j.Members)
  if err != nil {
    return nil, err
  }

  return NewGroupExt(members), nil
}

func (ext *GroupExt) Process(context *StateContext, node *Node, signal Signal) error {
  return nil
}
