package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "crypto/ecdsa"
  "crypto/x509"
)

type User struct {
  SimpleLockable

  Granted time.Time
  Pubkey *ecdsa.PublicKey
  Shared []byte
}

type UserJSON struct {
  SimpleLockableJSON
  Granted time.Time `json:"granted"`
  Pubkey []byte `json:"pubkey"`
  Shared []byte `json:"shared"`
}

func (user *User) Type() NodeType {
  return NodeType("gql_user")
}

func (user *User) Serialize() ([]byte, error) {
  lockable_json := NewSimpleLockableJSON(&user.SimpleLockable)
  pubkey, err := x509.MarshalPKIXPublicKey(user.Pubkey)
  if err != nil {
    return nil, err
  }

  return json.MarshalIndent(&UserJSON{
    SimpleLockableJSON: lockable_json,
    Granted: user.Granted,
    Shared: user.Shared,
    Pubkey: pubkey,
  }, "", "  ")
}

func LoadUser(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j UserJSON
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
    return nil, fmt.Errorf("Invalid key type")
  }

  user := NewUser(j.Name, j.Granted, pubkey, j.Shared)
  nodes[id] = &user

  err = RestoreSimpleLockable(ctx, &user, j.SimpleLockableJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &user, nil
}

func NewUser(name string, granted time.Time, pubkey *ecdsa.PublicKey, shared []byte) User {
  id := KeyID(pubkey)
  return User{
    SimpleLockable: NewSimpleLockable(id, name),
    Granted: granted,
    Pubkey: pubkey,
    Shared: shared,
  }
}
