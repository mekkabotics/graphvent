package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "crypto/ecdsa"
  "crypto/x509"
)

type User struct {
  Lockable

  Granted time.Time
  Pubkey *ecdsa.PublicKey
  Shared []byte
  Tags []string
}

type UserJSON struct {
  LockableJSON
  Granted time.Time `json:"granted"`
  Pubkey []byte `json:"pubkey"`
  Shared []byte `json:"shared"`
  Tags []string `json:"tags"`
}

func (user *User) Type() NodeType {
  return NodeType("user")
}

func (user *User) Serialize() ([]byte, error) {
  lockable_json := NewLockableJSON(&user.Lockable)
  pubkey, err := x509.MarshalPKIXPublicKey(user.Pubkey)
  if err != nil {
    return nil, err
  }

  return json.MarshalIndent(&UserJSON{
    LockableJSON: lockable_json,
    Granted: user.Granted,
    Shared: user.Shared,
    Pubkey: pubkey,
    Tags: user.Tags,
  }, "", "  ")
}

func LoadUser(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  ctx.Log.Logf("test", "LOADING_USER: %s", id)
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

  user := NewUser(j.Name, j.Granted, pubkey, j.Shared, j.Tags)
  nodes[id] = &user

  err = RestoreLockable(ctx, &user.Lockable, j.LockableJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &user, nil
}

func NewUser(name string, granted time.Time, pubkey *ecdsa.PublicKey, shared []byte, tags []string) User {
  id := KeyID(pubkey)
  return User{
    Lockable: NewLockable(id, name),
    Granted: granted,
    Pubkey: pubkey,
    Shared: shared,
    Tags: tags,
  }
}
