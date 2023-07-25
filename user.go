package graphvent

import (
  "time"
  "fmt"
  "encoding/json"
  "crypto/ecdsa"
  "crypto/x509"
)

type GroupNode interface {
  Node
  Users() map[NodeID]*User
}

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
  }, "", "  ")
}

var LoadUser = LoadJSONNode(func(id NodeID, j UserJSON) (Node, error) {
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
  return &user, nil
}, func(ctx *Context, user *User, j UserJSON, nodes NodeMap) error {
  return RestoreLockable(ctx, user, j.LockableJSON, nodes)
})

func NewUser(name string, granted time.Time, pubkey *ecdsa.PublicKey, shared []byte) User {
  id := KeyID(pubkey)
  return User{
    Lockable: NewLockable(id, name),
    Granted: granted,
    Pubkey: pubkey,
    Shared: shared,
  }
}

type Group struct {
  Lockable

  UserMap map[NodeID]*User
}

func NewGroup(id NodeID, name string) Group {
  return Group{
    Lockable: NewLockable(id, name),
    UserMap: map[NodeID]*User{},
  }
}

type GroupJSON struct {
  LockableJSON
  Users []string `json:"users"`
}

func (group *Group) Type() NodeType {
  return NodeType("group")
}

func (group *Group) Serialize() ([]byte, error) {
  users := make([]string, len(group.UserMap))
  i := 0
  for id, _ := range(group.UserMap) {
    users[i] = id.String()
    i += 1
  }

  return json.MarshalIndent(&GroupJSON{
    LockableJSON: NewLockableJSON(&group.Lockable),
    Users: users,
  }, "", "  ")
}

var LoadGroup = LoadJSONNode(func(id NodeID, j GroupJSON) (Node, error) {
  group := NewGroup(id, j.Name)
  return &group, nil
}, func(ctx *Context, group *Group, j GroupJSON, nodes NodeMap) error {
  for _, id_str := range(j.Users) {
    id, err := ParseID(id_str)
    if err != nil {
      return err
    }

    user_node, err := LoadNodeRecurse(ctx, id, nodes)
    if err != nil {
      return err
    }

    user, ok := user_node.(*User)
    if ok == false {
      return fmt.Errorf("%s is not a *User", id_str)
    }

    group.UserMap[id] = user
  }

  return RestoreLockable(ctx, group, j.LockableJSON, nodes)
})
