package graphvent

import (
  "encoding/json"
)

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

