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
  return json.Marshal(&GroupExtJSON{
    Members: IDMap(ext.Members),
  })
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

func (ext *GroupExt) Deserialize(ctx *Context, data []byte) error {
  var j GroupExtJSON
  err := json.Unmarshal(data, &j)

  ext.Members, err = LoadIDMap(j.Members)
  return err
}

func (ext *GroupExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
  return
}

