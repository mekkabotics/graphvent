package graphvent

import (
  "encoding/json"
)

type GroupExt struct {
  Members map[NodeID]string
}

func (ext *GroupExt) Type() ExtType {
  return GroupExtType
}

func (ext *GroupExt) MarshalBinary() ([]byte, error) {
  return json.Marshal(ext)
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
  ext.Members = map[NodeID]string{}
  return json.Unmarshal(data, ext)
}

func (ext *GroupExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) Messages {
  return nil
}

