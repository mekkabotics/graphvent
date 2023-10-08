package graphvent

import (
)

var GroupReadPolicy = NewAllNodesPolicy(Tree{
  SerializedType(ReadSignalType): {
    SerializedType(GroupExtType): {
      Hash(FieldNameBase, "members"): nil,
    },
  },
})

type GroupExt struct {
  Members []NodeID `gv:"members"`
}

func NewGroupExt(members []NodeID) *GroupExt {
  return &GroupExt{
    Members: members,
  }
}

func (ext *GroupExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  return nil, nil
}

