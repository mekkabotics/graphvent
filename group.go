package graphvent

import (
  "slices"
)

type AddMemberSignal struct {
  SignalHeader
  MemberID NodeID `gv:"member_id"`
}

func (signal AddMemberSignal) Permission() Tree {
  return Tree{
    SerializedType(AddMemberSignalType): nil,
  }
}

func NewAddMemberSignal(member_id NodeID) *AddMemberSignal {
  return &AddMemberSignal{
    NewSignalHeader(Direct),
    member_id,
  }
}

type RemoveMemberSignal struct {
  SignalHeader
  MemberID NodeID `gv:"member_id"`
}

func (signal RemoveMemberSignal) Permission() Tree {
  return Tree{
    SerializedType(RemoveMemberSignalType): nil,
  }
}

func NewRemoveMemberSignal(member_id NodeID) *RemoveMemberSignal {
  return &RemoveMemberSignal{
    NewSignalHeader(Direct),
    member_id,
  }
}

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
  var messages Messages = nil
  var changes Changes = nil

  switch sig := signal.(type) {
  case *AddMemberSignal:
    if slices.Contains(ext.Members, sig.MemberID) == true {
      messages = messages.Add(ctx, node.ID, node.Key, NewErrorSignal(sig.Id, "already_member"), source)
    } else {
      ext.Members = append(ext.Members, sig.MemberID)
      messages = messages.Add(ctx, node.ID, node.Key, NewSuccessSignal(sig.Id), source)
      changes = changes.Add("member_added")
    }
  case *RemoveMemberSignal:
    idx := slices.Index(ext.Members, sig.MemberID)
    if idx == -1 {
      messages = messages.Add(ctx, node.ID, node.Key, NewErrorSignal(sig.Id, "not_member"), source)
    } else {
      ext.Members = slices.Delete(ext.Members, idx, idx+1)
      messages = messages.Add(ctx, node.ID, node.Key, NewSuccessSignal(sig.Id), source)
      changes = changes.Add("member_removed")
    }
  }

  return messages, changes
}

