package graphvent

import (
  "slices"
)

type AddSubGroupSignal struct {
  SignalHeader
  Name string `gv:"name"`
}

func NewAddSubGroupSignal(name string) *AddSubGroupSignal {
  return &AddSubGroupSignal{
    NewSignalHeader(Direct),
    name,
  }
}

func (signal AddSubGroupSignal) Permission() Tree {
  return Tree{
    SerializedType(AddSubGroupSignalType): {
      Hash("name", signal.Name): nil,
    },
  }
}

type RemoveSubGroupSignal struct {
  SignalHeader
  Name string `gv:"name"`
}

func NewRemoveSubGroupSignal(name string) *RemoveSubGroupSignal {
  return &RemoveSubGroupSignal{
    NewSignalHeader(Direct),
    name,
  }
}

func (signal RemoveSubGroupSignal) Permission() Tree {
  return Tree{
    SerializedType(RemoveSubGroupSignalType): {
      Hash("command", signal.Name): nil,
    },
  }
}

type AddMemberSignal struct {
  SignalHeader
  SubGroup string `gv:"sub_group"`
  MemberID NodeID `gv:"member_id"`
}

type SubGroupGQL struct {
  Name string
  Members []NodeID
}

func (signal AddMemberSignal) Permission() Tree {
  return Tree{
    SerializedType(AddMemberSignalType): {
      Hash("sub_group", signal.SubGroup): nil,
    },
  }
}

func NewAddMemberSignal(sub_group string, member_id NodeID) *AddMemberSignal {
  return &AddMemberSignal{
    NewSignalHeader(Direct),
    sub_group,
    member_id,
  }
}

type RemoveMemberSignal struct {
  SignalHeader
  SubGroup string `gv:"sub_group"`
  MemberID NodeID `gv:"member_id"`
}

func (signal RemoveMemberSignal) Permission() Tree {
  return Tree{
    SerializedType(RemoveMemberSignalType): {
      Hash("sub_group", signal.SubGroup): nil,
    },
  }
}

func NewRemoveMemberSignal(sub_group string, member_id NodeID) *RemoveMemberSignal {
  return &RemoveMemberSignal{
    NewSignalHeader(Direct),
    sub_group,
    member_id,
  }
}

var DefaultGroupPolicy = NewAllNodesPolicy(Tree{
  SerializedType(ReadSignalType): {
    SerializedType(GroupExtType): {
      Hash(FieldNameBase, "sub_groups"): nil,
    },
  },
})

type SubGroup struct {
  Members []NodeID
  Permissions Tree
}

type MemberOfPolicy struct {
  PolicyHeader
  Groups map[NodeID]map[string]Tree
}

func NewMemberOfPolicy(groups map[NodeID]map[string]Tree) MemberOfPolicy {
  return MemberOfPolicy{
    PolicyHeader: NewPolicyHeader(),
    Groups: groups,
  }
}

func (policy MemberOfPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  sig, ok := signal.(*ReadResultSignal)
  if ok == false {
    return Deny
  }
  ctx.Log.Logf("group", "member_of_read_result: %+v", sig.Extensions)

  group_ext_data, ok := sig.Extensions[GroupExtType]
  if ok == false {
    return Deny
  }

  sub_groups_ser, ok := group_ext_data["sub_groups"]
  if ok == false {
    return Deny
  }

  _, sub_groups_if, _, err := DeserializeValue(ctx, sub_groups_ser)
  if err != nil {
    return Deny
  }

  ext_sub_groups, ok := sub_groups_if.Interface().(map[string][]NodeID)
  if ok == false {
    return Deny
  }

  group, exists := policy.Groups[sig.NodeID]
  if exists == false {
    return Deny
  }

  for sub_group_name, permissions := range(group) {
    ext_sub_group, exists := ext_sub_groups[sub_group_name]
    if exists == true {
      for _, member_id := range(ext_sub_group) {
        if member_id == current.Principal {
          if permissions.Allows(current.Action) == Allow {
            return Allow
          }
        }
      }
    }
  }

  return Deny
}

// Send a read signal to Group to check if principal_id is a member of it
func (policy MemberOfPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node) (Messages, RuleResult) {
  var messages Messages = nil
  for group_id, sub_groups := range(policy.Groups) {
    if group_id == node.ID {
      ext, err := GetExt[*GroupExt](node, GroupExtType)
      if err != nil {
        ctx.Log.Logf("group", "MemberOfPolicy with self ID error: %s", err)
      } else {
        for sub_group_name, permission := range(sub_groups) {
          ext_sub_group, exists := ext.SubGroups[sub_group_name]
          if exists == true {
            for _, member := range(ext_sub_group) {
              if member == principal_id {
                if permission.Allows(action) == Allow {
                  return nil, Allow
                }
                break
              }
            }
          }
        }
      }
    } else {
      // Send the read request to the group so that ContinueAllows can parse the response to check membership
      messages = messages.Add(ctx, group_id, node, nil, NewReadSignal(map[ExtType][]string{
        GroupExtType: {"sub_groups"},
      }))
    }
  }
  if len(messages) > 0 {
    return messages, Pending
  } else {
    return nil, Deny
  }
}

type GroupExt struct {
  SubGroups map[string][]NodeID `gv:"sub_groups"`
}

func NewGroupExt(sub_groups map[string][]NodeID) *GroupExt {
  if sub_groups == nil {
    sub_groups = map[string][]NodeID{}
  }
  return &GroupExt{
    SubGroups: sub_groups,
  }
}

func (ext *GroupExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  var messages Messages = nil
  var changes Changes = nil

  switch sig := signal.(type) {
  case *AddMemberSignal:
    sub_group, exists := ext.SubGroups[sig.SubGroup]
    if exists == false {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "not_subgroup"))
    } else {
      if slices.Contains(sub_group, sig.MemberID) == true {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "already_member"))
      } else {
        sub_group = append(sub_group, sig.MemberID)
        ext.SubGroups[sig.SubGroup] = sub_group

        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
        changes = changes.Add("member_added")
      }
    }

  case *RemoveMemberSignal:
    sub_group, exists := ext.SubGroups[sig.SubGroup]
    if exists == false {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "not_subgroup"))
    } else {
      idx := slices.Index(sub_group, sig.MemberID)
      if idx == -1 {
        messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "not_member"))
      } else {
        sub_group = slices.Delete(sub_group, idx, idx+1)
        ext.SubGroups[sig.SubGroup] = sub_group

        messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
        changes = changes.Add("member_removed")
      }
    }

  case *AddSubGroupSignal:
    _, exists := ext.SubGroups[sig.Name]
    if exists == true {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "already_subgroup"))
    } else {
      ext.SubGroups[sig.Name] = []NodeID{}

      changes = changes.Add("subgroup_added")
      messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
    }
  case *RemoveSubGroupSignal:
    _, exists := ext.SubGroups[sig.Name]
    if exists == false {
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "not_subgroup"))
    } else {
      delete(ext.SubGroups, sig.Name)

      changes = changes.Add("subgroup_removed")
      messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
    }
  }

  return messages, changes
}

