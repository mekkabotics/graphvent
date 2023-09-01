package graphvent

import (
)

type Policy interface {
  Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult)
  ContinueAllows(ctx *Context, current PendingACL, signal Signal)RuleResult
  // Merge with another policy of the same underlying type
  Merge(Policy) Policy
  // Make a copy of this policy
  Copy() Policy
}

func (policy *AllNodesPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  return nil, policy.Rules.Allows(action)
}

func (policy *AllNodesPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  return Deny
}

func (policy *PerNodePolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  for id, actions := range(policy.NodeRules) {
    if id != principal_id {
      continue
    }
    return nil, actions.Allows(action)
  }
  return nil, Deny
}

func (policy *PerNodePolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  return Deny
}

type RequirementOfPolicy struct {
  PerNodePolicy
}

func (policy *RequirementOfPolicy) Type() PolicyType {
  return RequirementOfPolicyType
}

func NewRequirementOfPolicy(dep_rules map[NodeID]Tree) RequirementOfPolicy {
  return RequirementOfPolicy {
    PerNodePolicy: NewPerNodePolicy(dep_rules),
  }
}

func (policy *RequirementOfPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  sig, ok := signal.(*ReadResultSignal)
  if ok == false {
    return Deny
  }

  ext, ok := sig.Extensions[LockableExtType]
  if ok == false {
    return Deny
  }

  reqs_ser, ok := ext["requirements"]
  if ok == false {
    return Deny
  }

  reqs_if, err := DeserializeValue(ctx, reqs_ser)
  if err != nil {
    return Deny
  }

  requirements, ok := reqs_if.(map[NodeID]ReqState)
  if ok == false {
    return Deny
  }

  for req, _ := range(requirements) {
    if req == current.Principal {
      return policy.NodeRules[sig.NodeID].Allows(current.Action)
    }
  }

  return Deny
}

type MemberOfPolicy struct {
  PerNodePolicy
}

func (policy *MemberOfPolicy) Type() PolicyType {
  return MemberOfPolicyType
}

func NewMemberOfPolicy(group_rules map[NodeID]Tree) MemberOfPolicy {
  return MemberOfPolicy{
    PerNodePolicy: NewPerNodePolicy(group_rules),
  }
}

func (policy *MemberOfPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  sig, ok := signal.(*ReadResultSignal)
  if ok == false {
    return Deny
  }

  group_ext_data, ok := sig.Extensions[GroupExtType]
  if ok == false {
    return Deny
  }

  members_ser, ok := group_ext_data["members"]
  if ok == false {
    return Deny
  }

  members_if, err := DeserializeValue(ctx, members_ser)
  if err != nil {
    return Deny
  }

  members, ok := members_if.(map[NodeID]string)
  if ok == false {
    return Deny
  }

  for member, _ := range(members) {
    if member == current.Principal {
      return policy.NodeRules[sig.NodeID].Allows(current.Action)
    }
  }

  return Deny
}

// Send a read signal to Group to check if principal_id is a member of it
func (policy *MemberOfPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node) (Messages, RuleResult) {
  msgs := Messages{}
  for id, rule := range(policy.NodeRules) {
    if id == node.ID {
      ext, err := GetExt[*GroupExt](node, GroupExtType)
      if err == nil {
        for member, _ := range(ext.Members) {
          if member == principal_id {
            if rule.Allows(action) == Allow {
              return nil, Allow
            }
          }
        }
      }
    } else {
      msgs = msgs.Add(ctx, node.ID, node.Key, NewReadSignal(map[ExtType][]string{
        GroupExtType: []string{"members"},
      }), id)
    }
  }
  return msgs, Pending
}

func (policy *MemberOfPolicy) Merge(p Policy) Policy {
  other := p.(*MemberOfPolicy)
  policy.NodeRules = MergeNodeRules(policy.NodeRules, other.NodeRules)
  return policy
}

func (policy *MemberOfPolicy) Copy() Policy {
  new_rules := CopyNodeRules(policy.NodeRules)
  return &MemberOfPolicy{
    PerNodePolicy: NewPerNodePolicy(new_rules),
  }
}

func CopyTree(tree Tree) Tree {
  if tree == nil {
    return nil
  }

  ret := Tree{}
  for name, sub := range(tree) {
    ret[name] = CopyTree(sub)
  }

  return ret
}

func MergeTrees(first Tree, second Tree) Tree {
  if first == nil || second == nil {
    return nil
  }

  ret := CopyTree(first)
  for name, sub := range(second) {
    current, exists := ret[name]
    if exists == true {
      ret[name] = MergeTrees(current, sub)
    } else {
      ret[name] = CopyTree(sub)
    }
  }
  return ret
}

func CopyNodeRules(rules map[NodeID]Tree) map[NodeID]Tree {
  ret := map[NodeID]Tree{}
  for id, r := range(rules) {
    ret[id] = r
  }
  return ret
}

func MergeNodeRules(first map[NodeID]Tree, second map[NodeID]Tree) map[NodeID]Tree {
  merged := map[NodeID]Tree{}
  for id, actions := range(first) {
    merged[id] = actions
  }
  for id, actions := range(second) {
    existing, exists := merged[id]
    if exists {
      merged[id] = MergeTrees(existing, actions)
    } else {
      merged[id] = actions
    }
  }
  return merged
}

func (policy *PerNodePolicy) Merge(p Policy) Policy {
  other := p.(*PerNodePolicy)
  policy.NodeRules = MergeNodeRules(policy.NodeRules, other.NodeRules)
  return policy
}

func (policy *PerNodePolicy) Copy() Policy {
  new_rules := CopyNodeRules(policy.NodeRules)
  return &PerNodePolicy{
    NodeRules: new_rules,
  }
}

func (policy *AllNodesPolicy) Merge(p Policy) Policy {
  other := p.(*AllNodesPolicy)
  policy.Rules = MergeTrees(policy.Rules, other.Rules)
  return policy
}

func (policy *AllNodesPolicy) Copy() Policy {
  new_rules := policy.Rules
  return &AllNodesPolicy {
    Rules: new_rules,
  }
}

type Tree map[uint64]Tree

func (rule Tree) Allows(action Tree) RuleResult {
  // If the current rule is nil, it's a wildcard and any action being processed is allowed
  if rule == nil {
    return Allow
    // If the rule isn't "allow all" but the action is "request all", deny
  } else if action == nil {
    return Deny
    // If the current action has no children, it's allowed
  } else if len(action) == 0 {
    return Allow
    // If the current rule has no children but the action goes further, it's not allowed
  } else if len(rule) == 0 {
    return Deny
    // If the current rule and action have children, all the children of action must be allowed by rule
  } else {
    for sub, subtree := range(action) {
      subrule, exists := rule[sub]
      if exists == false {
        return Deny
      } else if subrule.Allows(subtree) == Deny {
        return Deny
      }
    }
    return Allow
  }
}

func NewPerNodePolicy(node_actions map[NodeID]Tree) PerNodePolicy {
  if node_actions == nil {
    node_actions = map[NodeID]Tree{}
  }

  return PerNodePolicy{
    NodeRules: node_actions,
  }
}

type PerNodePolicy struct {
  NodeRules map[NodeID]Tree `json:"node_actions"`
}

func (policy *PerNodePolicy) Type() PolicyType {
  return PerNodePolicyType
}

func NewAllNodesPolicy(rules Tree) AllNodesPolicy {
  return AllNodesPolicy{
    Rules: rules,
  }
}

type AllNodesPolicy struct {
  Rules Tree
}

func (policy *AllNodesPolicy) Type() PolicyType {
  return AllNodesPolicyType
}

var DefaultPolicy = NewAllNodesPolicy(Tree{
  uint64(ErrorSignalType): nil,
  uint64(ReadResultSignalType): nil,
})
