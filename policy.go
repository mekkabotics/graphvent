package graphvent

import (
  "encoding/json"
)

const (
  MemberOfPolicyType = PolicyType("USER_OF")
  PerNodePolicyType = PolicyType("PER_NODE")
  AllNodesPolicyType = PolicyType("ALL_NODES")
)

type Policy interface {
  Serializable[PolicyType]
  Allows(principal_id NodeID, action Tree, node *Node)(Messages, RuleResult)
  ContinueAllows(current PendingACL, signal Signal)RuleResult
  // Merge with another policy of the same underlying type
  Merge(Policy) Policy
  // Make a copy of this policy
  Copy() Policy
}

func (policy *AllNodesPolicy) Allows(principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  return nil, policy.Rules.Allows(action)
}

func (policy *AllNodesPolicy) ContinueAllows(current PendingACL, signal Signal) RuleResult {
  return Deny
}

func (policy *PerNodePolicy) Allows(principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  for id, actions := range(policy.NodeRules) {
    if id != principal_id {
      continue
    }
    return nil, actions.Allows(action)
  }
  return nil, Deny
}

func (policy *PerNodePolicy) ContinueAllows(current PendingACL, signal Signal) RuleResult {
  return Deny
}

type MemberOfPolicy struct {
  PerNodePolicy
}

func (policy *MemberOfPolicy) Type() PolicyType {
  return MemberOfPolicyType
}

func NewMemberOfPolicy(group_rules NodeRules) MemberOfPolicy {
  return MemberOfPolicy{
    PerNodePolicy: NewPerNodePolicy(group_rules),
  }
}

func (policy *MemberOfPolicy) ContinueAllows(current PendingACL, signal Signal) RuleResult {
  sig, ok := signal.(*ReadResultSignal)
  if ok == false {
    return Deny
  }

  group_ext_data, ok := sig.Extensions[GroupExtType]
  if ok == false {
    return Deny
  }

  members, ok := group_ext_data["members"].(map[NodeID]string)
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
func (policy *MemberOfPolicy) Allows(principal_id NodeID, action Tree, node *Node) (Messages, RuleResult) {
  msgs := Messages{}
  for id, rule := range(policy.NodeRules) {
    if id == node.ID {
      ext, err := GetExt[*GroupExt](node)
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
      msgs = msgs.Add(node.ID, node.Key, NewReadSignal(map[ExtType][]string{
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

func CopyNodeRules(rules NodeRules) NodeRules {
  ret := NodeRules{}
  for id, r := range(rules) {
    ret[id] = r
  }
  return ret
}

func MergeNodeRules(first NodeRules, second NodeRules) NodeRules {
  merged := NodeRules{}
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

type Tree map[string]Tree

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

type NodeRules map[NodeID]Tree

func (rules NodeRules) MarshalJSON() ([]byte, error) {
  tmp := map[string]Tree{}
  for id, r := range(rules) {
    tmp[id.String()] = r
  }
  return json.Marshal(tmp)
}

func (rules *NodeRules) UnmarshalJSON(data []byte) error {
  tmp := map[string]Tree{}
  err := json.Unmarshal(data, &tmp)
  if err != nil {
    return err
  }

  for id_str, r := range(tmp) {
    id, err := ParseID(id_str)
    if err != nil {
      return err
    }
    ru := *rules
    ru[id] = r
  }

  return nil
}

func NewPerNodePolicy(node_actions NodeRules) PerNodePolicy {
  if node_actions == nil {
    node_actions = NodeRules{}
  }

  return PerNodePolicy{
    NodeRules: node_actions,
  }
}

type PerNodePolicy struct {
  NodeRules NodeRules `json:"node_actions"`
}

func (policy *PerNodePolicy) Type() PolicyType {
  return PerNodePolicyType
}

func (policy *PerNodePolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(policy, "", "  ")
}

func (policy *PerNodePolicy) Deserialize(ctx *Context, data []byte) error {
  return json.Unmarshal(data, policy)
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

func (policy *AllNodesPolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(policy, "", "  ")
}

func (policy *AllNodesPolicy) Deserialize(ctx *Context, data []byte) error {
  return json.Unmarshal(data, policy)
}

var DefaultPolicy = NewAllNodesPolicy(Tree{
  ErrorSignalType.String(): nil,
  ReadResultSignalType.String(): nil,
})
