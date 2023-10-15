package graphvent

import (
  "github.com/google/uuid"
)

type Policy interface {
  Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult)
  ContinueAllows(ctx *Context, current PendingACL, signal Signal)RuleResult
  ID() uuid.UUID
}

type PolicyHeader struct {
  UUID uuid.UUID `gv:"uuid"`
}

func (header PolicyHeader) ID() uuid.UUID {
  return header.UUID
}

func (policy AllNodesPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  return nil, policy.Rules.Allows(action)
}

func (policy AllNodesPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  return Deny
}

func (policy PerNodePolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node)(Messages, RuleResult) {
  for id, actions := range(policy.NodeRules) {
    if id != principal_id {
      continue
    }
    return nil, actions.Allows(action)
  }
  return nil, Deny
}

func (policy PerNodePolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  return Deny
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

type Tree map[SerializedType]Tree

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

func NewPolicyHeader() PolicyHeader {
  return PolicyHeader{
    UUID: uuid.New(),
  }
}

func NewPerNodePolicy(node_actions map[NodeID]Tree) PerNodePolicy {
  if node_actions == nil {
    node_actions = map[NodeID]Tree{}
  }

  return PerNodePolicy{
    PolicyHeader: NewPolicyHeader(),
    NodeRules: node_actions,
  }
}

type PerNodePolicy struct {
  PolicyHeader
  NodeRules map[NodeID]Tree `gv:"node_rules"`
}

func NewAllNodesPolicy(rules Tree) AllNodesPolicy {
  return AllNodesPolicy{
    PolicyHeader: NewPolicyHeader(),
    Rules: rules,
  }
}

type AllNodesPolicy struct {
  PolicyHeader
  Rules Tree `gv:"rules"`
}

var DefaultPolicy = NewAllNodesPolicy(Tree{
  ResponseType: nil,
  StatusType: nil,
})
