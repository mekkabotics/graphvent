package graphvent

import (
  "encoding/json"
)

// A policy represents a set of rules attached to a Node that allow it to preform actions
type Policy interface {
  Node
  // Returns true if the policy allows the action on the given principal
  Allows(action string, principal NodeID) bool
}

type PerNodePolicy struct {
  GraphNode
  AllowedActions map[NodeID][]string
}

type PerNodePolicyJSON struct {
  GraphNodeJSON
  AllowedActions map[string][]string `json:"allowed_actions"`
}

func (policy *PerNodePolicy) Type() NodeType {
  return NodeType("per_node_policy")
}

func (policy *PerNodePolicy) Serialize() ([]byte, error) {
  allowed_actions := map[string][]string{}
  for principal, actions := range(policy.AllowedActions) {
    allowed_actions[principal.String()] = actions
  }

  return json.MarshalIndent(&PerNodePolicyJSON{
    GraphNodeJSON: NewGraphNodeJSON(&policy.GraphNode),
    AllowedActions: allowed_actions,
  }, "", "  ")
}

func NewPerNodePolicy(id NodeID, allowed_actions map[NodeID][]string) PerNodePolicy {
  if allowed_actions == nil {
    allowed_actions = map[NodeID][]string{}
  }

  return PerNodePolicy{
    GraphNode: NewGraphNode(id),
    AllowedActions: allowed_actions,
  }
}

func LoadPerNodePolicy(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j PerNodePolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  allowed_actions := map[NodeID][]string{}
  for principal_str, actions := range(j.AllowedActions) {
    principal_id, err := ParseID(principal_str)
    if err != nil {
      return nil, err
    }

    allowed_actions[principal_id] = actions
  }

  policy := NewPerNodePolicy(id, allowed_actions)
  nodes[id] = &policy

  err = RestoreGraphNode(ctx, &policy.GraphNode, j.GraphNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *PerNodePolicy) Allows(action string, principal NodeID) bool {
  actions, exists := policy.AllowedActions[principal]
  if exists == false {
    return false
  }

  for _, a := range(actions) {
    if a == action {
      return true
    }
  }

  return false
}

type AllNodePolicy struct {
  GraphNode
  AllowedActions []string
}

type AllNodePolicyJSON struct {
  GraphNodeJSON
  AllowedActions []string `json:"allowed_actions"`
}

func (policy *AllNodePolicy) Type() NodeType {
  return NodeType("all_node_policy")
}

func (policy *AllNodePolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(&AllNodePolicyJSON{
    GraphNodeJSON: NewGraphNodeJSON(&policy.GraphNode),
    AllowedActions: policy.AllowedActions,
  }, "", "  ")
}

func NewAllNodePolicy(id NodeID, allowed_actions []string) AllNodePolicy {
  if allowed_actions == nil {
    allowed_actions = []string{}
  }

  return AllNodePolicy{
    GraphNode: NewGraphNode(id),
    AllowedActions: allowed_actions,
  }
}

func LoadAllNodePolicy(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j AllNodePolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  policy := NewAllNodePolicy(id, j.AllowedActions)
  nodes[id] = &policy

  err = RestoreGraphNode(ctx, &policy.GraphNode, j.GraphNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *AllNodePolicy) Allows(action string, principal NodeID) bool {
  for _, a := range(policy.AllowedActions) {
    if a == action {
      return true
    }
  }
  return false
}

