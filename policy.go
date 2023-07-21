package graphvent

import (
  "encoding/json"
)

// A policy represents a set of rules attached to a Node that allow principals to perform actions on it
type Policy interface {
  Node
  // Returns true if the principal is allowed to perform the action on the resource
  Allows(action string, resource string, principal Node) bool
}

type NodeActions map[string][]string
func (actions NodeActions) Allows(action string, resource string) bool {
  for _, a := range(actions[""]) {
    if a == action || a == "*" {
      return true
    }
  }

  resource_actions, exists := actions[resource]
  if exists == true {
    for _, a := range(resource_actions) {
      if a == action || a == "*" {
        return true
      }
    }
  }

  return false
}

func NewNodeActions(resource_actions NodeActions, wildcard_actions []string) NodeActions {
  if resource_actions == nil {
    resource_actions = NodeActions{}
  }
  // Wildcard actions, all actions in "" will be allowed on all resources
  if wildcard_actions == nil {
    wildcard_actions = []string{}
  }
  resource_actions[""] = wildcard_actions
  return resource_actions
}

type PerNodePolicy struct {
  GraphNode
  Actions map[NodeID]NodeActions
}

type PerNodePolicyJSON struct {
  GraphNodeJSON
  Actions map[string]map[string][]string `json:"actions"`
}

func (policy *PerNodePolicy) Type() NodeType {
  return NodeType("per_node_policy")
}

func (policy *PerNodePolicy) Serialize() ([]byte, error) {
  allowed_actions := map[string]map[string][]string{}
  for principal, actions := range(policy.Actions) {
    allowed_actions[principal.String()] = actions
  }

  return json.MarshalIndent(&PerNodePolicyJSON{
    GraphNodeJSON: NewGraphNodeJSON(&policy.GraphNode),
    Actions: allowed_actions,
  }, "", "  ")
}

func NewPerNodePolicy(id NodeID, actions map[NodeID]NodeActions) PerNodePolicy {
  if actions == nil {
    actions = map[NodeID]NodeActions{}
  }

  return PerNodePolicy{
    GraphNode: NewGraphNode(id),
    Actions: actions,
  }
}

func LoadPerNodePolicy(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j PerNodePolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  actions := map[NodeID]NodeActions{}
  for principal_str, node_actions := range(j.Actions) {
    principal_id, err := ParseID(principal_str)
    if err != nil {
      return nil, err
    }

    actions[principal_id] = node_actions
  }

  policy := NewPerNodePolicy(id, actions)
  nodes[id] = &policy

  err = RestoreGraphNode(ctx, &policy.GraphNode, j.GraphNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *PerNodePolicy) Allows(action string, resource string, principal Node) bool {
  node_actions, exists := policy.Actions[principal.ID()]
  if exists == false {
    return false
  }

  if node_actions.Allows(action, resource) == true {
    return true
  }

  return false
}

type SimplePolicy struct {
  GraphNode
  Actions NodeActions
}

type SimplePolicyJSON struct {
  GraphNodeJSON
  Actions map[string][]string `json:"actions"`
}

func (policy *SimplePolicy) Type() NodeType {
  return NodeType("simple_policy")
}

func (policy *SimplePolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(&SimplePolicyJSON{
    GraphNodeJSON: NewGraphNodeJSON(&policy.GraphNode),
    Actions: policy.Actions,
  }, "", "  ")
}

func NewSimplePolicy(id NodeID, actions NodeActions) SimplePolicy {
  if actions == nil {
    actions = NodeActions{}
  }

  return SimplePolicy{
    GraphNode: NewGraphNode(id),
    Actions: actions,
  }
}

func LoadSimplePolicy(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j SimplePolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  policy := NewSimplePolicy(id, j.Actions)
  nodes[id] = &policy

  err = RestoreGraphNode(ctx, &policy.GraphNode, j.GraphNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *SimplePolicy) Allows(action string, resource string, principal Node) bool {
  return policy.Actions.Allows(action, resource)
}

