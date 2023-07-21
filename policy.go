package graphvent

import (
  "encoding/json"
)

// A policy represents a set of rules attached to a Node that allow it to preform actions
type Policy interface {
  Node
  // Returns true if the policy allows the action on the given principal
  Allows(action string, resource string, principal NodeID) bool
}

type NodeActions map[string][]string
func (actions NodeActions) Allows(action string, resource string) bool {
  for _, a := range(actions[""]) {
    if a == action {
      return true
    }
  }

  resource_actions, exists := actions[resource]
  if exists == true {
    for _, a := range(resource_actions) {
      if a == action {
        return true
      }
    }
  }

  return false
}

func NewNodeActions(wildcard_actions []string) NodeActions {
  actions := NodeActions{}
  // Wildcard actions, all actions in "" will be allowed on all resources
  if wildcard_actions == nil {
    wildcard_actions = []string{}
  }
  actions[""] = wildcard_actions
  return actions
}

type PerNodePolicy struct {
  GraphNode
  NodeActions map[NodeID]NodeActions
  WildcardActions NodeActions
}

type PerNodePolicyJSON struct {
  GraphNodeJSON
  NodeActions map[string]map[string][]string `json:"allowed_actions"`
  WildcardActions map[string][]string `json:"wildcard_actions"`
}

func (policy *PerNodePolicy) Type() NodeType {
  return NodeType("per_node_policy")
}

func (policy *PerNodePolicy) Serialize() ([]byte, error) {
  allowed_actions := map[string]map[string][]string{}
  for principal, actions := range(policy.NodeActions) {
    allowed_actions[principal.String()] = actions
  }

  return json.MarshalIndent(&PerNodePolicyJSON{
    GraphNodeJSON: NewGraphNodeJSON(&policy.GraphNode),
    NodeActions: allowed_actions,
    WildcardActions: policy.WildcardActions,
  }, "", "  ")
}

func NewPerNodePolicy(id NodeID, node_actions map[NodeID]NodeActions, wildcard_actions NodeActions) PerNodePolicy {
  if node_actions == nil {
    node_actions = map[NodeID]NodeActions{}
  }

  if wildcard_actions == nil {
    wildcard_actions = NewNodeActions(nil)
  }

  return PerNodePolicy{
    GraphNode: NewGraphNode(id),
    NodeActions: node_actions,
    WildcardActions: wildcard_actions,
  }
}

func LoadPerNodePolicy(ctx *Context, id NodeID, data []byte, nodes NodeMap) (Node, error) {
  var j PerNodePolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  allowed_actions := map[NodeID]NodeActions{}
  for principal_str, actions := range(j.NodeActions) {
    principal_id, err := ParseID(principal_str)
    if err != nil {
      return nil, err
    }

    allowed_actions[principal_id] = actions
  }

  policy := NewPerNodePolicy(id, allowed_actions, j.WildcardActions)
  nodes[id] = &policy

  err = RestoreGraphNode(ctx, &policy.GraphNode, j.GraphNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *PerNodePolicy) Allows(action string, resource string, principal NodeID) bool {
  // Check wildcard actions
  if policy.WildcardActions.Allows(action, resource) == true {
    return true
  }

  node_actions, exists := policy.NodeActions[principal]
  if exists == false {
    return false
  }

  if node_actions.Allows(action, resource) == true {
    return true
  }

  return false
}
