package graphvent

import (
  "encoding/json"
)

// A policy represents a set of rules attached to a Node that allow principals to perform actions on it
type Policy interface {
  Node
  // Returns true if the principal is allowed to perform the action on the resource
  Allows(node Node, resource string, action string, principal Node) bool
}

type NodeActions map[string][]string
func (actions NodeActions) Allows(resource string, action string) bool {
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
  SimpleNode
  Actions map[NodeID]NodeActions
}

type PerNodePolicyJSON struct {
  SimpleNodeJSON
  Actions map[string]map[string][]string `json:"actions"`
}

func (policy *PerNodePolicy) Type() NodeType {
  return NodeType("per_node_policy")
}

func (policy *PerNodePolicy) Serialize() ([]byte, error) {
  actions := map[string]map[string][]string{}
  for principal, resource_actions := range(policy.Actions) {
    actions[principal.String()] = resource_actions
  }

  return json.MarshalIndent(&PerNodePolicyJSON{
    SimpleNodeJSON: NewSimpleNodeJSON(&policy.SimpleNode),
    Actions: actions,
  }, "", "  ")
}

func NewPerNodePolicy(id NodeID, actions map[NodeID]NodeActions) PerNodePolicy {
  if actions == nil {
    actions = map[NodeID]NodeActions{}
  }

  return PerNodePolicy{
    SimpleNode: NewSimpleNode(id),
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

  err = RestoreSimpleNode(ctx, &policy.SimpleNode, j.SimpleNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *PerNodePolicy) Allows(node Node, resource string, action string, principal Node) bool {
  node_actions, exists := policy.Actions[principal.ID()]
  if exists == false {
    return false
  }

  if node_actions.Allows(resource, action) == true {
    return true
  }

  return false
}

type SimplePolicy struct {
  SimpleNode
  Actions NodeActions
}

type SimplePolicyJSON struct {
  SimpleNodeJSON
  Actions map[string][]string `json:"actions"`
}

func (policy *SimplePolicy) Type() NodeType {
  return NodeType("simple_policy")
}

func (policy *SimplePolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(&SimplePolicyJSON{
    SimpleNodeJSON: NewSimpleNodeJSON(&policy.SimpleNode),
    Actions: policy.Actions,
  }, "", "  ")
}

func NewSimplePolicy(id NodeID, actions NodeActions) SimplePolicy {
  if actions == nil {
    actions = NodeActions{}
  }

  return SimplePolicy{
    SimpleNode: NewSimpleNode(id),
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

  err = RestoreSimpleNode(ctx, &policy.SimpleNode, j.SimpleNodeJSON, nodes)
  if err != nil {
    return nil, err
  }

  return &policy, nil
}

func (policy *SimplePolicy) Allows(node Node, resource string, action string, principal Node) bool {
  return policy.Actions.Allows(resource, action)
}


type DependencyPolicy struct {
  SimplePolicy
}


func (policy *DependencyPolicy) Type() NodeType {
  return NodeType("dependency_policy")
}

func NewDependencyPolicy(id NodeID, actions NodeActions) DependencyPolicy {
  return DependencyPolicy{
    SimplePolicy: NewSimplePolicy(id, actions),
  }
}

func (policy *DependencyPolicy) Allows(node Node, resource string, action string, principal Node) bool {
  lockable, ok := node.(LockableNode)
  if ok == false {
    return false
  }

  for _, dep := range(lockable.LockableHandle().Dependencies) {
    if dep.ID() == principal.ID() {
      return policy.Actions.Allows(resource, action)
    }
  }

  return false
}

type RequirementPolicy struct {
  SimplePolicy
}


func (policy *RequirementPolicy) Type() NodeType {
  return NodeType("dependency_policy")
}

func NewRequirementPolicy(id NodeID, actions NodeActions) RequirementPolicy {
  return RequirementPolicy{
    SimplePolicy: NewSimplePolicy(id, actions),
  }
}

func (policy *RequirementPolicy) Allows(node Node, resource string, action string, principal Node) bool {
  lockable_node, ok := node.(LockableNode)
  if ok == false {
    return false
  }
  lockable := lockable_node.LockableHandle()

  for _, req := range(lockable.Requirements) {
    if req.ID() == principal.ID() {
      return policy.Actions.Allows(resource, action)
    }
  }

  return false
}

type ParentPolicy struct {
  SimplePolicy
}


func (policy *ParentPolicy) Type() NodeType {
  return NodeType("parent_policy")
}

func NewParentPolicy(id NodeID, actions NodeActions) ParentPolicy {
  return ParentPolicy{
    SimplePolicy: NewSimplePolicy(id, actions),
  }
}

func (policy *ParentPolicy) Allows(node Node, resource string, action string, principal Node) bool {
  thread_node, ok := node.(ThreadNode)
  if ok == false {
    return false
  }
  thread := thread_node.ThreadHandle()

  if thread.Owner != nil {
    if thread.Owner.ID() == principal.ID() {
      return policy.Actions.Allows(resource, action)
    }
  }

  return false
}

type ChildrenPolicy struct {
  SimplePolicy
}


func (policy *ChildrenPolicy) Type() NodeType {
  return NodeType("children_policy")
}

func NewChildrenPolicy(id NodeID, actions NodeActions) ChildrenPolicy {
  return ChildrenPolicy{
    SimplePolicy: NewSimplePolicy(id, actions),
  }
}

func (policy *ChildrenPolicy) Allows(node Node, resource string, action string, principal Node) bool {
  thread_node, ok := node.(ThreadNode)
  if ok == false {
    return false
  }
  thread := thread_node.ThreadHandle()

  for _, info := range(thread.Children) {
    if info.Child.ID() == principal.ID() {
      return policy.Actions.Allows(resource, action)
    }
  }

  return false
}
