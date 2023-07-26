package graphvent

import (
  "encoding/json"
  "fmt"
)

type Policy interface {
  Serialize() ([]byte, error)
  Allows(context *StateContext, principal *Node, action string, node *Node) bool
}

func LoadAllNodesPolicy(ctx *Context, data []byte) (Policy, error) {
  var policy AllNodesPolicy
  err := json.Unmarshal(data, &policy)
  if err != nil {
    return policy, err
  }
  return policy, nil
}

type AllNodesPolicy struct {
  Actions []string `json:"actions"`
}

func (policy AllNodesPolicy) Type() PolicyType {
  return PolicyType("simple_policy")
}

func (policy AllNodesPolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(&policy, "", "  ")
}

// Extension to allow a node to hold ACL policies
type ACLPolicyExt struct {
  Policies map[PolicyType]Policy
}

// The ACL extension stores a map of nodes to delegate ACL to, and a list of policies
type ACLExt struct {
  Delegations NodeMap
}

func (ext *ACLExt) Process(context *StateContext, node *Node, signal GraphSignal) error {
  return nil
}

func LoadACLExt(ctx *Context, data []byte) (Extension, error) {
  var j struct {
    Delegations []string `json:"delegation"`
  }

  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  delegations, err := RestoreNodeList(ctx, j.Delegations)
  if err != nil {
    return nil, err
  }

  return &ACLExt{
    Delegations: delegations,
  }, nil
}

func NewACLExt(delegations NodeMap) *ACLExt {
  if delegations == nil {
    delegations = NodeMap{}
  }

  return &ACLExt{
    Delegations: delegations,
  }
}

func (ext *ACLExt) Serialize() ([]byte, error) {
  delegations := make([]string, len(ext.Delegations))
  i := 0
  for id, _ := range(ext.Delegations) {
    delegations[i] = id.String()
    i += 1
  }

  return json.MarshalIndent(&struct{
    Delegations []string `json:"delegations"`
  }{
    Delegations: delegations,
  }, "", "  ")
}

const ACLExtType = ExtType("ACL")
func (ext *ACLExt) Type() ExtType {
  return ACLExtType
}

type PolicyLoadFunc func(*Context, []byte) (Policy, error)
type PolicyInfo struct {
  Load PolicyLoadFunc
  Type PolicyType
}

type ACLPolicyExtContext struct {
  Types map[PolicyType]PolicyInfo
}

func NewACLPolicyExtContext() *ACLPolicyExtContext {
  return nil
}

func (ext ACLPolicyExt) Serialize() ([]byte, error) {
  policies := map[string][]byte{}
  for name, policy := range(ext.Policies) {
    ser, err := policy.Serialize()
    if err != nil {
      return nil, err
    }
    policies[string(name)] = ser
  }

  return json.MarshalIndent(&struct{
    Policies map[string][]byte `json:"policies"`
  }{
    Policies: policies,
  }, "", "  ")
}

func (ext ACLPolicyExt) Process(context *StateContext, node *Node, signal GraphSignal) error {
  return nil
}

func LoadACLPolicyExt(ctx *Context, data []byte) (Extension, error) {
  var j struct {
    Policies map[string][]byte `json:"policies"`
  }
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  policies := map[PolicyType]Policy{}
  acl_ctx := ctx.ExtByType(ACLPolicyExtType).Data.(ACLPolicyExtContext)
  for name, ser := range(j.Policies) {
    policy_def, exists := acl_ctx.Types[PolicyType(name)]
    if exists == false {
      return nil, fmt.Errorf("%s is not a known policy type", name)
    }
    policy, err := policy_def.Load(ctx, ser)
    if err != nil {
      return nil, err
    }

    policies[PolicyType(name)] = policy
  }

  return ACLPolicyExt{
    Policies: policies,
  }, nil
}

const ACLPolicyExtType = ExtType("ACL_POLICIES")
func (ext ACLPolicyExt) Type() ExtType {
  return ACLPolicyExtType
}

// Check if the extension allows the principal to perform action on node
func (ext ACLPolicyExt) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  for _, policy := range(ext.Policies) {
    if policy.Allows(context, principal, action, node) == true {
      return true
    }
  }
  return false
}

func (policy AllNodesPolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  for _, a := range(policy.Actions) {
    if a == action {
      return true
    }
  }
  return false
}

