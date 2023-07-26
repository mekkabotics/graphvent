package graphvent

import (
  "encoding/json"
  "fmt"
)

type Policy interface {
  Type() PolicyType
  Serialize() ([]byte, error)
  Allows(context *StateContext, principal *Node, action string, node *Node) bool
}

const ChildOfPolicyType = PolicyType("CHILD_OF")
type ChildOfPolicy struct {
  NodeActions map[NodeID][]string
}

func (policy *ChildOfPolicy) Type() PolicyType {
  return ChildOfPolicyType
}

func (policy *ChildOfPolicy) Serialize() ([]byte, error) {
  node_actions := map[string][]string{}
  for id, actions := range(policy.NodeActions) {
    node_actions[id.String()] = actions
  }
  return json.MarshalIndent(&ChildOfPolicyJSON{
    NodeActions: node_actions,
  }, "", "  ")
}

func (policy *ChildOfPolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  context.Graph.Log.Logf("policy", "CHILD_OF_POLICY: %+v", policy)
  thread_ext, err := GetExt[*ThreadExt](principal)
  if err != nil {
    return false
  }

  parent := thread_ext.Parent
  if parent != nil {
    actions, exists := policy.NodeActions[parent.ID]
    if exists == false {
      return false
    }
    for _, a := range(actions) {
      if a == action {
        return true
      }
    }
  }

  return false
}

type ChildOfPolicyJSON struct {
  NodeActions map[string][]string `json:"node_actions"`
}

func LoadChildOfPolicy(ctx *Context, data []byte) (Policy, error) {
  var j ChildOfPolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  node_actions := map[NodeID][]string{}
  for id_str, actions := range(j.NodeActions) {
    id, err := ParseID(id_str)
    if err != nil {
      return nil, err
    }

    _, err = LoadNode(ctx, id)
    if err != nil {
      return nil, err
    }

    node_actions[id] = actions
  }

  return NewChildOfPolicy(node_actions), nil
}

func NewChildOfPolicy(node_actions map[NodeID][]string) *ChildOfPolicy {
  if node_actions == nil {
    node_actions = map[NodeID][]string{}
  }

  return &ChildOfPolicy{
    NodeActions: node_actions,
  }
}

const ParentOfPolicyType = PolicyType("PARENT_OF")
type ParentOfPolicy struct {
  NodeActions map[NodeID][]string
}

func (policy *ParentOfPolicy) Type() PolicyType {
  return ParentOfPolicyType
}

func (policy *ParentOfPolicy) Serialize() ([]byte, error) {
  node_actions := map[string][]string{}
  for id, actions := range(policy.NodeActions) {
    node_actions[id.String()] = actions
  }
  return json.MarshalIndent(&ParentOfPolicyJSON{
    NodeActions: node_actions,
  }, "", "  ")
}

func (policy *ParentOfPolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  context.Graph.Log.Logf("policy", "PARENT_OF_POLICY: %+v", policy)
  for id, actions := range(policy.NodeActions) {
    thread_ext, err := GetExt[*ThreadExt](context.Graph.Nodes[id])
    if err != nil {
      continue
    }

      context.Graph.Log.Logf("policy", "PARENT_OF_PARENT: %s %+v", id, thread_ext.Parent)
    if thread_ext.Parent != nil {
      if thread_ext.Parent.ID == principal.ID {
        for _, a := range(actions) {
          if a == action {
            return true
          }
        }
      }
    }
  }

  return false
}

type ParentOfPolicyJSON struct {
  NodeActions map[string][]string `json:"node_actions"`
}

func LoadParentOfPolicy(ctx *Context, data []byte) (Policy, error) {
  var j ParentOfPolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  node_actions := map[NodeID][]string{}
  for id_str, actions := range(j.NodeActions) {
    id, err := ParseID(id_str)
    if err != nil {
      return nil, err
    }

    _, err = LoadNode(ctx, id)
    if err != nil {
      return nil, err
    }

    node_actions[id] = actions
  }

  return NewParentOfPolicy(node_actions), nil
}

func NewParentOfPolicy(node_actions map[NodeID][]string) *ParentOfPolicy {
  if node_actions == nil {
    node_actions = map[NodeID][]string{}
  }

  return &ParentOfPolicy{
    NodeActions: node_actions,
  }
}

func LoadPerNodePolicy(ctx *Context, data []byte) (Policy, error) {
  var j PerNodePolicyJSON
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  node_actions := map[NodeID][]string{}
  for id_str, actions := range(j.NodeActions) {
    id, err := ParseID(id_str)
    if err != nil {
      return nil, err
    }

    _, err = LoadNode(ctx, id)
    if err != nil {
      return nil, err
    }

    node_actions[id] = actions
  }


  return NewPerNodePolicy(node_actions, j.WildcardActions), nil
}

func NewPerNodePolicy(node_actions map[NodeID][]string, wildcard_actions []string) *PerNodePolicy {
  if node_actions == nil {
    node_actions = map[NodeID][]string{}
  }

  if wildcard_actions == nil {
    wildcard_actions = []string{}
  }

  return &PerNodePolicy{
    NodeActions: node_actions,
    WildcardActions: wildcard_actions,
  }
}

type PerNodePolicy struct {
  NodeActions map[NodeID][]string
  WildcardActions []string
}

type PerNodePolicyJSON struct {
  NodeActions map[string][]string `json:"node_actions"`
  WildcardActions []string `json:"wildcard_actions"`
}

const PerNodePolicyType = PolicyType("PER_NODE")
func (policy PerNodePolicy) Type() PolicyType {
  return PerNodePolicyType
}

func (policy PerNodePolicy) Serialize() ([]byte, error) {
  node_actions := map[string][]string{}
  for id, actions := range(policy.NodeActions) {
    node_actions[id.String()] = actions
  }

  return json.MarshalIndent(&PerNodePolicyJSON{
    NodeActions: node_actions,
    WildcardActions: policy.WildcardActions,
  }, "", "  ")
}

func (policy PerNodePolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  for _, a := range(policy.WildcardActions) {
    if a == action {
      return true
    }
  }

  for id, actions := range(policy.NodeActions) {
    if id != principal.ID {
      continue
    }
    for _, a := range(actions) {
      if a == action {
        return true
      }
    }
  }
  return false
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

func NodeList(nodes ...*Node) NodeMap {
  m := NodeMap{}
  for _, node := range(nodes) {
    m[node.ID] = node
  }
  return m
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
}

type ACLPolicyExtContext struct {
  Types map[PolicyType]PolicyInfo
}

func NewACLPolicyExtContext() *ACLPolicyExtContext {
  return &ACLPolicyExtContext{
    Types: map[PolicyType]PolicyInfo{
      PerNodePolicyType: PolicyInfo{
        Load: LoadPerNodePolicy,
      },
      ParentOfPolicyType: PolicyInfo{
        Load: LoadParentOfPolicy,
      },
      ChildOfPolicyType: PolicyInfo{
        Load: LoadChildOfPolicy,
      },
    },
  }
}

func (ext *ACLPolicyExt) Serialize() ([]byte, error) {
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

func (ext *ACLPolicyExt) Process(context *StateContext, node *Node, signal GraphSignal) error {
  return nil
}

func NewACLPolicyExt(policies map[PolicyType]Policy) *ACLPolicyExt {
  if policies == nil {
    policies = map[PolicyType]Policy{}
  }

  return &ACLPolicyExt{
    Policies: policies,
  }
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

  return NewACLPolicyExt(policies), nil
}

const ACLPolicyExtType = ExtType("ACL_POLICIES")
func (ext *ACLPolicyExt) Type() ExtType {
  return ACLPolicyExtType
}

// Check if the extension allows the principal to perform action on node
func (ext *ACLPolicyExt) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  context.Graph.Log.Logf("policy", "POLICY_EXT_ALLOWED: %+v", ext)
  for _, policy := range(ext.Policies) {
    context.Graph.Log.Logf("policy", "POLICY_CHECK_POLICY: %+v", policy)
    if policy.Allows(context, principal, action, node) == true {
      return true
    }
  }
  return false
}
