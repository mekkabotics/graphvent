package graphvent

import (
  "encoding/json"
  "fmt"
)

type Policy interface {
  Serializable[PolicyType]
  Allows(context *StateContext, principal *Node, action string, node *Node) bool
}

const RequirementOfPolicyType = PolicyType("REQUIREMENT_OF")
type RequirementOfPolicy struct {
  PerNodePolicy
}
func (policy *RequirementOfPolicy) Type() PolicyType {
  return RequirementOfPolicyType
}

func NewRequirementOfPolicy(nodes NodeActions) RequirementOfPolicy {
  return RequirementOfPolicy{
    PerNodePolicy: NewPerNodePolicy(nodes),
  }
}

// Check if any of principals dependencies are in the policy
func (policy *RequirementOfPolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  lockable_ext, err := GetExt[*LockableExt](principal)

  if err != nil {
    return false
  }

  for dep_id, _ := range(lockable_ext.Dependencies) {
    for node_id, actions := range(policy.NodeActions) {
      if node_id == dep_id {
        if actions.Allows(action) == true {
          return true
        }
        break
      }
    }
  }
  return false
}

const ChildOfPolicyType = PolicyType("CHILD_OF")
type ChildOfPolicy struct {
  PerNodePolicy
}
func (policy *ChildOfPolicy) Type() PolicyType {
  return ChildOfPolicyType
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

type Actions []string

func (actions Actions) Allows(action string) bool {
  for _, a := range(actions) {
    if a == action {
      return true
    }
  }
  return false
}

type NodeActions map[NodeID]Actions

func PerNodePolicyLoad(init_fn func(NodeActions)(Policy, error)) func(*Context, []byte)(Policy, error) {
  return func(ctx *Context, data []byte)(Policy, error){
    var j PerNodePolicyJSON
    err := json.Unmarshal(data, &j)
    if err != nil {
      return nil, err
    }

    node_actions := NodeActions{}
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

    return init_fn(node_actions)
  }
}

func NewChildOfPolicy(node_actions NodeActions) ChildOfPolicy {
  return ChildOfPolicy{
    PerNodePolicy: NewPerNodePolicy(node_actions),
  }
}

const ParentOfPolicyType = PolicyType("PARENT_OF")
type ParentOfPolicy struct {
  PerNodePolicy
}
func (policy *ParentOfPolicy) Type() PolicyType {
  return ParentOfPolicyType
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

func NewParentOfPolicy(node_actions NodeActions) ParentOfPolicy {
  return ParentOfPolicy{
    PerNodePolicy: NewPerNodePolicy(node_actions),
  }
}

func NewPerNodePolicy(node_actions NodeActions) PerNodePolicy {
  if node_actions == nil {
    node_actions = NodeActions{}
  }

  return PerNodePolicy{
    NodeActions: node_actions,
  }
}

type PerNodePolicy struct {
  NodeActions NodeActions
}

type PerNodePolicyJSON struct {
  NodeActions map[string][]string `json:"node_actions"`
}

const PerNodePolicyType = PolicyType("PER_NODE")
func (policy *PerNodePolicy) Type() PolicyType {
  return PerNodePolicyType
}

func (policy *PerNodePolicy) Serialize() ([]byte, error) {
  node_actions := map[string][]string{}
  for id, actions := range(policy.NodeActions) {
    node_actions[id.String()] = actions
  }

  return json.MarshalIndent(&PerNodePolicyJSON{
    NodeActions: node_actions,
  }, "", "  ")
}

func (policy *PerNodePolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
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

func NewAllNodesPolicy(actions Actions) AllNodesPolicy {
  if actions == nil {
    actions = Actions{}
  }

  return AllNodesPolicy{
    Actions: actions,
  }
}

type AllNodesPolicy struct {
  Actions Actions `json:"actions"`
}

const AllNodesPolicyType = PolicyType("ALL_NODES")
func (policy *AllNodesPolicy) Type() PolicyType {
  return AllNodesPolicyType
}

func (policy *AllNodesPolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(policy, "", "  ")
}

func (policy *AllNodesPolicy) Allows(context *StateContext, principal *Node, action string, node *Node) bool {
  return policy.Actions.Allows(action)
}


// Extension to allow a node to hold ACL policies
type ACLPolicyExt struct {
  Policies map[PolicyType]Policy
}

// The ACL extension stores a map of nodes to delegate ACL to, and a list of policies
type ACLExt struct {
  Delegations NodeMap
}

func (ext *ACLExt) Process(context *StateContext, node *Node, signal Signal) error {
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
        Load: PerNodePolicyLoad(func(nodes NodeActions)(Policy,error){
          policy := NewPerNodePolicy(nodes)
          return &policy, nil
        }),
      },
      ParentOfPolicyType: PolicyInfo{
        Load: PerNodePolicyLoad(func(nodes NodeActions)(Policy,error){
          policy := NewParentOfPolicy(nodes)
          return &policy, nil
        }),
      },
      ChildOfPolicyType: PolicyInfo{
        Load: PerNodePolicyLoad(func(nodes NodeActions)(Policy,error){
          policy := NewChildOfPolicy(nodes)
          return &policy, nil
        }),
      },
      RequirementOfPolicyType: PolicyInfo{
        Load: PerNodePolicyLoad(func(nodes NodeActions)(Policy,error){
          policy := NewRequirementOfPolicy(nodes)
          return &policy, nil
        }),
      },
      AllNodesPolicyType: PolicyInfo{
        Load: func(ctx *Context, data []byte) (Policy, error) {
          var policy AllNodesPolicy
          err := json.Unmarshal(data, &policy)
          if err != nil {
            return nil, err
          }
          return &policy, nil
        },
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

func (ext *ACLPolicyExt) Process(context *StateContext, node *Node, signal Signal) error {
  return nil
}

func NewACLPolicyExt(policies map[PolicyType]Policy) *ACLPolicyExt {
  if policies == nil {
    policies = map[PolicyType]Policy{}
  }

  for policy_type, policy := range(policies) {
    if policy_type != policy.Type() {
      panic("POLICY_TYPE_MISMATCH")
    }
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
  acl_ctx := ctx.ExtByType(ACLPolicyExtType).Data.(*ACLPolicyExtContext)
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
