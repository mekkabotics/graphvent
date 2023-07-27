package graphvent

import (
  "encoding/json"
  "fmt"
)

type Policy interface {
  Serializable[PolicyType]
  Allows(context *StateContext, principal_id NodeID, action string, node *Node) error
}

//TODO: Update with change from principal *Node to principal_id so sane policies can still be made
func (policy *AllNodesPolicy) Allows(context *StateContext, principal_id NodeID, action string, node *Node) error {
  return policy.Actions.Allows(action)
}

func (policy *PerNodePolicy) Allows(context *StateContext, principal_id NodeID, action string, node *Node) error {
  for id, actions := range(policy.NodeActions) {
    if id != principal_id {
      continue
    }
    for _, a := range(actions) {
      if a == action {
        return nil
      }
    }
  }
  return fmt.Errorf("%s is not in per node policy of %s", principal_id, node.ID)
}

func (policy *RequirementOfPolicy) Allows(context *StateContext, principal_id NodeID, action string, node *Node) error {
  lockable_ext, err := GetExt[*LockableExt](node)
  if err != nil {
    return err
  }

  for id, _ := range(lockable_ext.Requirements) {
    if id == principal_id {
      return policy.Actions.Allows(action)
    }
  }

  return fmt.Errorf("%s is not a requirement of %s", principal_id, node.ID)
}

func (policy *ParentOfPolicy) Allows(context *StateContext, principal_id NodeID, action string, node *Node) error {
  thread_ext, err := GetExt[*ThreadExt](node)
  if err != nil {
    return err
  }

  if thread_ext.Parent != nil {
    if thread_ext.Parent.ID == principal_id {
      return policy.Actions.Allows(action)
    }
  }

  return fmt.Errorf("%s is not a parent of %s", principal_id, node.ID)
}

func (policy *ChildOfPolicy) Allows(context *StateContext, principal_id NodeID, action string, node *Node) error {
  thread_ext, err := GetExt[*ThreadExt](node)
  if err != nil {
    return err
  }

  for id, _ := range(thread_ext.Children) {
    if id == principal_id {
      return policy.Actions.Allows(action)
    }
  }

  return fmt.Errorf("%s is not a child of %s", principal_id, node.ID)
}

const RequirementOfPolicyType = PolicyType("REQUIREMENT_OF")
type RequirementOfPolicy struct {
  AllNodesPolicy
}
func (policy *RequirementOfPolicy) Type() PolicyType {
  return RequirementOfPolicyType
}

func NewRequirementOfPolicy(actions Actions) RequirementOfPolicy {
  return RequirementOfPolicy{
    AllNodesPolicy: NewAllNodesPolicy(actions),
  }
}

const ChildOfPolicyType = PolicyType("CHILD_OF")
type ChildOfPolicy struct {
  AllNodesPolicy
}
func (policy *ChildOfPolicy) Type() PolicyType {
  return ChildOfPolicyType
}

type Actions []string

func (actions Actions) Allows(action string) error {
  for _, a := range(actions) {
    if a == action {
      return nil
    }
  }
  return fmt.Errorf("%s not in allows list", action)
}

type NodeActions map[NodeID]Actions

type AllNodesPolicyJSON struct {
  Actions Actions `json:"actions"`
}

func AllNodesPolicyLoad(init_fn func(Actions)(Policy, error)) func(*Context, []byte)(Policy, error) {
  return func(ctx *Context, data []byte)(Policy, error){
    var j AllNodesPolicyJSON
    err := json.Unmarshal(data, &j)

    if err != nil {
      return nil, err
    }

    return init_fn(j.Actions)
  }
}

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

func NewChildOfPolicy(actions Actions) ChildOfPolicy {
  return ChildOfPolicy{
    AllNodesPolicy: NewAllNodesPolicy(actions),
  }
}

const ParentOfPolicyType = PolicyType("PARENT_OF")
type ParentOfPolicy struct {
  AllNodesPolicy
}
func (policy *ParentOfPolicy) Type() PolicyType {
  return ParentOfPolicyType
}

func NewParentOfPolicy(actions Actions) ParentOfPolicy {
  return ParentOfPolicy{
    AllNodesPolicy: NewAllNodesPolicy(actions),
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

func NewAllNodesPolicy(actions Actions) AllNodesPolicy {
  if actions == nil {
    actions = Actions{}
  }

  return AllNodesPolicy{
    Actions: actions,
  }
}

type AllNodesPolicy struct {
  Actions Actions
}

const AllNodesPolicyType = PolicyType("ALL_NODES")
func (policy *AllNodesPolicy) Type() PolicyType {
  return AllNodesPolicyType
}

func (policy *AllNodesPolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(policy, "", "  ")
}

// Extension to allow a node to hold ACL policies
type ACLExt struct {
  Policies map[PolicyType]Policy
}


func NodeList(nodes ...*Node) NodeMap {
  m := NodeMap{}
  for _, node := range(nodes) {
    m[node.ID] = node
  }
  return m
}

type PolicyLoadFunc func(*Context, []byte) (Policy, error)
type PolicyInfo struct {
  Load PolicyLoadFunc
}

type ACLExtContext struct {
  Types map[PolicyType]PolicyInfo
}

func NewACLExtContext() *ACLExtContext {
  return &ACLExtContext{
    Types: map[PolicyType]PolicyInfo{
      AllNodesPolicyType: PolicyInfo{
        Load: AllNodesPolicyLoad(func(actions Actions)(Policy, error){
          policy := NewAllNodesPolicy(actions)
          return &policy, nil
        }),
      },
      PerNodePolicyType: PolicyInfo{
        Load: PerNodePolicyLoad(func(nodes NodeActions)(Policy,error){
          policy := NewPerNodePolicy(nodes)
          return &policy, nil
        }),
      },
      ParentOfPolicyType: PolicyInfo{
        Load: AllNodesPolicyLoad(func(actions Actions)(Policy, error){
          policy := NewParentOfPolicy(actions)
          return &policy, nil
        }),
      },
      ChildOfPolicyType: PolicyInfo{
        Load: AllNodesPolicyLoad(func(actions Actions)(Policy, error){
          policy := NewChildOfPolicy(actions)
          return &policy, nil
        }),
      },
      RequirementOfPolicyType: PolicyInfo{
        Load: AllNodesPolicyLoad(func(actions Actions)(Policy, error){
          policy := NewRequirementOfPolicy(actions)
          return &policy, nil
        }),
      },
    },
  }
}

func (ext *ACLExt) Serialize() ([]byte, error) {
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

func (ext *ACLExt) Process(context *StateContext, node *Node, signal Signal) error {
  return nil
}

func NewACLExt(policies map[PolicyType]Policy) *ACLExt {
  if policies == nil {
    policies = map[PolicyType]Policy{}
  }

  for policy_type, policy := range(policies) {
    if policy_type != policy.Type() {
      panic("POLICY_TYPE_MISMATCH")
    }
  }

  return &ACLExt{
    Policies: policies,
  }
}

func LoadACLExt(ctx *Context, data []byte) (Extension, error) {
  var j struct {
    Policies map[string][]byte `json:"policies"`
  }
  err := json.Unmarshal(data, &j)
  if err != nil {
    return nil, err
  }

  policies := map[PolicyType]Policy{}
  acl_ctx := ctx.ExtByType(ACLExtType).Data.(*ACLExtContext)
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

  return NewACLExt(policies), nil
}

const ACLExtType = ExtType("ACL_POLICIES")
func (ext *ACLExt) Type() ExtType {
  return ACLExtType
}

// Check if the extension allows the principal to perform action on node
func (ext *ACLExt) Allows(context *StateContext, principal_id NodeID, action string, node *Node) error {
  context.Graph.Log.Logf("policy", "POLICY_EXT_ALLOWED: %+v", ext)
  errs := []error{}
  for _, policy := range(ext.Policies) {
    err := policy.Allows(context, principal_id, action, node)
    if err == nil {
      return nil
    }
    errs = append(errs, err)
  }
  return fmt.Errorf("POLICY_CHECK_ERRORS: %s %s.%s - %+v", principal_id, node.ID, action, errs)
}
