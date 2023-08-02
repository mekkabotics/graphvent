package graphvent

import (
  "encoding/json"
  "fmt"
)

type PolicyType string
func (policy PolicyType) Prefix() string { return "POLICY: " }
func (policy PolicyType) String() string { return string(policy) }

const (
  RequirementOfPolicyType = PolicyType("REQUIREMENT_OF")
  PerNodePolicyType = PolicyType("PER_NODE")
  AllNodesPolicyType = PolicyType("ALL_NODES")
)

type Policy interface {
  Serializable[PolicyType]
  Allows(principal_id NodeID, action Action, node *Node) error
  // Merge with another policy of the same underlying type
  Merge(Policy) Policy
  // Make a copy of this policy
  Copy() Policy
}

func (policy AllNodesPolicy) Allows(principal_id NodeID, action Action, node *Node) error {
  return policy.Actions.Allows(action)
}

func (policy PerNodePolicy) Allows(principal_id NodeID, action Action, node *Node) error {
  for id, actions := range(policy.NodeActions) {
    if id != principal_id {
      continue
    }
    return actions.Allows(action)
  }
  return fmt.Errorf("%s is not in per node policy of %s", principal_id, node.ID)
}

func (policy RequirementOfPolicy) Allows(principal_id NodeID, action Action, node *Node) error {
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

type RequirementOfPolicy struct {
  AllNodesPolicy
}
func (policy RequirementOfPolicy) Type() PolicyType {
  return RequirementOfPolicyType
}

func NewRequirementOfPolicy(actions Actions) RequirementOfPolicy {
  return RequirementOfPolicy{
    AllNodesPolicy: NewAllNodesPolicy(actions),
  }
}

func MergeActions(first Actions, second Actions) Actions {
  ret := second
  for _, action := range(first) {
    ret = append(ret, action)
  }
  return ret
}

func CopyNodeActions(actions NodeActions) NodeActions {
  ret := NodeActions{}
  for id, a := range(actions) {
    ret[id] = a
  }
  return ret
}

func MergeNodeActions(modified NodeActions, read NodeActions) {
  for id, actions := range(read) {
    existing, exists := modified[id]
    if exists {
      modified[id] = MergeActions(existing, actions)
    } else {
      modified[id] = actions
    }
  }
}

func (policy *PerNodePolicy) Merge(p Policy) Policy {
  other := p.(*PerNodePolicy)
  MergeNodeActions(policy.NodeActions, other.NodeActions)
  return policy
}

func (policy *PerNodePolicy) Copy() Policy {
  new_actions := CopyNodeActions(policy.NodeActions)
  return &PerNodePolicy{
    NodeActions: new_actions,
  }
}

func (policy *AllNodesPolicy) Merge(p Policy) Policy {
  other := p.(*AllNodesPolicy)
  policy.Actions = MergeActions(policy.Actions, other.Actions)
  return policy
}

func (policy *AllNodesPolicy) Copy() Policy {
  new_actions := policy.Actions
  return &AllNodesPolicy {
    Actions: new_actions,
  }
}

func (policy *RequirementOfPolicy) Merge(p Policy) Policy {
  other := p.(*RequirementOfPolicy)
  policy.Actions = MergeActions(policy.Actions, other.Actions)
  return policy
}

func (policy *RequirementOfPolicy) Copy() Policy {
  new_actions := policy.Actions
  return &RequirementOfPolicy{
    AllNodesPolicy {
      Actions: new_actions,
    },
  }
}

type Action []string

func MakeAction(parts ...interface{}) Action {
  action := make(Action, len(parts))
  for i, part := range(parts) {
    stringer, ok := part.(fmt.Stringer)
    if ok == false {
      switch p := part.(type) {
      case string:
        action[i] = p
      default:
        panic("%s can not be part of an action")
      }
    } else {
      action[i] = stringer.String()
    }
  }
  return action
}

func (action Action) Allows(test Action) bool {
  action_len := len(action)
  for i, part := range(test) {
    if i >= action_len {
      return false
    } else if action[i] == part || action[i] == "*" {
      continue
    } else if action[i] == "+" {
      break
    } else {
      return false
    }
  }

  return true
}

type Actions []Action

func (actions Actions) Allows(action Action) error {
  for _, a := range(actions) {
    if a.Allows(action) == true {
      return nil
    }
  }
  return fmt.Errorf("%s not in allows list", action)
}

type NodeActions map[NodeID]Actions

func (actions NodeActions) MarshalJSON() ([]byte, error) {
  tmp := map[string]Actions{}
  for id, a := range(actions) {
    tmp[id.String()] = a
  }
  return json.Marshal(tmp)
}

func (actions *NodeActions) UnmarshalJSON(data []byte) error {
  tmp := map[string]Actions{}
  err := json.Unmarshal(data, &tmp)
  if err != nil {
    return err
  }

  for id_str, a := range(tmp) {
    id, err := ParseID(id_str)
    if err != nil {
      return err
    }
    ac := *actions
    ac[id] = a
  }

  return nil
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
  NodeActions NodeActions `json:"node_actions"`
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

func (policy *AllNodesPolicy) Type() PolicyType {
  return AllNodesPolicyType
}

func (policy *AllNodesPolicy) Serialize() ([]byte, error) {
  return json.MarshalIndent(policy, "", "  ")
}

func (policy *AllNodesPolicy) Deserialize(ctx *Context, data []byte) error {
  return json.Unmarshal(data, policy)
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

type PolicyLoadFunc func(*Context,[]byte) (Policy, error)
type ACLExtContext struct {
  Loads map[PolicyType]PolicyLoadFunc
}

func NewACLExtContext() *ACLExtContext {
  return &ACLExtContext{
    Loads: map[PolicyType]PolicyLoadFunc{
      AllNodesPolicyType: LoadPolicy[AllNodesPolicy,*AllNodesPolicy],
      PerNodePolicyType: LoadPolicy[PerNodePolicy,*PerNodePolicy],
      RequirementOfPolicyType: LoadPolicy[RequirementOfPolicy,*RequirementOfPolicy],
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

func (ext *ACLExt) Process(ctx *Context, princ_id NodeID, node *Node, signal Signal) {
}

func (ext *ACLExt) Field(name string) interface{} {
  return ResolveFields(ext, name, map[string]func(*ACLExt)interface{}{
    "policies": func(ext *ACLExt) interface{} {
      return ext.Policies
    },
  })
}

var ErrorSignalAction = Action{"ERROR_RESP"}
var ReadResultSignalAction = Action{"READ_RESULT"}
var AuthorizedSignalAction = Action{"AUTHORIZED_READ"}
var defaultPolicy = NewAllNodesPolicy(Actions{ErrorSignalAction, ReadResultSignalAction, AuthorizedSignalAction})
var DefaultACLPolicies = []Policy{
  &defaultPolicy,
}

func NewACLExt(policies ...Policy) *ACLExt {
  policy_map := map[PolicyType]Policy{}
  for _, policy_arg := range(append(policies, DefaultACLPolicies...)) {
    policy := policy_arg.Copy()
    existing, exists := policy_map[policy.Type()]
    if exists == true {
      policy = existing.Merge(policy)
    }

    policy_map[policy.Type()] = policy
  }

  return &ACLExt{
    Policies: policy_map,
  }
}

func LoadPolicy[T any, P interface {
  *T
  Policy
}](ctx *Context, data []byte) (Policy, error) {
  p := P(new(T))
  err := p.Deserialize(ctx, data)
  if err != nil {
    return nil, err
  }

  return p, nil
}

func (ext *ACLExt) Deserialize(ctx *Context, data []byte) error {
  var j struct {
    Policies map[string][]byte `json:"policies"`
  }

  err := json.Unmarshal(data, &j)
  if err != nil {
    return err
  }

  acl_ctx, err := GetCtx[*ACLExt, *ACLExtContext](ctx)
  if err != nil {
    return err
  }
  ext.Policies = map[PolicyType]Policy{}

  for name, ser := range(j.Policies) {
    policy_load, exists := acl_ctx.Loads[PolicyType(name)]
    if exists == false {
      return fmt.Errorf("%s is not a known policy type", name)
    }
    policy, err := policy_load(ctx, ser)
    if err != nil {
      return err
    }

    ext.Policies[PolicyType(name)] = policy
  }

  return nil
}

func (ext *ACLExt) Type() ExtType {
  return ACLExtType
}

// Check if the extension allows the principal to perform action on node
func (ext *ACLExt) Allows(ctx *Context, principal_id NodeID, action Action, node *Node) error {
  errs := []error{}
  for _, policy := range(ext.Policies) {
    err := policy.Allows(principal_id, action, node)
    if err == nil {
      return nil
    }
    errs = append(errs, err)
  }
  return fmt.Errorf("POLICY_CHECK_ERRORS: %s %s.%s - %+v", principal_id, node.ID, action, errs)
}
