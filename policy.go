package graphvent

import (
  "encoding/json"
  "fmt"
)

const (
  UserOfPolicyType = PolicyType("USER_OF")
  RequirementOfPolicyType = PolicyType("REQUIREMENT_OF")
  PerNodePolicyType = PolicyType("PER_NODE")
  AllNodesPolicyType = PolicyType("ALL_NODES")
)

type Policy interface {
  Serializable[PolicyType]
  Allows(principal_id NodeID, action Action, node *Node)(Messages, error)
  // Merge with another policy of the same underlying type
  Merge(Policy) Policy
  // Make a copy of this policy
  Copy() Policy
}

func (policy AllNodesPolicy) Allows(principal_id NodeID, action Action, node *Node)(Messages, error) {
  return nil, policy.Actions.Allows(action)
}

func (policy PerNodePolicy) Allows(principal_id NodeID, action Action, node *Node)(Messages, error) {
  for id, actions := range(policy.NodeActions) {
    if id != principal_id {
      continue
    }
    return nil, actions.Allows(action)
  }
  return nil, fmt.Errorf("%s is not in per node policy of %s", principal_id, node.ID)
}

func (policy *RequirementOfPolicy) Allows(principal_id NodeID, action Action, node *Node)(Messages, error) {
  lockable_ext, err := GetExt[*LockableExt](node)
  if err != nil {
    return nil, err
  }

  for id, _ := range(lockable_ext.Requirements) {
    if id == principal_id {
      return nil, policy.Actions.Allows(action)
    }
  }

  return nil, fmt.Errorf("%s is not a requirement of %s", principal_id, node.ID)
}

type UserOfPolicy struct {
  PerNodePolicy
}

func (policy *UserOfPolicy) Type() PolicyType {
  return UserOfPolicyType
}

func NewUserOfPolicy(group_actions NodeActions) UserOfPolicy {
  return UserOfPolicy{
    PerNodePolicy: NewPerNodePolicy(group_actions),
  }
}

// Send a read signal to Group to check if principal_id is a member of it
func (policy *UserOfPolicy) Allows(principal_id NodeID, action Action, node *Node) (Messages, error) {
  // Send a read signal to each of the groups in the map
  // Check for principal_id in any of the returned member lists(skipping errors)
  // Return an error in the default case
  return nil, fmt.Errorf("NOT_IMPLEMENTED")
}

func (policy *UserOfPolicy) Merge(p Policy) Policy {
  other := p.(*UserOfPolicy)
  policy.NodeActions = MergeNodeActions(policy.NodeActions, other.NodeActions)
  return policy
}

func (policy *UserOfPolicy) Copy() Policy {
  new_actions := CopyNodeActions(policy.NodeActions)
  return &UserOfPolicy{
    PerNodePolicy: NewPerNodePolicy(new_actions),
  }
}

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

func MergeNodeActions(first NodeActions, second NodeActions) NodeActions {
  merged := NodeActions{}
  for id, actions := range(first) {
    merged[id] = actions
  }
  for id, actions := range(second) {
    existing, exists := merged[id]
    if exists {
      merged[id] = MergeActions(existing, actions)
    } else {
      merged[id] = actions
    }
  }
  return merged
}

func (policy *PerNodePolicy) Merge(p Policy) Policy {
  other := p.(*PerNodePolicy)
  policy.NodeActions = MergeNodeActions(policy.NodeActions, other.NodeActions)
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

var ErrorSignalAction = Action{"ERROR_RESP"}
var ReadResultSignalAction = Action{"READ_RESULT"}
var AuthorizedSignalAction = Action{"AUTHORIZED_READ"}
var defaultPolicy = NewAllNodesPolicy(Actions{ErrorSignalAction, ReadResultSignalAction, AuthorizedSignalAction})
var DefaultACLPolicies = []Policy{
  &defaultPolicy,
}
