package graphvent

import (
  "github.com/google/uuid"
  "slices"
  "time"
)

type ACLSignal struct {
  SignalHeader
  Principal NodeID `gv:"principal"`
  Action Tree `gv:"tree"`
}

func NewACLSignal(principal NodeID, action Tree) *ACLSignal {
  return &ACLSignal{
    SignalHeader: NewSignalHeader(Direct),
    Principal: principal,
    Action: action,
  }
}

var DefaultACLPolicy = NewAllNodesPolicy(Tree{
  SerializedType(ACLSignalType): nil,
})

func (signal ACLSignal) Permission() Tree {
  return Tree{
    SerializedType(ACLSignalType): nil,
  }
}

type ACLExt struct {
  Policies []Policy `gv:"policies"`
  PendingACLs map[uuid.UUID]PendingACL `gv:"pending_acls"`
  Pending map[uuid.UUID]PendingACLSignal `gv:"pending"`
}

func NewACLExt(policies []Policy) *ACLExt {
  return &ACLExt{
    Policies: policies,
    PendingACLs: map[uuid.UUID]PendingACL{},
    Pending: map[uuid.UUID]PendingACLSignal{},
  }
}

func (ext *ACLExt) Process(ctx *Context, node *Node, source NodeID, signal Signal) (Messages, Changes) {
  response, is_response := signal.(ResponseSignal)
  if is_response == true {
    var messages Messages = nil
    var changes = Changes{}
    info, waiting := ext.Pending[response.ResponseID()]
    if waiting == true {
      changes.Add(ACLExtType, "pending")
      delete(ext.Pending, response.ResponseID())
      if response.ID() != info.Timeout {
        err := node.DequeueSignal(info.Timeout)
        if err != nil {
          ctx.Log.Logf("acl", "timeout dequeue error: %s", err)
        }
      }

      acl_info, found := ext.PendingACLs[info.ID]
      if found == true {
        acl_info.Counter -= 1
        acl_info.Responses = append(acl_info.Responses, response)

        policy_index := slices.IndexFunc(ext.Policies, func(policy Policy) bool {
          return policy.ID() == info.Policy
        })

        if policy_index == -1 {
          ctx.Log.Logf("acl", "pending signal for nonexistent policy")
          delete(ext.PendingACLs, info.ID)
          err := node.DequeueSignal(acl_info.TimeoutID)
          if err != nil {
            ctx.Log.Logf("acl", "acl proxy timeout dequeue error: %s", err)
          }
        } else {
          if ext.Policies[policy_index].ContinueAllows(ctx, acl_info, response) == Allow {
            changes.Add(ACLExtType, "pending_acls")
            delete(ext.PendingACLs, info.ID)
            ctx.Log.Logf("acl", "Request delayed allow")
            messages = messages.Add(ctx, acl_info.Source, node, nil, NewSuccessSignal(info.ID))
            err := node.DequeueSignal(acl_info.TimeoutID)
            if err != nil {
              ctx.Log.Logf("acl", "acl proxy timeout dequeue error: %s", err)
            }
          } else if acl_info.Counter == 0 {
            changes.Add(ACLExtType, "pending_acls")
            delete(ext.PendingACLs, info.ID)
            ctx.Log.Logf("acl", "Request delayed deny")
            messages = messages.Add(ctx, acl_info.Source, node, nil, NewErrorSignal(info.ID, "acl_denied"))
            err := node.DequeueSignal(acl_info.TimeoutID)
            if err != nil {
              ctx.Log.Logf("acl", "acl proxy timeout dequeue error: %s", err)
            }
          } else {
            node.PendingACLs[info.ID] = acl_info
            changes.Add(ACLExtType, "pending_acls")
          }
        }
      }
    }
    return messages, changes
  }

  var messages Messages = nil
  var changes = Changes{}

  switch sig := signal.(type) {
  case *ACLSignal:
    var acl_messages map[uuid.UUID]Messages = nil
    denied := true
    for _, policy := range(ext.Policies) {
      policy_messages, result := policy.Allows(ctx, sig.Principal, sig.Action, node)
      if result == Allow {
        denied = false
        break
      } else if result == Pending {
        if len(policy_messages) == 0 {
          ctx.Log.Logf("acl", "Pending result for %s with no messages returned", policy.ID())
          continue
        } else if acl_messages == nil {
          acl_messages = map[uuid.UUID]Messages{}
          denied = false
        }

        acl_messages[policy.ID()] = policy_messages
        ctx.Log.Logf("acl", "Pending result for %s:%s - %+v", node.ID, policy.ID(), acl_messages)
      }
    }

    if denied == true {
      ctx.Log.Logf("acl", "Request denied")
      messages = messages.Add(ctx, source, node, nil, NewErrorSignal(sig.Id, "acl_denied"))
    } else if acl_messages != nil {
      ctx.Log.Logf("acl", "Request pending")
      changes.Add(ACLExtType, "pending")
      total_messages := 0
      // TODO: reasonable timeout/configurable
      timeout_time := time.Now().Add(time.Second)
      for policy_id, policy_messages := range(acl_messages) {
        total_messages += len(policy_messages)
        for _, message := range(policy_messages) {
          timeout_signal := NewTimeoutSignal(message.Signal.ID())
          ext.Pending[message.Signal.ID()] = PendingACLSignal{
            Policy: policy_id,
            Timeout: timeout_signal.Id,
            ID: sig.Id,
          }
          node.QueueSignal(timeout_time, timeout_signal)
          messages = append(messages, message)
        }
      }

      acl_timeout := NewACLTimeoutSignal(sig.Id)
      node.QueueSignal(timeout_time, acl_timeout)
      ext.PendingACLs[sig.Id] = PendingACL{
        Counter: total_messages,
        Responses: []ResponseSignal{},
        TimeoutID: acl_timeout.Id,
        Action: sig.Action,
        Principal: sig.Principal,

        Source: source,
        Signal: signal,
      }
    } else {
      ctx.Log.Logf("acl", "Request allowed")
      messages = messages.Add(ctx, source, node, nil, NewSuccessSignal(sig.Id))
    }
    // Test an action against the policy list, sending any intermediate signals necessary and seeting Pending and PendingACLs accordingly. Add a TimeoutSignal for every message awaiting a response, and an ACLTimeoutSignal for the overall request
  case *ACLTimeoutSignal:
    acl_info, exists := ext.PendingACLs[sig.ReqID]
    if exists == true {
      delete(ext.PendingACLs, sig.ReqID)
      changes.Add(ACLExtType, "pending_acls")
      ctx.Log.Logf("acl", "Request timeout deny")
      messages = messages.Add(ctx, acl_info.Source, node, nil, NewErrorSignal(sig.ReqID, "acl_timeout"))
      err := node.DequeueSignal(acl_info.TimeoutID)
      if err != nil {
        ctx.Log.Logf("acl", "acl proxy timeout dequeue error: %s", err)
      }
    } else {
      ctx.Log.Logf("acl", "ACL_TIMEOUT_SIGNAL for passed acl")
    }
    // Delete from PendingACLs
  }

  return messages, changes
}

type ACLProxyPolicy struct {
  PolicyHeader
  Proxies []NodeID `gv:"proxies"`
}

func NewACLProxyPolicy(proxies []NodeID) ACLProxyPolicy {
  return ACLProxyPolicy{
    NewPolicyHeader(),
    proxies,
  }
}

func (policy ACLProxyPolicy) Allows(ctx *Context, principal_id NodeID, action Tree, node *Node) (Messages, RuleResult) {
  if len(policy.Proxies) == 0 {
    return nil, Deny
  }

  messages := Messages{}
  for _, proxy := range(policy.Proxies) {
    messages = messages.Add(ctx, proxy, node, nil, NewACLSignal(principal_id, action))
  }

  return messages, Pending
}

func (policy ACLProxyPolicy) ContinueAllows(ctx *Context, current PendingACL, signal Signal) RuleResult {
  _, is_success := signal.(*SuccessSignal)
  if is_success == true {
    return Allow
  }
  return Deny
}

