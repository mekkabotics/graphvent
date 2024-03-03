package graphvent

import (
  "testing"
  "time"
)

func TestGroupAdd(t *testing.T) {
  ctx := logTestContext(t, []string{"listener", "test"})

  group_listener := NewListenerExt(10)
  group, err := NewNode(ctx, nil, "Base", 10, nil, group_listener, NewGroupExt(nil))
  fatalErr(t, err)

  add_subgroup_signal := NewAddSubGroupSignal("test_group")
  messages := Messages{}
  messages = messages.Add(ctx, group.ID, group, nil, add_subgroup_signal)
  fatalErr(t, ctx.Send(messages))

  resp_1, _, err := WaitForResponse(group_listener.Chan, 10*time.Millisecond, add_subgroup_signal.Id)
  fatalErr(t, err)

  error_1, is_error := resp_1.(*ErrorSignal)
  if is_error {
    t.Fatalf("Error returned: %s", error_1.Error)
  }

  user_id := RandID()
  add_member_signal := NewAddMemberSignal("test_group", user_id)

  messages = Messages{}
  messages = messages.Add(ctx, group.ID, group, nil, add_member_signal)
  fatalErr(t, ctx.Send(messages))

  resp_2, _, err := WaitForResponse(group_listener.Chan, 10*time.Millisecond, add_member_signal.Id)
  fatalErr(t, err)

  error_2, is_error := resp_2.(*ErrorSignal)
  if is_error {
    t.Fatalf("Error returned: %s", error_2.Error)
  }

  read_signal := NewReadSignal(map[ExtType][]string{
    ExtTypeFor[GroupExt](): {"sub_groups"},
  })

  messages = Messages{}
  messages = messages.Add(ctx, group.ID, group, nil, read_signal)
  fatalErr(t, ctx.Send(messages))

  response, _, err := WaitForResponse(group_listener.Chan, 10*time.Millisecond, read_signal.Id)
  fatalErr(t, err)

  read_response := response.(*ReadResultSignal)

  sub_groups_serialized := read_response.Extensions[ExtTypeFor[GroupExt]()]["sub_groups"]

  sub_groups_type, remaining_types, err := DeserializeType(ctx, sub_groups_serialized.TypeStack)
  fatalErr(t, err)
  if len(remaining_types) > 0 {
    t.Fatalf("Types remaining after deserializing subgroups: %d", len(remaining_types))
  }

  sub_groups_value, remaining, err := DeserializeValue(ctx, sub_groups_type, sub_groups_serialized.Data)
  fatalErr(t, err)
  if len(remaining) > 0 {
    t.Fatalf("Data remaining after deserializing subgroups: %d", len(remaining_types))
  }

  sub_groups, ok := sub_groups_value.Interface().(map[string][]NodeID)
  
  if ok != true {
    t.Fatalf("sub_groups wrong type %s", sub_groups_value.Type())
  }

  if len(sub_groups) != 1 {
    t.Fatalf("sub_groups wrong length %d", len(sub_groups))
  }

  test_subgroup, exists := sub_groups["test_group"]
  if exists == false {
    t.Fatal("test_group not in subgroups")
  }

  if len(test_subgroup) != 1 {
    t.Fatalf("test_group wrong size %d/1", len(test_subgroup))
  }

  if test_subgroup[0] != user_id {
    t.Fatalf("sub_groups wrong value %s", test_subgroup[0])
  }

  ctx.Log.Logf("test", "Read Response: %+v", read_response)
}
