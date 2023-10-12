package graphvent

import (
  "testing"
  "time"
)

func TestGroupAdd(t *testing.T) {
  ctx := logTestContext(t, []string{"listener", "test"})

  group_listener := NewListenerExt(10)
  group, err := NewNode(ctx, nil, GroupNodeType, 10, nil, group_listener, NewGroupExt(nil))
  fatalErr(t, err)

  user_id := RandID()
  add_member_signal := NewAddMemberSignal(user_id)

  messages := Messages{}
  messages = messages.Add(ctx, group.ID, group.Key, add_member_signal, group.ID)
  fatalErr(t, ctx.Send(messages))

  _, err = WaitForResponse(group_listener.Chan, 10*time.Millisecond, add_member_signal.Id)
  fatalErr(t, err)

  read_signal := NewReadSignal(map[ExtType][]string{
    GroupExtType: {"members"},
  })

  messages = Messages{}
  messages = messages.Add(ctx, group.ID, group.Key, read_signal, group.ID)
  fatalErr(t, ctx.Send(messages))

  response, err := WaitForResponse(group_listener.Chan, 10*time.Millisecond, read_signal.Id)
  fatalErr(t, err)

  read_response := response.(*ReadResultSignal)

  members_serialized := read_response.Extensions[GroupExtType]["members"]

  _, member_value, remaining, err := DeserializeValue(ctx, members_serialized)

  if len(remaining.Data) > 0 {
    t.Fatalf("Data remaining after deserializing member list: %d", len(remaining.Data))
  }

  member_list, ok := member_value.Interface().([]NodeID)
  
  if ok != true {
    t.Fatalf("member_list wrong type %s", member_value.Type())
  }

  if len(member_list) != 1 {
    t.Fatalf("member_list wrong length %d", len(member_list))
  }

  if member_list[0] != user_id {
    t.Fatalf("member_list wrong value %s", member_list[0])
  }

  ctx.Log.Logf("test", "Read Response: %+v", read_response)
}
