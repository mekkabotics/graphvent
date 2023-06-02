package main

import (
  "testing"
  "fmt"
  "time"
)

type vex_tester graph_tester

func TestNewMemberAdd(t *testing.T) {
  name := "Noah"

  member := NewMember(name)
  root_event := NewEvent("", "", []Resource{member})
  event_manager := NewEventManager(root_event, []Resource{member})
  res := event_manager.FindResource(member.ID())

  if res == nil {
    t.Fatal("Failed to find member in event_manager")
  }

  if res.Name() != name || res.ID() != member.ID() {
    t.Fatal("Name/ID of returned resource did not match")
  }
}

func TestNewTeamAdd(t *testing.T) {
  name_1 := "Noah"
  name_2 := "Ben"

  org := "6659"
  team_id := "S"

  member_1 := NewMember(name_1)
  member_2 := NewMember(name_2)

  team := NewTeam(org, team_id, []*Member{member_1, member_2})

  root_event := NewEvent("", "", []Resource{team})
  event_manager := NewEventManager(root_event, []Resource{member_1, member_2, team})
  res := event_manager.FindResource(team.ID())

  if res == nil {
    t.Fatal("Failed to find team in event_manager")
  }

  if res.Name() !=  fmt.Sprintf("%s%s", org, team_id) || res.ID() != team.ID() {
    t.Fatal("Name/ID of returned team did not match")
  }

  if res.Children()[0].ID() != member_1.ID() && res.Children()[1].ID() != member_1.ID() {
    t.Fatal("Could not find member_1 in team")
  }

  if res.Children()[0].ID() != member_2.ID() && res.Children()[1].ID() != member_2.ID() {
    t.Fatal("Could not find member_2 in team")
  }

  res = event_manager.FindResource(member_1.ID())

  if res == nil {
    t.Fatal("Failed to find member_1 in event_manager")
  }

  if res.Name() != name_1 || res.ID() != member_1.ID() {
    t.Fatal("Name/ID of returned member_1 did not match")
  }
}

func TestNewAllianceAdd(t *testing.T) {
  name_1 := "Noah"
  name_2 := "Ben"

  org_1 := "6659"
  team_id_1 := "S"

  org_2 := "210"
  team_id_2 := "Y"

  member_1 := NewMember(name_1)
  member_2 := NewMember(name_2)

  team_1 := NewTeam(org_1, team_id_1, []*Member{member_1})
  team_2 := NewTeam(org_2, team_id_2, []*Member{member_2})

  alliance := NewAlliance(team_1, team_2)

  root_event := NewEvent("", "", []Resource{alliance})
  event_manager := NewEventManager(root_event, []Resource{member_1, member_2, team_1, team_2, alliance})
  res := event_manager.FindResource(alliance.ID())

  if res == nil {
    t.Fatal("Failed to find alliance in event_manager")
  }

  if res.Name() !=  fmt.Sprintf("Alliance %s/%s", team_1.Name(), team_2.Name()) || res.ID() != alliance.ID() {
    t.Fatal("Name/ID of returned alliance did not match")
  }
}

func TestNewMatch(t *testing.T) {
  name_1 := "Noah"
  name_2 := "Ben"
  name_3 := "Justin"
  name_4 := "Ian"

  org_1 := "6659"
  org_2 := "210"

  team_id_1 := "S"
  team_id_2 := "Y"

  arena_name := "Center Arena"

  member_1 := NewMember(name_1)
  member_2 := NewMember(name_2)

  member_3 := NewMember(name_3)
  member_4 := NewMember(name_4)

  team_1 := NewTeam(org_1, team_id_1, []*Member{member_1})
  team_2 := NewTeam(org_1, team_id_2, []*Member{member_2})

  team_3 := NewTeam(org_2, team_id_1, []*Member{member_3})
  team_4 := NewTeam(org_2, team_id_2, []*Member{member_4})

  alliance_1 := NewAlliance(team_1, team_2)
  alliance_2 := NewAlliance(team_3, team_4)

  arena := NewVirtualArena(arena_name)

  match := NewMatch(alliance_1, alliance_2, arena)
  root_event := NewEventQueue("root_event", "", []Resource{})
  r := root_event.DoneResource()

  event_manager := NewEventManager(root_event, []Resource{member_1, member_2, member_3, member_4, team_1, team_2, team_3, team_4, alliance_1, alliance_2, arena})
  event_manager.AddEvent(root_event, match, NewEventQueueInfo(1))

  go func() {
    time.Sleep(time.Second * 5)
    if r.Owner() != nil {
      AbortEvent(root_event)
    }
  }()

  err := event_manager.Run()
  if err != nil {
    t.Fatal(err)
  }
}
