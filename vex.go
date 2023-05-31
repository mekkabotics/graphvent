package main

import (
  "fmt"
  "log"
  "time"
)

type Arena struct {
  BaseResource
}

func NewVirtualArena(name string) * Arena {
  arena := &Arena{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: "A virtual vex arena",
        id: randid(),
        listeners: []chan error{},
      },
      parents: []Resource{},
      children: []Resource{},
    },
  }

  return arena
}

type Member struct {
  BaseResource
}

func NewMember(name string) * Member {
  member := &Member{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: "A Team Member",
        id: randid(),
        listeners: []chan error{},
      },
      parents: []Resource{},
      children: []Resource{},
    },
  }

  return member
}

type Team struct {
  BaseResource
  Org string
  Team string
}

func (team * Team) Members() []*Member {
  ret := make([]*Member, len(team.children))
  for idx, member := range(team.children) {
    ret[idx] = member.(*Member)
  }

  return ret
}

func NewTeam(org string, team string, members []*Member) * Team {
  name := fmt.Sprintf("%s%s", org, team)
  description := fmt.Sprintf("Team %s", name)
  resource := &Team{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: description,
        id: randid(),
        listeners: []chan error{},
      },
      parents: []Resource{},
      children: make([]Resource, len(members)),
    },
    Org: org,
    Team: team,
  }
  for idx, member := range(members) {
    resource.children[idx] = member
  }
  return resource
}

type Alliance struct {
  BaseResource
}

func NewAlliance(team0 * Team, team1 * Team) * Alliance {
  name := fmt.Sprintf("Alliance %s/%s", team0.Name(), team1.Name())
  description := ""

  resource := &Alliance{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: description,
        id: randid(),
        listeners: []chan error{},
      },
      parents: []Resource{},
      children: []Resource{team0, team1},
    },
  }
  return resource
}

type Match struct {
  BaseEvent
  state string
  control string
  control_start time.Time
}

const start_slack = 3000 * time.Millisecond

func NewMatch(alliance0 * Alliance, alliance1 * Alliance, arena * Arena) * Match {
  name := fmt.Sprintf("Match: %s vs. %s", alliance0.Name(), alliance1.Name() )
  description := "A vex match"

  match := &Match{
    BaseEvent: NewBaseEvent(name, description, []Resource{alliance0, alliance1, arena}),
    state: "init",
    control: "init",
    control_start: time.UnixMilli(0),
  }
  match.LockDone()

  match.actions["start"] = func() (string, error) {
    log.Printf("Starting match")
    match.control = "none"
    match.state = "scheduled"
    return "wait", nil
  }

  match.actions["queue_autonomous"] = func() (string, error) {
    match.control = "none"
    match.state = "autonomous_queued"
    match.control_start = time.Now().Add(start_slack)
    return "wait", nil
  }

  match.actions["start_autonomous"] = func() (string, error) {
    match.control = "autonomous"
    match.state = "autonomous_running"
    return "wait", nil
  }

  return match
}
