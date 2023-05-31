package main

import (
  "fmt"
  "log"
)

type ArenaDriver interface {

}

type VirtualArenaDriver struct {

}

type Arena struct {
  BaseResource
  driver ArenaDriver
}

func NewVirtualArena(name string) * Arena {
  arena := &Arena{
    BaseResource: BaseResource{
      BaseNode: BaseNode{
        name: name,
        description: "A virtual vex arena",
        id: gql_randid(),
        listeners: []chan error{},
      },
      parents: []Resource{},
      children: []Resource{},
    },
    driver: VirtualArenaDriver{},
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
        id: gql_randid(),
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
        id: gql_randid(),
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
        id: gql_randid(),
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
}

func NewMatch(alliance0 * Alliance, alliance1 * Alliance, arena * Arena) * Match {
  name := fmt.Sprintf("Match: %s vs. %s", alliance0.Name(), alliance1.Name() )
  description := "A vex match"

  match := &Match{
    BaseEvent: NewBaseEvent(name, description, []Resource{alliance0, alliance1, arena}),
    state: "init",
  }
  match.LockDone()

  match.actions["start"] = func() (string, error) {
    // put the match into "scheduled" state
    log.Printf("Starting match")
    match.state = "scheduled"
    return "wait", nil
  }

  match.actions["start_autonomous"] = func() (string, error) {
    if match.state != "scheduled" {
      log.Printf("Cannot start_autonomous when the match is in %s", match.state)
      return "wait", nil
    }
    log.Printf("Starting autonomous")
    return "wait", nil
  }

  return match
}
