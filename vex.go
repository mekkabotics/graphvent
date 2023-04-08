package main

import (
  "fmt"
)

type Team struct {
  BaseResource
  Members []string
  Org string
  Team string
}

func NewTeam(org string, team string, members []string) * Team {
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
      children: []Resource{},
    },
    Members: members,
    Org: org,
    Team: team,
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
