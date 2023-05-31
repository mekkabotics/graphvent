package main

import (
  "log"
)

func fake_data() * EventManager {
  resources := []Resource{}

  teams := []*Team{}
  teams = append(teams, NewTeam("6659", "A", []*Member{NewMember("jimmy")}))
  teams = append(teams, NewTeam("6659", "B", []*Member{NewMember("timmy")}))
  teams = append(teams, NewTeam("6659", "C", []*Member{NewMember("grace")}))
  teams = append(teams, NewTeam("6659", "D", []*Member{NewMember("jeremy")}))
  teams = append(teams, NewTeam("210",  "W", []*Member{NewMember("bobby")}))
  teams = append(teams, NewTeam("210",  "X", []*Member{NewMember("toby")}))
  teams = append(teams, NewTeam("210",  "Y", []*Member{NewMember("jennifer")}))
  teams = append(teams, NewTeam("210",  "Z", []*Member{NewMember("emily")}))
  teams = append(teams, NewTeam("315",  "W", []*Member{NewMember("bobby")}))
  teams = append(teams, NewTeam("315",  "X", []*Member{NewMember("toby")}))
  teams = append(teams, NewTeam("315",  "Y", []*Member{NewMember("jennifer")}))
  teams = append(teams, NewTeam("315",  "Z", []*Member{NewMember("emily")}))

  for _, team := range teams {
    resources = append(resources, team)
  }

  for i, team := range teams[:len(teams)-1] {
    for _, team2 := range teams[i+1:] {
      alliance := NewAlliance(team, team2)
      resources = append(resources, alliance)
    }
  }

  root_event := NewEventQueue("root_event", "", []Resource{})

  event_manager := NewEventManager(root_event, resources)

  return event_manager
}

func main() {
  event_manager := fake_data()
  log.Printf("Starting event_manager: %+v", event_manager)
}
