package main

import (
  "log"
)

func fake_team(org string, id string, names []string) (*Team, []*Member) {
  members := []*Member{}
  for _, name := range(names) {
    members = append(members, NewMember(name))
  }
  team := NewTeam(org, id, members)
  return team, members
}

func fake_data() * EventManager {
  resources := []Resource{}

  teams := []*Team{}
  t1,  m1  := fake_team("6659", "A", []string{"jimmy"})
  t2,  m2  := fake_team("6659", "B", []string{"timmy"})
  t3,  m3  := fake_team("6659", "C", []string{"grace"})
  t4,  m4  := fake_team("6659", "D", []string{"jeremy"})
  t5,  m5  := fake_team("210",  "W", []string{"bobby"})
  t6,  m6  := fake_team("210",  "X", []string{"toby"})
  t7,  m7  := fake_team("210",  "Y", []string{"jennifer"})
  t8,  m8  := fake_team("210",  "Z", []string{"emily"})
  t9,  m9  := fake_team("315",  "W", []string{"bobby"})
  t10, m10 := fake_team("315",  "X", []string{"toby"})
  t11, m11 := fake_team("315",  "Y", []string{"jennifer"})
  t12, m12 := fake_team("315",  "Z", []string{"emily"})

  teams = append(teams, t1)
  teams = append(teams, t2)
  teams = append(teams, t3)
  teams = append(teams, t4)
  teams = append(teams, t5)
  teams = append(teams, t6)
  teams = append(teams, t7)
  teams = append(teams, t8)
  teams = append(teams, t9)
  teams = append(teams, t10)
  teams = append(teams, t11)
  teams = append(teams, t12)

  resources = append(resources, m1[0])
  resources = append(resources, m2[0])
  resources = append(resources, m3[0])
  resources = append(resources, m4[0])
  resources = append(resources, m5[0])
  resources = append(resources, m6[0])
  resources = append(resources, m7[0])
  resources = append(resources, m8[0])
  resources = append(resources, m9[0])
  resources = append(resources, m10[0])
  resources = append(resources, m11[0])
  resources = append(resources, m12[0])

  alliances := []*Alliance{}
  for i, team := range teams[:len(teams)-1] {
    for _, team2 := range teams[i+1:] {
      alliance := NewAlliance(team, team2)
      alliances = append(alliances, alliance)
    }
  }

  arenas := []*Arena{}
  arenas = append(arenas, NewVirtualArena("Arena 1"))
  arenas = append(arenas, NewVirtualArena("Arena 2"))
  arenas = append(arenas, NewVirtualArena("Arena 3"))

  for _, arena := range arenas {
    resources = append(resources, arena)
  }

  for _, team := range teams {
    resources = append(resources, team)
  }

  for _, alliance := range alliances {
    resources = append(resources, alliance)
  }



  root_event := NewEventQueue("root_event", "", []Resource{})
  event_manager := NewEventManager(root_event, resources)
  arena_idx := 0
  for i, alliance := range alliances[:len(alliances)-1] {
    for _, alliance2 := range alliances[i+1:] {
      match := NewMatch(alliance, alliance2, arenas[arena_idx])
      err := event_manager.AddEvent(root_event, match, NewEventQueueInfo(i))
      if err != nil {
        log.Printf("Error adding %s: %s", match.Name(), err)
      }
      arena_idx += 1
      if arena_idx >= len(arenas) {
        arena_idx = 0
      }
    }
  }



  return event_manager
}

func main() {
  event_manager := fake_data()
  log.Printf("Starting event_manager")
  err := event_manager.Run()
  if err != nil {
    log.Printf("Error running event_manager: %s", err)
  } else {
    log.Printf("Finished event_manager")
  }
}
