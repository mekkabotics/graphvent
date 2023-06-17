<script lang="ts">

import Button from "./Button.svelte"

import { createClient } from 'graphql-ws';
import { WebSocket } from 'ws';

const client = createClient({
  url: "ws://localhost:8080/gqlws",
  webSocketImpl: WebSocket,
  keepAlive: 10_000,
});

var game_id = "5746b429-de0b-409b-b10f-e64bd2faa02c"

console.log("STARTING_CLIENT")


client.subscribe({
  query: "query { Arenas { Name Owner { ... on Match { Name, ID } } } }"
  },
  {
  next: (data) => {
    console.log(data)
  },
  error: (err) => {
    console.log(err)
  },
  complete: () => {
  },
});

client.subscribe({
  query: "subscription { Updates { String } }"
  },
  {
  next: (data) => {
    console.log(data)
  },
  error: (err) => {
    console.log(err)
  },
  complete: () => {
  },
});

client.subscribe({
  query: "subscription($arena_id:String) { Arena(arena_id:$arena_id) { Owner { Name }} }",
  variables: {
    arena_id: "bcd6c679-964f-4c4a-96e1-58e39032ded8"
  },
  },
  {
  next: (data) => {
    console.log("ARENA_SUB")
    console.log(data)
  },
  error: (err) => {
    console.log("ARENA_SUB")
    console.log(err)
  },
  complete: () => {
  },
});




async function match_state(match_id, state) {
  let url = "http://localhost:8080/gql"
  let data = {
    operationName: "MatchState",
    query: "mutation MatchState($match_id:String, $match_state:String) { setMatchState(id:$match_id, state:$match_state) { String } }",
    variables: {
      match_id: match_id,
      match_state: state,
    }
  }
  const response = await fetch(url, {
    method: "POST",
    mode: "cors",
    cache: "no-cache",
    credentials: "same-origin",
    headers: {
      "Content-Type": "applicaton/json",
    },
    redirect: "follow",
    referrerPolicy: "no-referrer",
    body: JSON.stringify(data),
  });
  console.log(response.json())
}

</script>

<Button on:click={()=>match_state(game_id, "queue_autonomous")}>Queue Autonomous</Button>
<Button on:click={()=>match_state(game_id, "start_autonomous")}>Start Autonomous</Button>
<Button on:click={()=>match_state(game_id, "queue_driver")}>Queue Driver</Button>
<Button on:click={()=>match_state(game_id, "start_driver")}>Start Driver</Button>
