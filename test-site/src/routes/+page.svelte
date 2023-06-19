<script lang="ts">

import Button from "./Button.svelte"

import { createClient } from 'graphql-ws';
import { WebSocket } from 'ws';

const client = createClient({
  url: "ws://localhost:8080/gqlws",
  webSocketImpl: WebSocket,
  keepAlive: 10_000,
});

var game_id = null
var arena_id = null

console.log("STARTING_CLIENT")

client.subscribe({
  query: "query { Arenas { Name ID Owner { Name, ID } }}"
  },
  {
  next: (data) => {
    let obj = JSON.parse(data.data)
    arena_id = obj.Arenas[0].ID
    game_id = obj.Arenas[0].Owner.ID

    client.subscribe({
      query: "subscription($arena_id:String) { Arena(arena_id:$arena_id) { Owner { ID Name ... on Match  { State Control StateStart StateDuration } } }}",
      variables: {
        arena_id: arena_id
      },
      },
      {
      next: (data) => {
        console.log("ARENA_SUB")
        let obj = JSON.parse(data.data)
        if(obj.Arena.Owner != null) {
          game_id = obj.Arena.Owner.ID
        }
      },
      error: (err) => {
        console.log("ARENA_SUB")
        console.log(err)
      },
      complete: () => {
      },
    });
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
