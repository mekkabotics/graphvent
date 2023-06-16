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

console.log("STARTING_CLIENT")
client.subscribe({
    operationName: "Sub",
    query: "query GetArenas { Arenas { Name Owner { ... on Match { Name, ID } } } } subscription Sub { Test { String } }",
  },
  {
  next: (data) => {
    console.log("NEXT")
    console.log(data)
  },
  error: (err) => {
    console.log("ERROR")
    console.log(err)
  },
  complete: () => {
    console.log("COMPLETED")
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
    mode: "same-origin",
    cache: "no-cache",
    credentials: "include",
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

<Button on:click={()=>match_state("eafd0201-caa4-4b35-a99b-869aac9455fa", "queue_autonomous")}>Queue Autonomous</Button>
