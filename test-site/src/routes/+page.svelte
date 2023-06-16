<script lang="ts">

import { createClient } from 'graphql-ws';
import { WebSocket } from 'ws';

const client = createClient({
  url: "ws://localhost:8080/gql",
  webSocketImpl: WebSocket,
  keepAlive: 10_000,
});

console.log("STARTING_CLIENT")
client.subscribe({
    query: "{ Arenas { Name Owner { ... on Match { Name, ID } } } }",
  },
  {
  next: (data) => {
    console.log("NEXT")
    let r = JSON.parse(data.data)
    let game_id = r.Arenas[0].Owner.ID
    console.log(game_id)
  },
  error: (err) => {
    console.log("ERROR")
  },
  complete: () => {
    console.log("COMPLETED")
  },
  });

</script>

<h1></h1>
