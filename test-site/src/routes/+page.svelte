<script lang="ts">

import { createClient } from 'graphql-ws';
import { WebSocket } from 'ws';

const client = createClient({
  url: "ws://localhost:8080/gql",
  webSocketImpl: WebSocket,
  keepAlive: 10_000,
});

var resp = ""

console.log("STARTING_CLIENT")
client.subscribe({
    query: "{ Arenas { Owner { ... on Match { Name } } } }",
  },
  {
  next: (data) => {
    console.log("NEXT")
    console.log(data)
    resp = data.data
  },
  error: (err) => {
    console.log("ERROR")
    console.log(err)
  },
  complete: () => {
    console.log("COMPLETED")
  },
  });

</script>

<h1>{resp}</h1>
