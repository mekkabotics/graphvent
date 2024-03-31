package graphvent

import (
  "fmt"
)

type Message struct {
  Node NodeID
  Signal Signal
}

type MessageQueue struct {
  out chan Message
  in chan Message
  buffer []Message
  write_cursor int
  read_cursor int
}

func (queue *MessageQueue) ProcessIncoming(message Message) {
  if (queue.write_cursor + 1) == queue.read_cursor || ((queue.write_cursor + 1) == len(queue.buffer) && queue.read_cursor == 0) {
    fmt.Printf("Growing queue from %d to %d\n", len(queue.buffer), len(queue.buffer)*2)

    new_buffer := make([]Message, len(queue.buffer) * 2)

    copy(new_buffer, queue.buffer[queue.read_cursor:])
    first_chunk := len(queue.buffer) - queue.read_cursor
    copy(new_buffer[first_chunk:], queue.buffer[0:queue.write_cursor])

    queue.write_cursor = len(queue.buffer) - 1
    queue.read_cursor = 0
    queue.buffer = new_buffer
  }

  queue.buffer[queue.write_cursor] = message
  queue.write_cursor += 1
  if queue.write_cursor >= len(queue.buffer) {
    queue.write_cursor = 0
  }
}

func NewMessageQueue(initial int) (chan<- Message, <-chan Message) {
  queue := MessageQueue{
    out: make(chan Message, 0),
    in: make(chan Message, 0),
    buffer: make([]Message, initial),
    write_cursor: 0,
    read_cursor: 0,
  }

  go func(queue *MessageQueue) {
    for true {
      if queue.write_cursor != queue.read_cursor {
        select {
        case incoming := <-queue.in:
          queue.ProcessIncoming(incoming)
        case queue.out <- queue.buffer[queue.read_cursor]:
          queue.read_cursor += 1
          if queue.read_cursor >= len(queue.buffer) {
            queue.read_cursor = 0
          }
        }
      } else {
        message := <-queue.in
        queue.ProcessIncoming(message)
      }
    }
  }(&queue)

  return queue.in, queue.out
}
