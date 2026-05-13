Connection Pipeline
===================

Each accepted Unix socket connection is managed by a self-contained pipeline
that handles request decoding, concurrent dispatch, response ordering, and
graceful shutdown.


1  Pipeline Structure
=====================

```
  Unix socket
       |
       v
  +----------+     spawn per request     +------------------+
  |  reader  | ------------------------> |  request task N  |
  |  (decode |     (semaphore-bounded)   |  dispatch + exec |
  |   loop)  |                           +--------+---------+
  +----------+                                    |
                                                  | mpsc
                                                  v
                                         +--------+---------+
                                         |   writer task    |
                                         |  (single, serial)|
                                         |  encode + send   |
                                         +------------------+
                                                  |
                                                  v
                                            Unix socket
```

**Reader loop.** Decodes length-prefixed frames from the socket. For each
decoded request, runs synchronous admission (see below), then spawns an
async task to execute the command and send the response.

**Request tasks.** Each request runs in its own Tokio task, bounded by a
per-connection semaphore. Tasks dispatch through the service layer and push
their response onto a shared mpsc channel.

**Writer task.** A single task drains the response channel and writes frames
(and optional FDs) to the socket. Serializing writes through one task is
what keeps frame ordering predictable and prevents interleaved FD sends.


2  Why a Single Writer
======================

Unix domain sockets guarantee atomicity for writes up to `PIPE_BUF` (at
least 4096 bytes on all POSIX systems), but response frames can be much
larger. More importantly, direct-I/O OPEN responses require sending a
response frame followed by an ancillary FD — these must not interleave with
another response's write. A single writer task is the simplest guarantee.


3  Admission Ordering
=====================

READ requests need special treatment: a READ must acquire a handle guard
*before* being spawned as a task, so a later CLOSE (decoded from the same
inbound stream) cannot remove the handle and invalidate the READ's target.

The `RequestDispatcher::admit` call runs synchronously on the reader loop in
wire order. It produces a guard that keeps the handle alive. The async
`dispatch` method then runs in the spawned task, consuming the guard.

Splitting admission from dispatch into two steps (instead of one async
function) is deliberate: if they were merged, admission would only run when
the future is first polled, defeating the ordering guarantee for spawned
tasks.


4  Backpressure
===============

Response backlog is bounded by two dimensions:

- **Item count.** The mpsc channel has a fixed capacity.
- **Byte budget.** A READ reserves its maximum possible response bytes
  *before* cache or backend allocation. This prevents a slow consumer from
  accumulating `queued_responses * max_read_size` of in-memory payload.

If the byte budget is exhausted, new READ tasks block until earlier responses
are consumed by the writer. Non-READ responses release any reserved byte
budget immediately since they carry no large payload.

A configurable response write timeout disconnects peers that stop reading.
This prevents a stuck client from holding connection resources indefinitely.


5  Shutdown State Machine
=========================

Connection shutdown handles three distinct failure modes:

```
  Reader EOF (client closed write half)
   |
   +--- stop accepting new requests
   |
   +--- drain: bounded timeout for in-flight tasks
   |    +--- tasks finish → responses flow to writer
   |    +--- timeout → abort remaining tasks
   |
   +--- remaining budget → await writer flush
   |
   +--- close

  Protocol error / decode failure
   |
   +--- abort all tasks immediately
   +--- close

  Writer failure / task panic
   |
   +--- abort connection immediately
```

The drain timeout is a *total* budget, not per-task. Reader EOF does not
immediately close the outbound half — it gives in-flight work a chance to
complete so the client can receive responses for requests it already sent.

Request tasks are tracked in a JoinSet. Connection shutdown aborts the
JoinSet, which cancels all in-flight Tokio tasks. This is how disconnect
aborts in-flight backend fetches — the task's future is dropped, which drops
the backend request future.
