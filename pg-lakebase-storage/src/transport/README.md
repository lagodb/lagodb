Transport Layer
===============

The transport layer handles Unix-socket wire mechanics: length-prefixed
framing and out-of-band file descriptor delivery. It sits below the
protocol codec (which owns payload layout) and above raw socket I/O.


1  Frame Format
===============

Each message is a length-prefixed frame:

```
  +--------+----------------------------+
  | 4 bytes|        payload             |
  +--------+----------------------------+
  | u32 BE |  protocol header + body    |
  +--------+----------------------------+
```

The four-byte big-endian length prefix tells the receiver exactly how
many bytes to read. This allows single-allocation reads: allocate once
after reading the length, then `read_exact` into the buffer.

Frame size is capped at `MAX_FRAME_BYTES` (64 MiB, defined in
`protocol`) before allocation, so a corrupt or hostile length prefix
cannot cause unbounded memory use.

Clean EOF is signalled as `Ok(None)` only when EOF occurs while reading
the four-byte length prefix. A partial payload triggers a normal I/O
error.


2  FD Side Channel
==================

Direct-I/O OPEN responses need to deliver a file descriptor to the
client. FDs travel out-of-band via `SCM_RIGHTS`:

```
  Server                              Client
    |                                   |
    +-- write_frame(response) --------> |  read_frame()
    |                                   |
    +-- FdSender::send(fd) -----------> |  read_fd_blocking() / FdReceiver
    |   (sendmsg + SCM_RIGHTS)          |  (recvmsg + SCM_RIGHTS)
```

The contract is: one framed response first, then an optional single FD
message for direct-I/O opens. This keeps length-prefix framing
independent of ancillary data parsing.

The FD message carries a single dummy byte (`0x00`) as its iov payload
because some kernels do not deliver ancillary data on zero-length
messages.


3  Async vs Blocking
====================

The transport provides both async and blocking variants:

- **Async** (`FrameReader`, `FrameWriter`, `FdSender`, `FdReceiver`) —
  used by the server and in-process wire layer. Built on Tokio's
  `AsyncRead` / `AsyncWrite`.
- **Blocking** (`read_frame_blocking`, `write_frame_blocking`,
  `read_fd_blocking`) — used by the synchronous test/tooling client
  over `std::os::unix::net::UnixStream`.

The two paths intentionally do not share a trait. The format is identical
(same length prefix, same byte order, same frame cap), but the
abstraction cost of a shared generic is not worth it for two callers.


4  Socket Binding
=================

`bind_storage_unix_listener` creates a Tokio `UnixListener` with
stale-socket handling:

- On `AddrInUse`, it probes with `UnixStream::connect` to distinguish a
  live server from a leftover socket file.
- If the probe fails (stale socket), the file is removed and bind is
  retried.
- If the probe succeeds (live server), the bind returns an error rather
  than silently stealing the socket.


5  Centralized Unsafe
=====================

All `libc` calls (`sendmsg`, `recvmsg`, `CMSG` macros) are confined to
`fd_channel.rs`. The two functions `sendmsg_with_fd` and
`recvmsg_with_fd` are the only `unsafe` blocks in the transport layer,
making auditing straightforward.


6  Error Handling
=================

All transport functions return `StorageResult<T>`:

- **Oversized frames** — `StorageError::protocol` before allocation.
- **Clean EOF** — `Ok(None)` from `read_frame`.
- **I/O failures** — propagated as `StorageError` via `From<io::Error>`.
- **FD channel failures** — `StorageError::protocol` for closed
  connections or missing FDs in the control message.
- **Bind failures** — `StorageError::io` with descriptive messages.
