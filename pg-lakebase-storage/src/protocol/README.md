Wire Protocol
=============

pg-lakebase-storage uses a compact binary protocol over Unix domain sockets.
The protocol is designed for local IPC between a database process and a
co-located storage service — not for cross-network use.


1  Design Choices
=================

**Fixed big-endian encoding.** Even though Unix domain sockets are local, a
fixed byte order keeps the protocol stable for non-Rust clients (C, Python
via ctypes) and independent of host CPU endianness.

**Length-prefixed framing.** Each message is preceded by a four-byte
big-endian payload length. This is the simplest framing that allows
zero-copy reads: the receiver allocates exactly once per frame after reading
the length prefix.

**Frame size cap.** Decoded frame length is capped at 64 MiB before
allocation, so a corrupt or hostile length prefix cannot cause unbounded
memory use.

**No TLS, no authentication.** The transport is a Unix socket. Access
control is filesystem permissions on the socket path.


2  Frame Layout
===============

```
  +--------+---------------------------------------------------+
  | 4 bytes|                 payload                            |
  +--------+---------------------------------------------------+
  | length |  header (15 bytes)  |  opcode + body              |
  +--------+---------------------+-----------------------------+

  Header:
    magic     u32   0x53544731  ("STG1")
    version   u16   3
    kind      u8    1 = request, 2 = response
    req_id    u64   correlates request/response pairs
```

The magic and version fields let the receiver reject garbage or incompatible
protocol versions immediately. The kind byte distinguishes requests from
responses on a shared connection (needed for multiplexed in-flight requests).
The request ID is echoed in responses so the client can match them without
ordering assumptions.


3  Opcodes
==========

```
  Code   Name                     Direction
  ----   ----                     ---------
     1   Open                     request / response
     2   Read                     request / response
     3   Close                    request / response
     4   Upload                   request / response
     5   RegisterStore            request / response
     6   UnregisterStore          request / response
     7   PurgeStoreCache          request / response
     8   InvalidateObjectCache    request / response
     9   Delete                   request / response
    10   DeletePrefix             request / response
    11   List                     request / response
    12   Head                     request / response
  1000   Error                    response only
```

Responses reuse the request opcode except for errors, which use the
dedicated Error opcode and carry an error-kind discriminant plus a UTF-8
message.


4  Field Encoding
=================

All scalar fields are big-endian:

- `bool` — 1 byte (0 or 1).
- `u16`, `u32`, `u64` — fixed width.
- `String` — 4-byte length prefix + UTF-8 bytes. String fields are
  individually capped at 1 MiB as defense against hostile length prefixes.
- `Vec<u8>` — 4-byte length prefix + raw bytes. Byte fields use the full
  frame budget as their cap.
- `Option<T>` — 1-byte presence tag (0 = absent, 1 = present) followed by
  the inner encoding when present.

Store configuration variants use a 1-byte tag to select the variant, so new
backend types can be added without changing the frame envelope.


5  FD Side Channel
==================

Responses that carry a POSIX file descriptor (direct-I/O OPEN for complete
cache files) use a two-step encoding:

1. The normal response frame is written with `direct_io: true` but without
   embedding the FD.
2. A follow-up ancillary-data write sends the duplicated FD via
   `sendmsg` / `SCM_RIGHTS`.

The client reads the response frame first, checks `direct_io`, and then
reads the FD from the ancillary channel. This preserves
request/response ordering on the main stream while keeping FD delivery
out-of-band.


6  READ Response Streaming
==========================

READ responses are special: the body can be large (up to the configured
max-read-size), and for file-backed cache entries the bytes may come from a
file range rather than an in-memory buffer. Instead of allocating the full
body, encoding it through the codec, and writing one frame, the writer:

1. Encodes a fixed-size prefix (header + opcode + eof flag + data length).
2. Streams the body directly — either copying from an in-memory buffer or
   sending a file range.

This avoids a double-copy for file-backed reads: data goes from cache file
→ socket without an intermediate codec buffer. The fixed prefix size is
known at compile time so the writer can pre-allocate exactly the right
amount.


7  Error Propagation
====================

Every error kind has a stable numeric code that travels over the wire. The
client decodes the code back into a typed error. This means error semantics
(not-found, busy, configuration, protocol, etc.) survive the Unix socket
boundary without string parsing.
