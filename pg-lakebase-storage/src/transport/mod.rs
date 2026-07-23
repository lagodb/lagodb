//! Unix connection framing: `u32` big-endian length prefix then payload bytes.
//!
//! Direct-I/O `OPEN` responses write the ordinary frame first, then send a one-byte
//! [`SCM_RIGHTS`](libc::SCM_RIGHTS) token through [`FdSender`] / [`FdReceiver`] so frame ordering
//! stays independent from ancillary FD delivery.
//!
//! The async path ([`FrameReader`] / [`FrameWriter`] / [`FdSender`] / [`FdReceiver`]) is used by the
//! server and by the in-process wire layer. The blocking path ([`blocking`]) mirrors it for the
//! synchronous test/tooling APIs. The production synchronous client supplies a
//! runtime-aware nonblocking `Read`/`Write` adapter and uses the same framing
//! functions. Ancillary-data unsafe code remains confined to [`fd_channel`].

mod bind;
mod blocking;
mod fd_channel;
mod frame;

pub use bind::bind_storage_unix_listener;
pub(crate) use blocking::BlockingFrameCursor;
pub use blocking::{read_fd_blocking, read_frame_blocking, write_frame_blocking};
pub(crate) use fd_channel::try_recv_fd;
pub use fd_channel::{FdReceiver, FdSender};
pub use frame::{FrameReader, FrameWriter, read_frame, write_frame};
