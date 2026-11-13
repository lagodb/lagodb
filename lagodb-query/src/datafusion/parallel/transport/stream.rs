//! `DsmStream`: one duplex connection over a pair of rings.
//!
//! Reads and writes are driven by different tasks, so each direction registers
//! its own waker; sharing one `AtomicWaker` would let the later registration
//! overwrite the earlier one.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::Connected;

use super::reactor::{WakerSet, WakerSlot};
use super::region::TransportRegion;
use super::ring::RingView;
use crate::datafusion::parallel::host::ParallelExecutionHost;

/// Everything a stream needs besides its two rings.
#[derive(Clone)]
pub(in crate::datafusion) struct StreamContext {
    region: Arc<TransportRegion>,
    host: Arc<dyn ParallelExecutionHost>,
    wakers: Arc<WakerSet>,
    live: Arc<AtomicBool>,
}

impl StreamContext {
    pub(in crate::datafusion) fn new(
        region: Arc<TransportRegion>,
        host: Arc<dyn ParallelExecutionHost>,
        wakers: Arc<WakerSet>,
        live: Arc<AtomicBool>,
    ) -> Self {
        Self {
            region,
            host,
            wakers,
            live,
        }
    }

    /// The run's mapping this context's streams live in.
    pub(in crate::datafusion) fn region(&self) -> &TransportRegion {
        &self.region
    }

    /// The client end of the connection from `client` to `server`.
    pub(in crate::datafusion) fn connect(
        &self,
        client: u32,
        server: u32,
    ) -> Option<DsmStream> {
        self.stream(client, server, true)
    }

    /// The server end of the connection from `client` to `server`.
    pub(in crate::datafusion) fn accept(
        &self,
        client: u32,
        server: u32,
    ) -> Option<DsmStream> {
        self.stream(client, server, false)
    }

    fn stream(
        &self,
        client: u32,
        server: u32,
        from_client: bool,
    ) -> Option<DsmStream> {
        // SAFETY: this process owns each ring direction exactly once. A client
        // end comes from the channel cache, which resolves one stream per peer
        // (`resolver.rs`), and `ServerStreams::new` builds the server ends once
        // per run.
        let tx = unsafe { self.region.transmit(client, server, from_client)? };
        let rx = unsafe { self.region.transmit(client, server, !from_client)? };
        // One read/write pair per local stream, reserved by the local registry
        // rather than derived from the connection index.
        let (read_slot, write_slot) = self.wakers.allocate_stream();
        Some(DsmStream {
            tx,
            rx,
            region: Arc::clone(&self.region),
            peer: if from_client { server } else { client },
            host: Arc::clone(&self.host),
            wakers: Arc::clone(&self.wakers),
            read_slot,
            write_slot,
            live: Arc::clone(&self.live),
        })
    }
}

/// One duplex ring pair, viewed from one participant.
pub(in crate::datafusion) struct DsmStream {
    tx: RingView,
    rx: RingView,
    /// The run's mapping, which holds the peer's wake target.
    region: Arc<TransportRegion>,
    /// Process number of the participant at the other end.
    peer: u32,
    host: Arc<dyn ParallelExecutionHost>,
    wakers: Arc<WakerSet>,
    read_slot: WakerSlot,
    write_slot: WakerSlot,
    /// Cleared when the DSM mapping is detached or the run releases transport.
    live: Arc<AtomicBool>,
}

impl DsmStream {
    /// Wakes the process at the other end of this connection.
    ///
    /// The token is read here rather than when the stream was built: a
    /// participant publishes it only after activation, while a client connection
    /// may already exist by then.
    fn wake_peer(&self) {
        self.host.wake(self.region.receiver_token(self.peer));
    }

    /// Ends this end's writer, and its reader when `reader` is set.
    ///
    /// `poll_shutdown` only ends the writer, while a dropped stream must also
    /// report that its reader is gone. A detached mapping has nothing left to
    /// close: both closing and waking would touch unmapped memory, so this is a
    /// no-op once `live` is clear.
    fn close(&mut self, reader: bool) {
        if !self.alive() {
            return;
        }
        let writer_peer = self.tx.close_writer();
        let reader_peer = reader && self.rx.close_reader();
        if writer_peer || reader_peer {
            self.wake_peer();
        }
    }

    fn alive(&self) -> bool {
        self.live.load(Ordering::Acquire)
    }
}

impl AsyncRead for DsmStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let this = self.get_mut();
        loop {
            if !this.alive() {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            let read = this.rx.read(buf.initialize_unfilled());
            if read > 0 {
                buf.advance(read);
                if this.rx.take_writer_waiting() {
                    this.wake_peer();
                }
                return Poll::Ready(Ok(()));
            }
            // A writer publishes its last bytes and then the close flag, so EOF
            // requires observing the close flag *and* re-reading the empty ring.
            if this.rx.writer_closed() {
                if this.rx.buffered() == 0 {
                    return Poll::Ready(Ok(()));
                }
                continue;
            }
            this.wakers.register(this.read_slot, cx.waker());
            if !this.rx.park_reader() {
                return Poll::Pending;
            }
        }
    }
}

impl AsyncWrite for DsmStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[io::IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        if bufs.iter().all(|slice| slice.is_empty()) {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        loop {
            if !this.alive() {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            // Neither a locally shut-down writer nor a peer-closed reader can
            // accept another publication; `park_writer` performs the matching
            // close recheck after a full-ring wait is registered.
            if this.tx.writer_closed() || this.tx.reader_closed() {
                return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
            }
            let written = this.tx.write(bufs);
            if written > 0 {
                if this.tx.take_reader_waiting() {
                    this.wake_peer();
                }
                return Poll::Ready(Ok(written));
            }
            this.wakers.register(this.write_slot, cx.waker());
            if !this.tx.park_writer() {
                return Poll::Pending;
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    /// Nothing is buffered here: a published write is already in the ring.
    fn poll_flush(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().close(false);
        Poll::Ready(Ok(()))
    }
}

impl Drop for DsmStream {
    fn drop(&mut self) {
        // A leftover task of an older run still drops its stream after the
        // mapping is gone; `close` honours `live`.
        self.close(true);
    }
}

impl Connected for DsmStream {
    type ConnectInfo = ();

    fn connect_info(&self) {}
}

/// The server end of every connection into one worker process.
///
/// tonic multiplexes all of them on one service, so each connection is yielded
/// once and the stream then stays pending: the run's transport is fixed for its
/// whole life.
pub(in crate::datafusion) struct ServerStreams {
    accepted: std::vec::IntoIter<DsmStream>,
}

impl ServerStreams {
    pub(in crate::datafusion) fn new(
        streams: &StreamContext,
        this_proc: u32,
    ) -> Self {
        let region = streams.region();
        let mut accepted = Vec::with_capacity(region.workers() as usize);
        for client in 0..=region.workers() {
            if client == this_proc {
                continue;
            }
            accepted.extend(streams.accept(client, this_proc));
        }
        Self {
            accepted: accepted.into_iter(),
        }
    }
}

impl futures::Stream for ServerStreams {
    type Item = std::result::Result<DsmStream, io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.accepted.next() {
            Some(stream) => Poll::Ready(Some(Ok(stream))),
            // Reporting the end of `incoming` would end a non-graceful `serve`
            // immediately and drop the runtime under the accepted connections.
            None => Poll::Pending,
        }
    }
}
