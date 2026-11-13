//! DSM layout of one run's transport prefix.
//!
//! ```text
//! [TransportHeader][receiver tokens][bootstrap][2·W² rings]
//! [source-inventory tail, written by the leader after the allocation prefix]
//! ```
//!
//! Capacity and every offset of that prefix are computed here and nowhere else;
//! a worker reconstructs the same prefix from the published header.

use std::ffi::c_void;
use std::mem::size_of;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use super::ring::RingView;

/// Rounding for the sections behind the fixed header.
const SECTION_ALIGN: usize = 64;

#[repr(C)]
struct TransportHeader {
    /// Set by the leader at the end of the run; let go of every worker.
    shutdown: AtomicBool,
    /// Attached workers `W`; the leader is process 0.
    workers: u32,
    /// Bytes per ring, a power of two.
    ring_capacity: u64,
    /// Encoded `ParallelBootstrap` bytes behind the receiver tokens.
    bootstrap_len: u64,
}

impl TransportHeader {
    /// Byte offset of the receiver tokens.
    const TOKENS_OFFSET: usize =
        size_of::<TransportHeader>().div_ceil(SECTION_ALIGN) * SECTION_ALIGN;

    const fn token_offset(proc: u32) -> usize {
        Self::TOKENS_OFFSET + proc as usize * size_of::<AtomicU64>()
    }

    /// Byte offset of the bootstrap for a run of `workers` attached workers.
    const fn bootstrap_offset(workers: u32) -> usize {
        Self::token_offset(workers + 1).div_ceil(SECTION_ALIGN) * SECTION_ALIGN
    }
}

/// Capacity and prefix sizes for one run, decided once before launch.
#[derive(Clone, Copy, Debug)]
pub(in crate::datafusion) struct TransportLayout {
    ring_capacity: usize,
    bootstrap_len: usize,
}

impl TransportLayout {
    /// Smallest ring the transport allocates.
    ///
    /// The budget is split across `2·W²` rings; the floor aligns the transport
    /// granularity with HTTP/2's minimum `MAX_FRAME_SIZE`. Must stay a power of two.
    const MIN_RING: usize = 16 * 1024;

    /// Fixes the ring capacity for a requested launch width.
    ///
    /// Recomputing it per actual width could let the prefix exceed the allocation
    /// the leader already made.
    pub(in crate::datafusion) fn new(
        launch: u32,
        queue_budget: usize,
        bootstrap_len: usize,
    ) -> Self {
        let ring_count = rings(launch);
        let candidate = queue_budget / ring_count;
        let floor_power_of_two = if candidate == 0 {
            0
        } else {
            1usize << (usize::BITS - 1 - candidate.leading_zeros())
        };
        Self {
            ring_capacity: floor_power_of_two.max(Self::MIN_RING),
            bootstrap_len,
        }
    }

    pub(in crate::datafusion) const fn ring_capacity(self) -> usize {
        self.ring_capacity
    }

    pub(in crate::datafusion) const fn bootstrap_len(self) -> usize {
        self.bootstrap_len
    }

    /// Prefix the leader allocates, sized for the requested launch width.
    pub(in crate::datafusion) const fn allocation_prefix(self, launch: u32) -> usize {
        rings_end(self.bootstrap_len, launch, self.ring_capacity)
    }

    /// Prefix actually used by the width PostgreSQL attached; `attached <=
    /// launch`, so this never exceeds [`Self::allocation_prefix`].
    fn prefix(self, attached: u32) -> usize {
        rings_end(self.bootstrap_len, attached, self.ring_capacity)
    }
}

const fn rings_end(
    bootstrap_len: usize,
    workers: u32,
    ring_capacity: usize,
) -> usize {
    rings_offset(bootstrap_len, workers) + rings_bytes(workers, ring_capacity)
}

const fn rings_offset(bootstrap_len: usize, workers: u32) -> usize {
    (TransportHeader::bootstrap_offset(workers) + bootstrap_len)
        .div_ceil(SECTION_ALIGN)
        * SECTION_ALIGN
}

const fn rings_bytes(workers: u32, ring_capacity: usize) -> usize {
    rings(workers) * RingView::stride(ring_capacity)
}

/// Rings of a run: both directions of every connection.
///
/// The allocation prefix, the initialization loop and the ring index in
/// [`TransportRegion::transmit`] all derive from this one count.
const fn rings(workers: u32) -> usize {
    2 * connections(workers)
}

/// Connections of a run: the leader to every worker plus worker to worker.
const fn connections(workers: u32) -> usize {
    (workers as usize) * (workers as usize)
}

/// Index of the connection from `client` to `server`, when that pair exists.
///
/// Participants never connect to themselves, so the index space is exactly
/// `workers²`.
fn connection_index(client: u32, server: u32, workers: u32) -> Option<usize> {
    if client > workers || server == 0 || server > workers || client == server {
        return None;
    }
    let previous = if client == 0 {
        0
    } else {
        // The leader's connections, then those of the earlier workers.
        workers as usize + (client as usize - 1) * (workers as usize - 1)
    };
    let offset = if client == 0 || server < client {
        server as usize - 1
    } else {
        server as usize - 2
    };
    Some(previous + offset)
}

/// One run's transport prefix inside the DSM mapping.
pub(in crate::datafusion) struct TransportRegion {
    base: NonNull<u8>,
    layout: TransportLayout,
    workers: u32,
}

// SAFETY: the shared memory is fixed for one run, and every access is either
// atomic or owned by exactly one participant: the mapping outlives every region,
// stream and channel built from it (the run releases its transport before
// PostgreSQL detaches the segment, and `live` covers a late poll); header fields
// and tokens are atomics; the bootstrap is written before `activate()` only;
// and each ring direction has one producer, one consumer and, per process, one
// owner, which is why `transmit` is `unsafe`.
unsafe impl Send for TransportRegion {}
unsafe impl Sync for TransportRegion {}

impl TransportRegion {
    /// Writes the header, the receiver tokens, the bootstrap and the rings.
    ///
    /// # Safety
    ///
    /// `base` must address an attached mapping that no other participant reads
    /// before `activate()`, covering the prefix `layout` gives `workers` plus the
    /// caller's inventory tail, and `layout` must have been built from
    /// `bootstrap`.
    pub(in crate::datafusion) unsafe fn initialize(
        base: NonNull<c_void>,
        workers: u32,
        layout: TransportLayout,
        bootstrap: &[u8],
    ) -> Self {
        let region = Self {
            base: base.cast(),
            layout,
            workers,
        };
        // SAFETY: the caller guarantees the prefix bytes, and every offset is
        // derived from `layout` here and nowhere else.
        unsafe {
            region
                .base
                .as_ptr()
                .cast::<TransportHeader>()
                .write(TransportHeader {
                    shutdown: AtomicBool::new(false),
                    workers,
                    ring_capacity: layout.ring_capacity() as u64,
                    bootstrap_len: layout.bootstrap_len() as u64,
                });
            for proc in 0..=workers {
                region
                    .base
                    .as_ptr()
                    .add(TransportHeader::token_offset(proc))
                    .cast::<AtomicU64>()
                    .write(AtomicU64::new(0));
            }
            std::ptr::copy_nonoverlapping(
                bootstrap.as_ptr(),
                region.ptr(TransportHeader::bootstrap_offset(workers)),
                bootstrap.len(),
            );
        }
        for index in 0..rings(workers) {
            // SAFETY: the caller guarantees the prefix holding these rings, and
            // this is the only writer before activation.
            unsafe {
                RingView::initialize(
                    region.ptr(region.rings_offset() + index * region.ring_stride()),
                    layout.ring_capacity(),
                );
            }
        }
        region
    }

    /// Rebuilds the region from the header the leader published.
    ///
    /// # Safety
    ///
    /// `base` must address the `mapping_len` bytes of an attached mapping whose
    /// leader finished [`Self::initialize`] and `activate()`.
    pub(in crate::datafusion) unsafe fn attach(
        base: NonNull<c_void>,
        mapping_len: usize,
    ) -> Self {
        // SAFETY: the mapping is attached and the leader published the header
        // before `activate()`.
        let header = unsafe { &*base.as_ptr().cast::<TransportHeader>() };
        let layout = TransportLayout {
            ring_capacity: header.ring_capacity as usize,
            bootstrap_len: header.bootstrap_len as usize,
        };
        assert!(
            layout.prefix(header.workers) <= mapping_len,
            "attached transport prefix must fit the mapped length"
        );
        Self {
            base: base.cast(),
            layout,
            workers: header.workers,
        }
    }

    /// The worker-side check that the prefix stays clear of the shared inventory
    /// tail.
    pub(in crate::datafusion) fn verify_layout(
        &self,
        mapping_len: usize,
        inventory_len: usize,
    ) {
        assert!(
            self.layout.prefix(self.workers) + inventory_len <= mapping_len,
            "attached transport prefix must stay clear of the source inventory"
        );
    }

    /// Attached workers `W`; the leader is process 0.
    pub(in crate::datafusion) const fn workers(&self) -> u32 {
        self.workers
    }

    pub(in crate::datafusion) fn bootstrap(&self) -> &[u8] {
        // SAFETY: `initialize`/`attach` validated a prefix that contains
        // `bootstrap_len` bytes from `bootstrap_offset`.
        unsafe {
            std::slice::from_raw_parts(
                self.ptr(TransportHeader::bootstrap_offset(self.workers)),
                self.layout.bootstrap_len(),
            )
        }
    }

    /// Shared "the leader ended the run" flag.
    pub(in crate::datafusion) fn shutdown(&self) -> &AtomicBool {
        // SAFETY: the header is the first section of the validated prefix.
        unsafe { &(*self.base.as_ptr().cast::<TransportHeader>()).shutdown }
    }

    /// Publishes one participant's wake target, written by that participant
    /// only: the leader cannot know a worker's `MyProcPid`.
    pub(in crate::datafusion) fn publish_token(&self, proc: u32, token: u64) {
        // SAFETY: `initialize` reserved `workers + 1` token slots.
        unsafe {
            (&*self
                .ptr(TransportHeader::token_offset(proc))
                .cast::<AtomicU64>())
                .store(token, Ordering::Release);
        }
    }

    /// The wake target of one participant.
    pub(in crate::datafusion) fn receiver_token(&self, proc: u32) -> u64 {
        // SAFETY: `initialize` wrote `workers + 1` tokens.
        unsafe {
            (&*self
                .ptr(TransportHeader::token_offset(proc))
                .cast::<AtomicU64>())
                .load(Ordering::Acquire)
        }
    }

    /// The transmit ring from `client` to `server`.
    ///
    /// # Safety
    ///
    /// The caller must be the only owner of this direction in this process for
    /// the rest of the run, or the single-producer/single-consumer discipline
    /// behind [`TransportRegion`]'s `Sync` implementation is defeated.
    pub(super) unsafe fn transmit(
        &self,
        client: u32,
        server: u32,
        from_client: bool,
    ) -> Option<RingView> {
        let connection = connection_index(client, server, self.workers)?;
        let index = 2 * connection + usize::from(!from_client);
        let chunk = self.ptr(self.rings_offset() + index * self.ring_stride());
        // SAFETY: `initialize` reserved `rings(workers)` chunks of this stride,
        // `connection_index` bounds `connection`, and the caller guarantees this
        // direction has no other view.
        Some(unsafe { RingView::new(chunk, self.layout.ring_capacity()) })
    }

    /// Reports to every worker that the run has ended.
    pub(in crate::datafusion) fn close(&self) {
        self.shutdown().store(true, Ordering::Release);
    }

    fn rings_offset(&self) -> usize {
        rings_offset(self.layout.bootstrap_len(), self.workers)
    }

    fn ring_stride(&self) -> usize {
        RingView::stride(self.layout.ring_capacity())
    }

    fn ptr(&self, offset: usize) -> *mut u8 {
        // SAFETY: every caller passes an offset derived from `layout` and
        // validated by `initialize`/`attach`.
        unsafe { self.base.as_ptr().add(offset) }
    }
}
