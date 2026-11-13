//! One direction of one connection: a single-producer, single-consumer byte
//! ring in shared memory.
//!
//! Positions advance with one `Release` store and need no compare-and-swap. The
//! two positions are a cache line apart so the memcpy loops do not invalidate
//! each other once per batch.

use std::io::IoSlice;
use std::mem::size_of;
use std::ptr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};

/// Contended fields are separated by this many bytes.
///
/// This is padding, not alignment: `shm_toc_allocate` only guarantees
/// `BUFFERALIGN`, so a ring header may start in the middle of a cache line. Two
/// fields a full cache line apart still never share one.
const CACHE_LINE: usize = 64;

#[repr(C)]
struct RingPositions {
    write_pos: AtomicU64,
    _writer_pad: [u8; CACHE_LINE - size_of::<u64>()],
    read_pos: AtomicU64,
    _reader_pad: [u8; CACHE_LINE - size_of::<u64>()],
}

/// The flags of the sleep handoff and of both close directions.
///
/// The handoff is the standard Dekker pattern (publish, barrier, re-read the
/// peer's state), so at least one side observes the other.
#[repr(C)]
struct RingFlags {
    writer_closed: AtomicBool,
    reader_closed: AtomicBool,
    reader_waiting: AtomicBool,
    writer_waiting: AtomicBool,
}

/// Header of one ring, at the start of its chunk.
#[repr(C)]
pub(in crate::datafusion) struct RingHeader {
    positions: RingPositions,
    flags: RingFlags,
}

impl RingHeader {
    /// Bytes reserved per ring: the flags start one cache line after the read
    /// position, and the data area one cache line further.
    const STRIDE_HEADER: usize =
        size_of::<RingHeader>().div_ceil(CACHE_LINE) * CACHE_LINE;

    fn init(header: *mut RingHeader) {
        // SAFETY: the caller reserves `STRIDE_HEADER` writable bytes here and no
        // other process reads them before activation publishes the layout. The
        // chunk sits a multiple of 64 bytes into a `BUFFERALIGN` region, which
        // covers the header's 8-byte alignment.
        unsafe {
            ptr::write(
                header,
                RingHeader {
                    positions: RingPositions {
                        write_pos: AtomicU64::new(0),
                        _writer_pad: [0; CACHE_LINE - size_of::<u64>()],
                        read_pos: AtomicU64::new(0),
                        _reader_pad: [0; CACHE_LINE - size_of::<u64>()],
                    },
                    flags: RingFlags {
                        writer_closed: AtomicBool::new(false),
                        reader_closed: AtomicBool::new(false),
                        reader_waiting: AtomicBool::new(false),
                        writer_waiting: AtomicBool::new(false),
                    },
                },
            );
        }
    }
}

/// One ring inside the mapping: the header plus its `capacity` data bytes.
///
/// `#[repr(transparent)]` cannot describe a variable-length tail, so the data
/// pointer is carried next to the header pointer.
pub(in crate::datafusion) struct RingView {
    header: *mut RingHeader,
    data: *mut u8,
    capacity: usize,
}

// SAFETY: the mapping outlives every view, which `TransportRegion` guarantees by
// owning it for the whole run. Producer/consumer discipline comes from the
// connection that created the view.
unsafe impl Send for RingView {}

impl RingView {
    /// Views an initialized ring chunk without writing to it.
    ///
    /// # Safety
    ///
    /// `chunk` must hold `RingView::stride(capacity)` mapped bytes whose header
    /// was initialized by [`Self::initialize`], and `capacity` must be a non-zero
    /// power of two.
    pub(in crate::datafusion) unsafe fn new(chunk: *mut u8, capacity: usize) -> Self {
        Self {
            header: chunk.cast::<RingHeader>(),
            // SAFETY: the caller guarantees the chunk covers the header and the
            // data area behind it.
            data: unsafe { chunk.add(RingHeader::STRIDE_HEADER) },
            capacity,
        }
    }

    /// Initializes a ring chunk and views it.
    ///
    /// # Safety
    ///
    /// `chunk` must hold `RingView::stride(capacity)` writable bytes of the
    /// leader's mapping that no participant reads before `activate()`.
    pub(in crate::datafusion) unsafe fn initialize(
        chunk: *mut u8,
        capacity: usize,
    ) -> Self {
        RingHeader::init(chunk.cast::<RingHeader>());
        // SAFETY: the caller guarantees the chunk's size.
        unsafe { Self::new(chunk, capacity) }
    }

    /// Bytes one ring occupies, header included.
    pub(in crate::datafusion) const fn stride(capacity: usize) -> usize {
        RingHeader::STRIDE_HEADER + capacity
    }

    fn header(&self) -> &RingHeader {
        // SAFETY: `new` requires the header to stay mapped while this view is
        // used.
        unsafe { &*self.header }
    }

    fn positions(&self) -> (u64, u64) {
        let positions = &self.header().positions;
        (
            positions.write_pos.load(Ordering::Acquire),
            positions.read_pos.load(Ordering::Acquire),
        )
    }

    pub(in crate::datafusion) fn free_space(&self) -> usize {
        let (write, read) = self.positions();
        self.capacity - (write - read) as usize
    }

    pub(in crate::datafusion) fn buffered(&self) -> usize {
        let (write, read) = self.positions();
        (write - read) as usize
    }

    /// Copies what fits of `slices` and publishes the new write position once.
    ///
    /// One publication keeps a vectored write one wakeup decision, and empty
    /// slices are skipped so a caller passing them cannot stall the stream.
    pub(in crate::datafusion) fn write(&self, slices: &[IoSlice<'_>]) -> usize {
        let (write, read) = self.positions();
        let mut free = self.capacity - (write - read) as usize;
        let mut written = 0;
        let mut start = (write as usize) & (self.capacity - 1);
        for slice in slices {
            if slice.is_empty() {
                continue;
            }
            if free == 0 {
                break;
            }
            let take = slice.len().min(free);
            let first = take.min(self.capacity - start);
            // SAFETY: `start..start + take` wraps inside a `capacity` byte area
            // owned by this mapping, and `slice` is readable for `take` bytes.
            unsafe {
                ptr::copy_nonoverlapping(slice.as_ptr(), self.data.add(start), first);
                ptr::copy_nonoverlapping(
                    slice.as_ptr().add(first),
                    self.data,
                    take - first,
                );
            }
            written += take;
            free -= take;
            start = (start + take) & (self.capacity - 1);
        }
        if written > 0 {
            self.header()
                .positions
                .write_pos
                .store(write + written as u64, Ordering::Release);
        }
        written
    }

    /// Copies what fits of the buffered bytes into `out`.
    pub(in crate::datafusion) fn read(&self, out: &mut [u8]) -> usize {
        let (write, read) = self.positions();
        let count = out.len().min((write - read) as usize);
        if count == 0 {
            return 0;
        }
        let start = (read as usize) & (self.capacity - 1);
        let first = count.min(self.capacity - start);
        // SAFETY: `start..start + count` wraps inside the mapping's `capacity`
        // byte area, and `out` is writable for `count` bytes.
        unsafe {
            ptr::copy_nonoverlapping(self.data.add(start), out.as_mut_ptr(), first);
            ptr::copy_nonoverlapping(
                self.data,
                out.as_mut_ptr().add(first),
                count - first,
            );
        }
        self.header()
            .positions
            .read_pos
            .store(read + count as u64, Ordering::Release);
        count
    }

    pub(in crate::datafusion) fn writer_closed(&self) -> bool {
        self.header().flags.writer_closed.load(Ordering::Acquire)
    }

    pub(in crate::datafusion) fn reader_closed(&self) -> bool {
        self.header().flags.reader_closed.load(Ordering::Acquire)
    }

    /// Publishes the writer's intent to sleep, then rechecks what the reader
    /// publishes. `true` means progress is already possible.
    pub(in crate::datafusion) fn park_writer(&self) -> bool {
        let flags = &self.header().flags;
        flags.writer_waiting.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let progressed =
            self.free_space() > 0 || flags.reader_closed.load(Ordering::Acquire);
        if progressed {
            flags.writer_waiting.store(false, Ordering::SeqCst);
        }
        progressed
    }

    /// Symmetric counterpart of [`Self::park_writer`] for the reader.
    pub(in crate::datafusion) fn park_reader(&self) -> bool {
        let flags = &self.header().flags;
        flags.reader_waiting.store(true, Ordering::SeqCst);
        fence(Ordering::SeqCst);
        let progressed =
            self.buffered() > 0 || flags.writer_closed.load(Ordering::Acquire);
        if progressed {
            flags.reader_waiting.store(false, Ordering::SeqCst);
        }
        progressed
    }

    /// Reports whether the peer was waiting after a position or close flag was
    /// published.
    ///
    /// The publishing half of the handoff: the fence orders that `Release` store
    /// before this read of the peer's flag, as the fence in `park_reader` orders
    /// the flag before its re-read, and as `SetLatch` places
    /// `pg_memory_barrier()` between `is_set` and `maybe_sleeping`.
    pub(in crate::datafusion) fn take_reader_waiting(&self) -> bool {
        fence(Ordering::SeqCst);
        self.header()
            .flags
            .reader_waiting
            .swap(false, Ordering::SeqCst)
    }

    /// Symmetric counterpart of [`Self::take_reader_waiting`].
    pub(in crate::datafusion) fn take_writer_waiting(&self) -> bool {
        fence(Ordering::SeqCst);
        self.header()
            .flags
            .writer_waiting
            .swap(false, Ordering::SeqCst)
    }

    /// Closes this direction and reports whether the peer was waiting.
    ///
    /// The close state joins the same handoff as a position change, or a close
    /// that landed before the peer published its intent would be lost.
    pub(in crate::datafusion) fn close_writer(&self) -> bool {
        self.header()
            .flags
            .writer_closed
            .store(true, Ordering::Release);
        self.take_reader_waiting()
    }

    pub(in crate::datafusion) fn close_reader(&self) -> bool {
        self.header()
            .flags
            .reader_closed
            .store(true, Ordering::Release);
        self.take_writer_waiting()
    }
}
