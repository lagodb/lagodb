//! Provider row production and framework-owned slot publication.

use core::marker::PhantomData;

use pgrx::pg_sys;

use crate::batch::ScanBatchDriver;
use crate::customscan::error::CustomScanError;
use crate::handles::{RelationHandle, ScanDirection};
use crate::tuple::{Row, RowDatumCodec, SlotColumns, TupleSlotWriter};

use super::contract::LagodbCustomScanProvider;

/// Sealed result of one provider row-production attempt.
///
/// Providers obtain this value from [`NextSlotContext`] or
/// [`NextSlotEmitter`]; they cannot independently claim that a row was
/// produced without going through the framework-owned slot publication path.
#[must_use]
pub struct NextSlotResult<'a> {
    produced: bool,
    _callback: PhantomData<&'a mut ()>,
}

impl NextSlotResult<'_> {
    #[inline]
    pub(crate) fn is_produced(&self) -> bool {
        self.produced
    }
}

/// Slot-emission half of a [`NextSlotContext`].
///
/// Splitting the context lets a provider borrow its state and the PostgreSQL
/// output slot independently. This avoids moving a cursor out of provider
/// state on every row merely to satisfy Rust's borrowing rules.
pub struct NextSlotEmitter<'a> {
    relation: RelationHandle<'a>,
    slot: *mut pg_sys::TupleTableSlot,
    scan_direction: ScanDirection,
    per_tuple_memory_context: pg_sys::MemoryContext,
    _marker: PhantomData<&'a ()>,
}

/// Result of attempting to advance one slot-first driver.
///
/// The exhausted case returns the still-uncommitted emitter so a parallel
/// provider can attach another task and retry. The produced case does not,
/// making it impossible to publish a row and later finish the same callback as
/// EOF.
#[must_use]
pub enum NextSlotAttempt<'a> {
    Produced(NextSlotResult<'a>),
    Exhausted(NextSlotEmitter<'a>),
}

impl<'a> NextSlotEmitter<'a> {
    #[inline]
    pub fn relation(&self) -> &RelationHandle<'_> {
        &self.relation
    }

    #[inline]
    pub fn scan_direction(&self) -> ScanDirection {
        self.scan_direction
    }

    /// Attempt to emit one row from a slot-first driver.
    ///
    /// [`NextSlotAttempt::Exhausted`] means that this driver is exhausted. A
    /// parallel provider may attach another task cursor and retry with the
    /// returned emitter.
    #[inline]
    pub fn try_emit_columns<D: ScanBatchDriver>(
        mut self,
        driver: &mut D,
    ) -> Result<NextSlotAttempt<'a>, CustomScanError> {
        let direction = self.scan_direction;
        let produced =
            self.emit_with(|columns| driver.next_into_slot(direction, columns))?;
        Ok(if produced {
            NextSlotAttempt::Produced(NextSlotResult {
                produced: true,
                _callback: PhantomData,
            })
        } else {
            NextSlotAttempt::Exhausted(self)
        })
    }

    /// Emit one row, or finish the callback when the driver is exhausted.
    ///
    /// Drivers that do not need to replace their underlying task source use
    /// this terminal operation. Native-parallel adapters use
    /// [`Self::try_emit_columns`] so they can retain the uncommitted emitter
    /// while advancing to another task group.
    #[inline]
    pub fn emit_columns<D: ScanBatchDriver>(
        self,
        driver: &mut D,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        Ok(match self.try_emit_columns(driver)? {
            NextSlotAttempt::Produced(produced) => produced,
            NextSlotAttempt::Exhausted(emitter) => emitter.finish_eof(),
        })
    }

    /// Finish the callback without producing a row.
    #[inline]
    pub fn finish_eof(self) -> NextSlotResult<'a> {
        NextSlotResult {
            produced: false,
            _callback: PhantomData,
        }
    }

    fn emit_with<F>(&mut self, advance: F) -> Result<bool, CustomScanError>
    where
        F: FnOnce(&mut SlotColumns<'_>) -> crate::api::AmResult<bool>,
    {
        let slot = self.slot;
        let datum_context = self.per_tuple_memory_context;
        emit_into_slot(
            || {
                let mut columns = unsafe { SlotColumns::new(slot, datum_context) };
                advance(&mut columns)
            },
            || unsafe {
                pg_sys::ExecStoreVirtualTuple(slot);
            },
        )
    }
}

/// Context for [`LagodbCustomScanProvider::next_slot`].
pub struct NextSlotContext<'a, P: LagodbCustomScanProvider + ?Sized> {
    state: &'a mut P::State,
    emitter: NextSlotEmitter<'a>,
}

impl<'a, P: LagodbCustomScanProvider> NextSlotContext<'a, P> {
    pub(crate) fn new(
        state: &'a mut P::State,
        relation: RelationHandle<'a>,
        slot: *mut pg_sys::TupleTableSlot,
        scan_direction: ScanDirection,
        per_tuple_memory_context: pg_sys::MemoryContext,
    ) -> Self {
        Self {
            state,
            emitter: NextSlotEmitter {
                relation,
                slot,
                scan_direction,
                per_tuple_memory_context,
                _marker: PhantomData,
            },
        }
    }

    /// Borrow provider state and slot output independently for this callback.
    #[inline]
    pub fn split(self) -> (&'a mut P::State, NextSlotEmitter<'a>) {
        (self.state, self.emitter)
    }

    #[inline]
    pub fn state(&mut self) -> &mut P::State {
        self.state
    }

    #[inline]
    pub fn relation(&self) -> &RelationHandle<'_> {
        self.emitter.relation()
    }

    /// PostgreSQL's current executor scan direction.
    #[inline]
    pub fn scan_direction(&self) -> ScanDirection {
        self.emitter.scan_direction()
    }

    /// Write a row into the scan slot.
    ///
    /// # Safety
    ///
    /// `codec` must be bound to the same relation tuple descriptor as this
    /// scan context's slot.
    pub unsafe fn emit_row(
        self,
        row: &mut Row,
        codec: &RowDatumCodec,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        let writer = unsafe {
            TupleSlotWriter::new(
                self.emitter.slot,
                self.emitter.per_tuple_memory_context,
                codec,
            )
        };
        unsafe { writer.write_row(row) }.map_err(CustomScanError::from)?;
        Ok(NextSlotResult {
            produced: true,
            _callback: PhantomData,
        })
    }

    /// Drive a slot-first scan driver into the scan slot.
    pub fn emit_columns<D: ScanBatchDriver>(
        self,
        driver: &mut D,
    ) -> Result<NextSlotResult<'a>, CustomScanError> {
        self.emitter.emit_columns(driver)
    }

    /// Finish the callback without producing a row.
    #[inline]
    pub fn finish_eof(self) -> NextSlotResult<'a> {
        self.emitter.finish_eof()
    }
}

/// Shared produced-row/end-of-scan protocol for slot-first providers.
pub(crate) fn emit_into_slot<A, S>(
    advance: A,
    store: S,
) -> Result<bool, CustomScanError>
where
    A: FnOnce() -> crate::api::AmResult<bool>,
    S: FnOnce(),
{
    let found = advance().map_err(CustomScanError::from)?;
    if found {
        store();
    }
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn emit_into_slot_stores_only_produced_rows() {
        let mut stores = 0;
        assert!(
            emit_into_slot(
                || Ok::<_, crate::diag::PgReportError>(true),
                || {
                    stores += 1;
                }
            )
            .unwrap()
        );
        assert_eq!(stores, 1);

        assert!(
            !emit_into_slot(
                || Ok::<_, crate::diag::PgReportError>(false),
                || {
                    stores += 1;
                }
            )
            .unwrap()
        );
        assert_eq!(stores, 1);
    }
}
