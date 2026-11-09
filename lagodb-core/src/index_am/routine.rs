//! Per-call `IndexAmRoutine` construction and capability registration.

use std::marker::PhantomData;

use pgrx::{AllocatedByRust, PgBox, pg_sys};

use super::build::IndexBuildPhases;
use super::opclass::IndexAdjustMembers;
#[cfg(feature = "pg18")]
use super::opclass::IndexStrategyTranslation;
use super::planning::IndexReturnable;
#[cfg(feature = "pg18")]
use super::planning::IndexTreeHeight;
use super::properties::IndexPropertyProvider;
use super::scan::{
    IndexBitmapScan, IndexMarkRestore, IndexParallelScan, IndexTupleScan,
};
use super::{
    IndexAccessMethod, IndexAmRoutine, build, insert, opclass, options, planning,
    properties, scan, vacuum,
};

/// Build the routine an index access method handler returns. Every call
/// allocates a fresh node: PostgreSQL `pfree`s it after use
/// (`InitIndexAmRoutine`).
pub fn index_am_routine<A: IndexAccessMethod>() -> IndexAmRoutine {
    let mut builder = IndexAmRoutineBuilder::<A>::new();
    A::register(&mut builder);
    builder.routine
}

/// Each method installs one complete PostgreSQL callback group.
pub struct IndexAmRoutineBuilder<A> {
    routine: IndexAmRoutine,
    _am: PhantomData<fn() -> A>,
}

impl<A: IndexAccessMethod> IndexAmRoutineBuilder<A> {
    fn new() -> Self {
        // SAFETY: PostgreSQL allocates IndexAmRoutine as a node; ownership
        // passes to the handler's caller once the routine is returned.
        let mut routine = unsafe {
            PgBox::<pg_sys::IndexAmRoutine, AllocatedByRust>::alloc_node(
                pg_sys::NodeTag::T_IndexAmRoutine,
            )
            .into_pg_boxed()
        };
        A::PROPERTIES.write_to(&mut routine);

        routine.ambuild = Some(build::ambuild::<A>);
        routine.ambuildempty = Some(build::ambuildempty::<A>);
        routine.aminsert = Some(insert::aminsert::<A>);
        #[cfg(any(feature = "pg17", feature = "pg18"))]
        {
            routine.aminsertcleanup = Some(insert::aminsertcleanup::<A>);
        }
        routine.ambulkdelete = Some(vacuum::ambulkdelete::<A>);
        routine.amvacuumcleanup = Some(vacuum::amvacuumcleanup::<A>);
        routine.amcostestimate = Some(planning::amcostestimate::<A>);
        routine.amoptions = Some(options::amoptions::<A>);
        routine.amvalidate = Some(opclass::amvalidate::<A>);
        routine.ambeginscan = Some(scan::ambeginscan::<A>);
        routine.amrescan = Some(scan::amrescan::<A>);
        routine.amendscan = Some(scan::amendscan::<A>);

        Self {
            routine,
            _am: PhantomData,
        }
    }

    pub fn tuple_scan(&mut self) -> &mut Self
    where
        A::ScanSession: IndexTupleScan,
    {
        self.routine.amgettuple = Some(scan::amgettuple::<A>);
        self
    }

    pub fn bitmap_scan(&mut self) -> &mut Self
    where
        A::ScanSession: IndexBitmapScan,
    {
        self.routine.amgetbitmap = Some(scan::amgetbitmap::<A>);
        self
    }

    /// Index-only scans fetch through `amgettuple`, so this also registers
    /// [`Self::tuple_scan`].
    pub fn index_only_scan(&mut self) -> &mut Self
    where
        A: IndexReturnable,
        A::ScanSession: IndexTupleScan,
    {
        self.routine.amcanreturn = Some(planning::amcanreturn::<A>);
        self.tuple_scan()
    }

    pub fn mark_restore(&mut self) -> &mut Self
    where
        A::ScanSession: IndexMarkRestore,
    {
        self.routine.ammarkpos = Some(scan::ammarkpos::<A>);
        self.routine.amrestrpos = Some(scan::amrestrpos::<A>);
        self
    }

    pub fn parallel_scan(&mut self) -> &mut Self
    where
        A::ScanSession: IndexParallelScan,
    {
        scan::assert_parallel_shared_layout::<A::ScanSession>();
        self.routine.amcanparallel = true;
        self.routine.amestimateparallelscan = Some(scan::amestimateparallelscan::<A>);
        self.routine.aminitparallelscan = Some(scan::aminitparallelscan::<A>);
        self.routine.amparallelrescan = Some(scan::amparallelrescan::<A>);
        self
    }

    pub fn property(&mut self) -> &mut Self
    where
        A: IndexPropertyProvider,
    {
        self.routine.amproperty = Some(properties::amproperty::<A>);
        self
    }

    pub fn build_phase_name(&mut self) -> &mut Self
    where
        A: IndexBuildPhases,
    {
        self.routine.ambuildphasename = Some(build::ambuildphasename::<A>);
        self
    }

    pub fn adjust_members(&mut self) -> &mut Self
    where
        A: IndexAdjustMembers,
    {
        self.routine.amadjustmembers = Some(opclass::amadjustmembers::<A>);
        self
    }

    #[cfg(feature = "pg18")]
    pub fn tree_height(&mut self) -> &mut Self
    where
        A: IndexTreeHeight,
    {
        self.routine.amgettreeheight = Some(planning::amgettreeheight::<A>);
        self
    }

    #[cfg(feature = "pg18")]
    pub fn strategy_translation(&mut self) -> &mut Self
    where
        A: IndexStrategyTranslation,
    {
        self.routine.amtranslatestrategy = Some(opclass::amtranslatestrategy::<A>);
        self.routine.amtranslatecmptype = Some(opclass::amtranslatecmptype::<A>);
        self
    }
}
