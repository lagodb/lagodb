use core::ffi::CStr;
use core::marker::PhantomData;

use crate::customscan::codec::{PrivateDataReader, PrivateDataWriter};
use crate::customscan::custom_private::CustomScanPrivate;
use crate::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LakebaseCustomScanProvider, NextSlotContext,
    PathVariant, PlanTranslateContext, ReScanContext, RelPathContext,
};
use crate::expr::predicate::PlanPredicate;
use crate::expr::split::{
    ColumnRef, PlanPushdownSplit, PushdownContract, PushdownCosting, PushedExpr,
    QualPushdownDecision,
};
use pgrx::pg_sys;

pub(crate) struct NoopPrivate;

impl CustomScanPrivate for NoopPrivate {
    fn encode(&self, _writer: &mut PrivateDataWriter) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn decode(_reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError> {
        Ok(NoopPrivate)
    }
}

pub(crate) trait NoopProviderSpec: 'static {
    const NAME: &'static CStr;
    type State: 'static;

    fn state() -> Self::State;
}

pub(crate) struct NoopProvider<S>(PhantomData<S>);

impl<S: NoopProviderSpec> LakebaseCustomScanProvider for NoopProvider<S> {
    const NAME: &'static CStr = S::NAME;
    type PrivateData = NoopPrivate;
    type State = S::State;

    fn supports_relation(_ctx: &RelPathContext) -> bool {
        false
    }

    fn classify_predicate(
        _ctx: &PlanTranslateContext,
        _predicate: &PlanPredicate<'_>,
    ) -> QualPushdownDecision {
        QualPushdownDecision::Unsupported
    }

    fn create_path(
        _ctx: &RelPathContext,
        _variant: &PathVariant<'_>,
        _builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        None
    }

    fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
        S::state()
    }

    fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
        unreachable!("NoopProvider::begin is not exercised by host-only tests")
    }

    fn next_slot(_ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError> {
        unreachable!("NoopProvider::next_slot is not exercised by host-only tests")
    }

    fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
        unreachable!("NoopProvider::rescan is not exercised by host-only tests")
    }

    fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
        unreachable!("NoopProvider::end is not exercised by host-only tests")
    }
}

pub(crate) struct PushdownSplitFixture {
    namespace: u64,
}

impl PushdownSplitFixture {
    pub(crate) const fn new(namespace: u64) -> Self {
        Self { namespace }
    }

    pub(crate) fn relids(tag: u64) -> *mut pg_sys::Bitmapset {
        ((tag + 1) * 8) as usize as *mut pg_sys::Bitmapset
    }

    fn expr(&self, section: u64, index: usize) -> *mut pg_sys::Expr {
        let value = self.namespace * 1_000_000 + section * 10_000 + index as u64 + 1;
        value as usize as *mut pg_sys::Expr
    }

    fn pushed_from_contracts(
        &self,
        contracts: &[PushdownContract],
    ) -> Vec<PushedExpr> {
        contracts
            .iter()
            .enumerate()
            .map(|(index, &contract)| PushedExpr {
                expr: self.expr(0, index),
                contract,
                costing: PushdownCosting::CostedPruning,
            })
            .collect()
    }

    pub(crate) fn split_from_contracts(
        &self,
        residual_len: usize,
        recheck_len: usize,
        contracts: &[PushdownContract],
        column_expr_indexes: &[usize],
    ) -> PlanPushdownSplit {
        let pushed = self.pushed_from_contracts(contracts);
        let residual = (0..residual_len).map(|i| self.expr(1, i)).collect();
        let recheck = (0..recheck_len).map(|i| self.expr(2, i)).collect();
        let column_refs = column_expr_indexes
            .iter()
            .enumerate()
            .map(|(k, &expr_index)| ColumnRef {
                expr_index,
                rel_oid: pg_sys::Oid::from(
                    16_000u32 + self.namespace as u32 * 100 + k as u32,
                ),
                attno: expr_index as pg_sys::AttrNumber + 1,
                atttypid: pg_sys::INT4OID,
                attcollation: pg_sys::Oid::INVALID,
                name: Some(format!("ns{}_col{k}", self.namespace)),
            })
            .collect();

        PlanPushdownSplit {
            residual,
            pushed,
            recheck,
            column_refs,
        }
    }

    pub(crate) fn split_exact_counts(
        &self,
        pushed_len: usize,
        residual_len: usize,
        recheck_len: usize,
    ) -> PlanPushdownSplit {
        let contracts = vec![PushdownContract::ExactRowFilter; pushed_len];
        self.split_from_contracts(residual_len, recheck_len, &contracts, &[])
    }

    pub(crate) fn split_alternating_contracts(
        &self,
        pushed_len: usize,
        residual_len: usize,
        recheck_len: usize,
        column_expr_indexes: &[usize],
    ) -> PlanPushdownSplit {
        let contracts: Vec<PushdownContract> = (0..pushed_len)
            .map(|index| {
                if index % 2 == 0 {
                    PushdownContract::ExactRowFilter
                } else {
                    PushdownContract::ConservativePruning
                }
            })
            .collect();
        self.split_from_contracts(
            residual_len,
            recheck_len,
            &contracts,
            column_expr_indexes,
        )
    }
}
