//! Hook-integration tests for the generic CustomScan framework.
//!
//! Unlike `hook.rs`, these tests exercise the real `set_rel_pathlist_hook`
//! router through SQL planning, using a dummy provider registered from `_PG_init`.
//! That provider is installed into the process-global registry for the lifetime
//! of the `pg-backend-tests` extension. Keep its relation-name prefixes
//! unique to this module so unrelated tests do not accidentally match it.

use std::error::Error;
use std::ffi::CStr;
use std::fmt::{self, Display, Formatter};
use std::sync::OnceLock;

use lagodb_core::customscan::provider::{
    BeginContext, CreateStateContext, CustomPathBuilder, CustomPathPlan,
    CustomScanError, EndContext, LagodbCustomScanProvider, NextSlotContext,
    PathContext, PathVariant, PathVariantKind, ReScanContext, RelationContext,
    register_provider,
};
use lagodb_core::customscan::provider::{
    CustomScanPrivate, PrivateDataReader, PrivateDataWriter,
};
use lagodb_core::diag::SqlStateError;
use lagodb_core::expr::PushdownCosting;
use lagodb_core::expr::pushdown::{
    FilterBindResult, FilterFragment, FilterNode, FilterPlan, FilterPlanningContext,
    FilterPushdown, FilterPushdownPlanner, FilterScalar, FilterValueBindings,
    FilterValueSlotId,
};
use lagodb_core::plan_data::{PlanDataError, PlanDataReader, PlanDataWriter};
use pgrx::pg_sys;
use pgrx::prelude::PgSqlErrorCode;

const PROVIDER_NAME: &CStr = c"hook-integration-test-provider";
const PLAIN_REL_PREFIX: &str = "hook_plain_";
const JOIN_REL_PREFIX: &str = "hook_join_";
const WIDEN_REL_PREFIX: &str = "hook_widen_";
const BIND_ERROR_REL_PREFIX: &str = "hook_bind_error_";
const CODEC_WRONG_TAG_REL_PREFIX: &str = "hook_codec_wrong_tag_";
const CODEC_TRAILING_REL_PREFIX: &str = "hook_codec_trailing_";
const INT4EQ_OPNO: u32 = 96;

#[derive(Clone, Copy)]
enum HookTestMode {
    Plain,
    Join,
    Widening,
    BindError,
    CodecWrongTag,
    CodecTrailing,
}

impl HookTestMode {
    fn for_relation_name(name: &str) -> Option<Self> {
        if name.starts_with(PLAIN_REL_PREFIX) {
            Some(Self::Plain)
        } else if name.starts_with(JOIN_REL_PREFIX) {
            Some(Self::Join)
        } else if name.starts_with(WIDEN_REL_PREFIX) {
            Some(Self::Widening)
        } else if name.starts_with(BIND_ERROR_REL_PREFIX) {
            Some(Self::BindError)
        } else if name.starts_with(CODEC_WRONG_TAG_REL_PREFIX) {
            Some(Self::CodecWrongTag)
        } else if name.starts_with(CODEC_TRAILING_REL_PREFIX) {
            Some(Self::CodecTrailing)
        } else {
            None
        }
    }

    fn path_kind(self) -> PathVariantKind {
        match self {
            Self::Join => PathVariantKind::JoinParameterized,
            Self::Plain
            | Self::Widening
            | Self::BindError
            | Self::CodecWrongTag
            | Self::CodecTrailing => PathVariantKind::Plain,
        }
    }

    fn codec_mode(self) -> HookCodecMode {
        match self {
            Self::CodecWrongTag => HookCodecMode::WrongTag,
            Self::CodecTrailing => HookCodecMode::Trailing,
            Self::Plain | Self::Join | Self::Widening | Self::BindError => {
                HookCodecMode::Valid
            }
        }
    }
}

pub(crate) fn install_hook_integration_provider() {
    static INIT: OnceLock<()> = OnceLock::new();

    INIT.get_or_init(|| {
        register_provider::<HookIntegrationProvider>();
        lagodb_core::customscan::init();
    });
}

struct HookIntegrationPrivate;

impl CustomScanPrivate for HookIntegrationPrivate {
    fn encode(&self, _writer: &mut PrivateDataWriter) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn decode(_reader: &mut PrivateDataReader<'_>) -> Result<Self, CustomScanError> {
        Ok(Self)
    }
}

struct HookIntegrationState;

struct HookIntegrationProvider;

#[derive(Debug)]
enum HookFilterError {
    Codec(PlanDataError),
    InvalidSlot { index: usize },
    BindFailure,
}

impl From<PlanDataError> for HookFilterError {
    fn from(error: PlanDataError) -> Self {
        Self::Codec(error)
    }
}

impl Display for HookFilterError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Codec(error) => Display::fmt(error, formatter),
            Self::InvalidSlot { index } => write!(
                formatter,
                "hook integration planned filter references invalid slot {index}"
            ),
            Self::BindFailure => {
                formatter.write_str("hook integration provider bind_filter failed")
            }
        }
    }
}

impl Error for HookFilterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::InvalidSlot { .. } | Self::BindFailure => None,
        }
    }
}

impl SqlStateError for HookFilterError {
    fn sql_error_code(&self) -> PgSqlErrorCode {
        PgSqlErrorCode::ERRCODE_INTERNAL_ERROR
    }
}

struct HookFilterPlanner {
    mode: HookTestMode,
}

#[derive(Clone, Copy, Debug)]
enum HookCodecMode {
    Valid,
    WrongTag,
    Trailing,
}

#[derive(Debug)]
struct HookPlannedFilter {
    values: Box<[FilterValueSlotId]>,
    codec_mode: HookCodecMode,
    bind_error: bool,
}

impl HookFilterPlanner {
    fn equality_value(
        node: &FilterNode,
    ) -> Option<(pg_sys::AttrNumber, FilterValueSlotId)> {
        let FilterNode::Comparison {
            operator,
            left,
            right,
        } = node
        else {
            return None;
        };
        let (column, value) = match (left, right) {
            (FilterScalar::Column(column), FilterScalar::Value(value))
            | (FilterScalar::Value(value), FilterScalar::Column(column)) => {
                (column, *value)
            }
            _ => return None,
        };
        (column.declared_type.type_oid == pg_sys::INT4OID
            && column.value_type.type_oid == pg_sys::INT4OID
            && operator.opno == pg_sys::Oid::from(INT4EQ_OPNO))
        .then_some((column.attno, value))
    }

    fn planned(&self, values: Vec<FilterValueSlotId>) -> HookPlannedFilter {
        HookPlannedFilter {
            values: values.into_boxed_slice(),
            codec_mode: self.mode.codec_mode(),
            bind_error: matches!(self.mode, HookTestMode::BindError),
        }
    }
}

impl FilterPushdownPlanner for HookFilterPlanner {
    type PlannedPredicate = HookPlannedFilter;
    type Error = HookFilterError;

    fn try_plan_filter(
        &mut self,
        fragment: &FilterFragment,
    ) -> Result<FilterPlan<Self::PlannedPredicate>, Self::Error> {
        if let Some((_, value)) = Self::equality_value(fragment.root()) {
            return Ok(FilterPlan::exact(
                self.planned(vec![value]),
                PushdownCosting::CostedPruning,
            ));
        }

        if !matches!(self.mode, HookTestMode::Widening) {
            return Ok(FilterPlan::Unsupported);
        }
        let FilterNode::Or(children) = fragment.root() else {
            return Ok(FilterPlan::Unsupported);
        };
        let mut attno = None;
        let mut values = Vec::with_capacity(children.len());
        for child in children {
            let Some((child_attno, value)) = Self::equality_value(child) else {
                return Ok(FilterPlan::Unsupported);
            };
            if attno.is_some_and(|expected| expected != child_attno) {
                return Ok(FilterPlan::Unsupported);
            }
            attno = Some(child_attno);
            values.push(value);
        }
        if values.len() < 2 {
            return Ok(FilterPlan::Unsupported);
        }
        Ok(FilterPlan::exact(
            self.planned(values),
            PushdownCosting::CostedPruning,
        ))
    }
}

impl FilterPushdown for HookIntegrationProvider {
    type Planner = HookFilterPlanner;
    type PlannedPredicate = HookPlannedFilter;
    type BoundPredicate = ();
    type Error = HookFilterError;

    fn begin_filter_planning(
        context: &FilterPlanningContext,
    ) -> Result<Self::Planner, Self::Error> {
        let name = relation_name(context.relation_oid())
            .expect("matched CustomScan relation must remain catalog-visible");
        let mode = HookTestMode::for_relation_name(&name)
            .expect("begin_filter_planning must follow supports_relation");
        Ok(HookFilterPlanner { mode })
    }

    fn encode_planned(
        predicate: &Self::PlannedPredicate,
        writer: &mut PlanDataWriter,
    ) -> Result<(), Self::Error> {
        if matches!(predicate.codec_mode, HookCodecMode::WrongTag) {
            writer.append_str("not-a-count");
            return Ok(());
        }
        writer.append_count(predicate.values.len());
        for value in &predicate.values {
            writer.append_count(value.index());
        }
        writer.append_bool(predicate.bind_error);
        if matches!(predicate.codec_mode, HookCodecMode::Trailing) {
            writer.append_i32(99);
        }
        Ok(())
    }

    fn decode_planned(
        reader: &mut PlanDataReader<'_>,
        binding_count: usize,
    ) -> Result<Self::PlannedPredicate, Self::Error> {
        let value_count = reader.read_count()?;
        let mut values = Vec::with_capacity(value_count);
        for _ in 0..value_count {
            let index = reader.read_count()?;
            let value = FilterValueSlotId::from_plan_data(index, binding_count)
                .ok_or(HookFilterError::InvalidSlot { index })?;
            values.push(value);
        }
        let bind_error = reader.read_bool()?;
        Ok(HookPlannedFilter {
            values: values.into_boxed_slice(),
            codec_mode: HookCodecMode::Valid,
            bind_error,
        })
    }

    fn bind_filter(
        predicate: &Self::PlannedPredicate,
        values: FilterValueBindings<'_>,
    ) -> Result<FilterBindResult<Self::BoundPredicate>, Self::Error> {
        if predicate.bind_error {
            return Err(HookFilterError::BindFailure);
        }
        for &value in &predicate.values {
            let _ = values.value(value);
        }
        Ok(FilterBindResult::Bound(()))
    }
}

impl LagodbCustomScanProvider for HookIntegrationProvider {
    const NAME: &'static CStr = PROVIDER_NAME;
    type PrivateData = HookIntegrationPrivate;
    type State = HookIntegrationState;

    fn supports_relation(ctx: &RelationContext<'_>) -> bool {
        relation_name(ctx.rel_oid())
            .as_deref()
            .and_then(HookTestMode::for_relation_name)
            .is_some()
    }

    fn create_path(
        ctx: &PathContext<'_>,
        variant: &PathVariant<'_>,
        builder: CustomPathBuilder<Self>,
    ) -> Option<CustomPathPlan<Self>> {
        let rel_name = relation_name(ctx.rel_oid())?;
        let mode = HookTestMode::for_relation_name(&rel_name)?;
        if !variant.pushdown.has_planned_filters() {
            return None;
        }

        let wants_variant = variant.kind == mode.path_kind();

        if !wants_variant {
            return None;
        }

        Some(builder.build(HookIntegrationPrivate))
    }

    fn create_state(_ctx: CreateStateContext<Self>) -> Self::State {
        HookIntegrationState
    }

    fn begin(_ctx: BeginContext<'_, Self>) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn next_slot(_ctx: NextSlotContext<'_, Self>) -> Result<bool, CustomScanError> {
        Ok(false)
    }

    fn rescan(_ctx: ReScanContext<'_, Self>) -> Result<(), CustomScanError> {
        Ok(())
    }

    fn end(_ctx: EndContext<'_, Self>) -> Result<(), CustomScanError> {
        Ok(())
    }
}

fn relation_name(rel_oid: pg_sys::Oid) -> Option<String> {
    unsafe {
        let raw = pg_sys::get_rel_name(rel_oid);
        if raw.is_null() {
            return None;
        }
        let name = CStr::from_ptr(raw).to_string_lossy().into_owned();
        pg_sys::pfree(raw.cast());
        Some(name)
    }
}

#[cfg(feature = "pg_test")]
mod pg_test;
