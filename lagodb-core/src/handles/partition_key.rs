//! Parse analysis of `PARTITION BY` for a relation without PostgreSQL partitions.
//!
//! A provider that keeps partitioning in its own metadata creates an ordinary
//! relation, so PostgreSQL neither analyzes the clause nor stores it.
//! `transformPartitionSpec()` and `ComputePartitionAttrs()` are private to
//! tablecmds.c, so this mirrors their column, expression and collation rules.
//! Expressions take the const-simplified form `RelationBuildPartitionKey()`
//! caches. Interpretation as a physical partitioning scheme belongs to the
//! provider, which also owns its type and expression restrictions. Re-audit
//! against tablecmds.c and partcache.c with the PostgreSQL-major-version boundary.
//!
//! Rules that protect PostgreSQL's own partition routing are not mirrored: the
//! LIST single-column rule and `PARTITION_MAX_KEYS` describe its executor, and
//! its IMMUTABLE rule is not a provider's lowering rule. A provider may
//! recognize its own marker functions in a key expression without running them.

use core::ffi::{CStr, c_char};
use core::ptr;
use std::panic::AssertUnwindSafe;

use pgrx::{PgSqlErrorCode, PgTryBuilder, pg_sys};

use crate::diag::PgReportError;
use crate::expr::pg::PgExprRef;

use super::RelationHandle;

/// The partitioning strategy written in `PARTITION BY`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PartitionStrategy {
    List,
    Range,
    Hash,
}

impl PartitionStrategy {
    /// PostgreSQL 15 keeps the strategy word from the grammar and validates it
    /// in `transformPartitionSpec()`, which this analysis replaces.
    ///
    /// # Safety
    ///
    /// `strategy` is the parser's NUL-terminated strategy word.
    #[cfg(feature = "pg15")]
    unsafe fn from_raw(strategy: *const c_char) -> Result<Self, PgReportError> {
        // SAFETY: guaranteed by the caller.
        let word = unsafe { CStr::from_ptr(strategy) };
        match word.to_bytes() {
            word if word.eq_ignore_ascii_case(b"hash") => Ok(Self::Hash),
            word if word.eq_ignore_ascii_case(b"list") => Ok(Self::List),
            word if word.eq_ignore_ascii_case(b"range") => Ok(Self::Range),
            _ => Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_INVALID_PARAMETER_VALUE,
                format!(
                    "unrecognized partitioning strategy \"{}\"",
                    word.to_string_lossy()
                ),
            )),
        }
    }

    /// The grammar of PostgreSQL 16 and later rejects any other strategy word,
    /// so this never fails.
    #[cfg(any(feature = "pg16", feature = "pg17", feature = "pg18"))]
    fn from_raw(
        strategy: pg_sys::PartitionStrategy::Type,
    ) -> Result<Self, PgReportError> {
        Ok(match strategy {
            pg_sys::PartitionStrategy::PARTITION_STRATEGY_LIST => Self::List,
            pg_sys::PartitionStrategy::PARTITION_STRATEGY_RANGE => Self::Range,
            pg_sys::PartitionStrategy::PARTITION_STRATEGY_HASH => Self::Hash,
            _ => unreachable!("PostgreSQL parsed an invalid partition strategy"),
        })
    }
}

/// An analyzed partition key.
///
/// Its expressions live in the memory context that analyzed them, so
/// [`PartitionKey::analyze`] lends the key to a closure instead of returning it.
/// Neither the key nor its fields can be copied out of that closure.
///
/// The key exposes PostgreSQL metadata only. Interpretation as a provider's
/// physical partitioning scheme belongs to that provider.
#[derive(Debug)]
pub struct PartitionKey {
    strategy: PartitionStrategy,
    fields: Vec<PartitionKeyField>,
}

/// One partition-key entry, in `PARTITION BY` order.
#[derive(Debug)]
pub struct PartitionKeyField {
    source: AnalyzedSource,
    type_oid: pg_sys::Oid,
    typmod: i32,
    collation: pg_sys::Oid,
}

/// What a partition-key entry reads from the table.
#[derive(Clone, Copy, Debug)]
pub enum PartitionKeySource<'a> {
    Column(pg_sys::AttrNumber),
    Expression(PgExprRef<'a>),
}

/// [`PartitionKeySource`] without a borrow, which [`PartitionKeyField::source`]
/// ties to the key.
#[derive(Debug)]
enum AnalyzedSource {
    Column(pg_sys::AttrNumber),
    Expression(*mut pg_sys::Expr),
}

impl PartitionKey {
    /// Analyze `spec` against the table created from the same statement and
    /// lend the key to `lower`.
    ///
    /// `spec` is left unchanged. The caller holds the relation's creation lock.
    pub fn analyze<R, E: From<PgReportError>>(
        relation: &RelationHandle<'_>,
        spec: &pg_sys::PartitionSpec,
        lower: impl FnOnce(&Self) -> Result<R, E>,
    ) -> Result<R, E> {
        // SAFETY: the statement owns `spec` and its raw element list, and the
        // handle keeps the Relation open. PostgreSQL ERRORs are captured here,
        // which drops their cursor position, so the ParseState has no source text.
        let key = unsafe {
            PgTryBuilder::new(AssertUnwindSafe(|| {
                Self::analyze_elements(relation, spec)
            }))
            .catch_others(|error| Err(PgReportError::from_caught(error)))
            .execute()
        }?;
        lower(&key)
    }

    unsafe fn analyze_elements(
        relation: &RelationHandle<'_>,
        spec: &pg_sys::PartitionSpec,
    ) -> Result<Self, PgReportError> {
        // SAFETY: the caller supplies a live raw PartitionSpec; its element
        // list holds PartitionElem nodes. The dummy ParseState mirrors
        // transformPartitionSpec's sole-rangetable-entry setup, which first
        // resolves the strategy.
        unsafe {
            let strategy = PartitionStrategy::from_raw(spec.strategy)?;
            let pstate = pg_sys::make_parsestate(ptr::null_mut());
            let nsitem = pg_sys::addRangeTableEntryForRelation(
                pstate,
                relation.as_raw(),
                pg_sys::AccessShareLock as i32,
                ptr::null_mut(),
                false,
                true,
            );
            pg_sys::addNSItemToQuery(pstate, nsitem, true, true, true);

            let count = pg_sys::list_length(spec.partParams);
            let mut fields = Vec::with_capacity(count as usize);
            for index in 0..count {
                let element = pg_sys::list_nth(spec.partParams, index)
                    .cast::<pg_sys::PartitionElem>();
                fields.push(PartitionKeyField::analyze(pstate, relation, &*element)?);
            }
            // An error above leaves the ParseState to the memory context, as
            // `transformPartitionSpec()` does on every path.
            pg_sys::free_parsestate(pstate);

            Ok(Self { strategy, fields })
        }
    }

    pub fn strategy(&self) -> PartitionStrategy {
        self.strategy
    }

    #[inline]
    pub fn fields(&self) -> &[PartitionKeyField] {
        &self.fields
    }
}

impl PartitionKeyField {
    unsafe fn analyze(
        pstate: *mut pg_sys::ParseState,
        relation: &RelationHandle<'_>,
        element: &pg_sys::PartitionElem,
    ) -> Result<Self, PgReportError> {
        // A provider's partitioning has no PostgreSQL comparison semantics, so
        // accepting an operator class would ignore what the user wrote.
        if !element.opclass.is_null() {
            return Err(PgReportError::from_message(
                PgSqlErrorCode::ERRCODE_FEATURE_NOT_SUPPORTED,
                "operator classes are not supported in this table's PARTITION BY",
            ));
        }

        // SAFETY: the parser sets exactly one of `name` and `expr`.
        let mut field = unsafe {
            if element.name.is_null() {
                Self::for_expression(pstate, element.expr)
            } else {
                Self::for_column(relation, element.name)?
            }
        };

        // SAFETY: the parser's collation name list and the resolved type are live.
        unsafe {
            if !element.collation.is_null() {
                field.collation = pg_sys::get_collation_oid(element.collation, false);
            }
            // A collatable type needs a collation and any other type must have
            // none, as `ComputePartitionAttrs()` checks.
            if pg_sys::type_is_collatable(field.type_oid) {
                if field.collation == pg_sys::InvalidOid {
                    return Err(PgReportError::from_parts(
                        PgSqlErrorCode::ERRCODE_INDETERMINATE_COLLATION,
                        "could not determine which collation to use for partition expression",
                        None,
                        Some(
                            "Use the COLLATE clause to set the collation explicitly."
                                .into(),
                        ),
                    ));
                }
            } else if field.collation != pg_sys::InvalidOid {
                let type_name =
                    CStr::from_ptr(pg_sys::format_type_be(field.type_oid));
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_DATATYPE_MISMATCH,
                    format!(
                        "collations are not supported by type {}",
                        type_name.to_string_lossy()
                    ),
                ));
            }
        }
        Ok(field)
    }

    unsafe fn for_column(
        relation: &RelationHandle<'_>,
        name: *const c_char,
    ) -> Result<Self, PgReportError> {
        let relid = relation.oid();
        // SAFETY: the parser supplies a NUL-terminated column name, and
        // get_attnum has established the attribute exists before its lookup.
        unsafe {
            let attno = pg_sys::get_attnum(relid, name);
            if attno == 0 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_UNDEFINED_COLUMN,
                    format!(
                        "column \"{}\" named in partition key does not exist",
                        CStr::from_ptr(name).to_string_lossy()
                    ),
                ));
            }
            if attno < 0 {
                return Err(PgReportError::from_message(
                    PgSqlErrorCode::ERRCODE_INVALID_OBJECT_DEFINITION,
                    format!(
                        "cannot use system column \"{}\" in partition key",
                        CStr::from_ptr(name).to_string_lossy()
                    ),
                ));
            }

            let mut type_oid = pg_sys::InvalidOid;
            let mut typmod = -1;
            let mut collation = pg_sys::InvalidOid;
            pg_sys::get_atttypetypmodcoll(
                relid,
                attno,
                &mut type_oid,
                &mut typmod,
                &mut collation,
            );
            Ok(Self {
                source: AnalyzedSource::Column(attno),
                type_oid,
                typmod,
                collation,
            })
        }
    }

    unsafe fn for_expression(
        pstate: *mut pg_sys::ParseState,
        raw: *mut pg_sys::Node,
    ) -> Self {
        // SAFETY: `raw` is a raw expression from the live statement. The copy
        // keeps parse transformation from scribbling on that statement.
        unsafe {
            let analyzed = pg_sys::transformExpr(
                pstate,
                pg_sys::copyObjectImpl(raw.cast()).cast(),
                pg_sys::ParseExprKind::EXPR_KIND_PARTITION_EXPRESSION,
            );
            pg_sys::assign_expr_collations(pstate, analyzed);
            let type_oid = pg_sys::exprType(analyzed);
            let typmod = pg_sys::exprTypmod(analyzed);
            let collation = pg_sys::exprCollation(analyzed);

            // "x COLLATE y" and "(x COLLATE y)" are the same key.
            let mut expression = analyzed;
            while (*expression).type_ == pg_sys::NodeTag::T_CollateExpr {
                expression = (*expression.cast::<pg_sys::CollateExpr>()).arg.cast();
            }

            // "(column)" is a simple attribute reference.
            let column = ((*expression).type_ == pg_sys::NodeTag::T_Var)
                .then(|| (*expression.cast::<pg_sys::Var>()).varattno)
                .filter(|attno| *attno > 0);
            Self {
                source: match column {
                    Some(attno) => AnalyzedSource::Column(attno),
                    None => AnalyzedSource::Expression(
                        pg_sys::eval_const_expressions(ptr::null_mut(), expression)
                            .cast(),
                    ),
                },
                type_oid,
                typmod,
                collation,
            }
        }
    }

    #[inline]
    pub fn source(&self) -> PartitionKeySource<'_> {
        match self.source {
            AnalyzedSource::Column(attno) => PartitionKeySource::Column(attno),
            // SAFETY: a key exists only inside `PartitionKey::analyze`, whose
            // caller's memory context holds the expression past this borrow of
            // the key, and eval_const_expressions never returns NULL for one.
            AnalyzedSource::Expression(expression) => {
                PartitionKeySource::Expression(unsafe {
                    PgExprRef::from_raw(expression)
                })
            }
        }
    }

    #[inline]
    pub fn type_oid(&self) -> pg_sys::Oid {
        self.type_oid
    }

    #[inline]
    pub fn typmod(&self) -> i32 {
        self.typmod
    }

    #[inline]
    pub fn collation(&self) -> pg_sys::Oid {
        self.collation
    }
}
