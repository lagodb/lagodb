//! Executor-side traversal and construction of provider predicates.

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::contract::{ColumnRef, ParamKey};
use crate::expr::pg::{
    PgBoolExpr, PgExprRef, PgNullTestKind, PgPredicateLeafRef, PgScalarExprRef,
};

pub use super::error::BuildPredicateError;
use super::params::{PgParamValue, ResolvedParam};
use super::translator::PgPredicateTranslator;
use super::value::{PgColumnRef, PgLiteral};

/// Builder for translating pushed PG expression trees into provider predicates.
pub struct PredicateBuilder<'a, T: PgPredicateTranslator> {
    translator: &'a mut T,
    exprs: &'a [*mut pg_sys::Expr],
    column_refs: ColumnRefs<'a>,
    resolved_params: ResolvedParams<'a>,
    var_resolver: ScanVarResolver<'a>,
}

/// Maps executor `Var` coordinates back to base-relation attribute numbers.
/// Relation-shaped scans use their RTI directly; projected CustomScans use
/// `INDEX_VAR` plus the plan-time `resno -> base attno` contract.
#[derive(Debug, Clone, Copy)]
pub(crate) enum ScanVarResolver<'a> {
    Relation {
        scan_relid: c_int,
    },
    Mapped {
        varno: c_int,
        source_attnos: &'a [pg_sys::AttrNumber],
    },
}

impl<'a> ScanVarResolver<'a> {
    pub(crate) fn relation(scan_relid: c_int) -> Self {
        Self::Relation { scan_relid }
    }

    pub(crate) fn mapped(
        varno: c_int,
        source_attnos: &'a [pg_sys::AttrNumber],
    ) -> Self {
        Self::Mapped {
            varno,
            source_attnos,
        }
    }

    fn expected_varno(self) -> c_int {
        match self {
            Self::Relation { scan_relid } => scan_relid,
            Self::Mapped { varno, .. } => varno,
        }
    }

    fn resolve(
        self,
        varno: c_int,
        varattno: pg_sys::AttrNumber,
    ) -> Result<pg_sys::AttrNumber, ResolveScanVarError> {
        if varno != self.expected_varno() {
            return Err(ResolveScanVarError::UnexpectedVarno);
        }
        if varattno <= 0 {
            return Err(ResolveScanVarError::UnsupportedAttno);
        }
        match self {
            Self::Relation { .. } => Ok(varattno),
            Self::Mapped { source_attnos, .. } => source_attnos
                .get(varattno as usize - 1)
                .copied()
                .ok_or(ResolveScanVarError::MappedResnoOutOfRange),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ResolveScanVarError {
    UnexpectedVarno,
    UnsupportedAttno,
    MappedResnoOutOfRange,
}

impl<'a, T: PgPredicateTranslator> PredicateBuilder<'a, T> {
    /// Create a builder for expressions whose scan Vars use `scan_relid`.
    pub fn new(
        translator: &'a mut T,
        exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [ColumnRef],
        resolved_params: &'a [ResolvedParam],
        scan_relid: c_int,
    ) -> Self {
        Self::with_var_resolver(
            translator,
            exprs,
            column_refs,
            resolved_params,
            ScanVarResolver::relation(scan_relid),
        )
    }

    pub(crate) fn with_var_resolver(
        translator: &'a mut T,
        exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [ColumnRef],
        resolved_params: &'a [ResolvedParam],
        var_resolver: ScanVarResolver<'a>,
    ) -> Self {
        Self {
            translator,
            exprs,
            column_refs: ColumnRefs::new(column_refs),
            resolved_params: ResolvedParams::new(resolved_params),
            var_resolver,
        }
    }

    /// Build one native predicate for `exprs[expr_index]`.
    ///
    /// # Safety
    ///
    /// Every pointer in the builder's expression slice must be NULL or a live
    /// PostgreSQL `Expr`; metadata must describe the same executor phase.
    pub unsafe fn build_one(
        &mut self,
        expr_index: usize,
    ) -> Result<T::Predicate, BuildPredicateError<T::Error>> {
        let raw = *self.exprs.get(expr_index).ok_or(
            BuildPredicateError::ExprIndexOutOfRange {
                expr_index,
                pushed_len: self.exprs.len(),
            },
        )?;
        let expr = unsafe { PgExprRef::from_raw_opt(raw) }
            .ok_or(BuildPredicateError::NullExpression { expr_index })?;
        let dispatched = self.dispatch_expr(expr, expr_index)?;
        match dispatched {
            DispatchResult::Predicate(p) => Ok(p),
            DispatchResult::Scalar(_) => {
                Err(BuildPredicateError::ExpectedPredicateAtTopLevel { expr_index })
            }
        }
    }

    /// Build one native predicate for every expression in the builder.
    ///
    /// The result order is the same as the input expression slice. Each item
    /// uses the same structural and lifetime contract as [`Self::build_one`].
    ///
    /// # Safety
    ///
    /// Every pointer in the builder's expression slice must be NULL or a live
    /// PostgreSQL `Expr`; metadata must describe the same executor phase.
    pub unsafe fn build_all(
        &mut self,
    ) -> Result<Vec<T::Predicate>, BuildPredicateError<T::Error>> {
        let mut predicates = Vec::with_capacity(self.exprs.len());
        for expr_index in 0..self.exprs.len() {
            predicates.push(unsafe { self.build_one(expr_index) }?);
        }
        Ok(predicates)
    }
}

enum DispatchResult<T: PgPredicateTranslator> {
    Scalar(T::Scalar),
    Predicate(T::Predicate),
}

impl<'a, T: PgPredicateTranslator> PredicateBuilder<'a, T> {
    fn dispatch_expr(
        &mut self,
        expr: PgExprRef<'_>,
        expr_index: usize,
    ) -> Result<DispatchResult<T>, BuildPredicateError<T::Error>> {
        let expr = expr.without_relabels();
        let tag = expr.node_tag();

        match tag {
            pg_sys::NodeTag::T_Var
            | pg_sys::NodeTag::T_Const
            | pg_sys::NodeTag::T_Param => self.dispatch_scalar(
                PgScalarExprRef::parse(expr).map_err(|source| {
                    BuildPredicateError::Structural { expr_index, source }
                })?,
                expr_index,
            ),

            pg_sys::NodeTag::T_OpExpr | pg_sys::NodeTag::T_NullTest => self
                .dispatch_leaf(
                    PgPredicateLeafRef::parse(expr).map_err(|source| {
                        BuildPredicateError::Structural { expr_index, source }
                    })?,
                    expr_index,
                ),

            pg_sys::NodeTag::T_BoolExpr => {
                let bool_expr = PgBoolExpr::try_from_expr(expr)
                    .expect("NodeTag established a BoolExpr");
                let boolop = bool_expr.boolop();
                let args = PgExprList::new(bool_expr.args_list());
                let n = args.len();
                match boolop {
                    pg_sys::BoolExprType::AND_EXPR
                    | pg_sys::BoolExprType::OR_EXPR => {
                        if n == 0 {
                            return Err(BuildPredicateError::EmptyBoolExpr {
                                expr_index,
                            });
                        }
                        let mut children = Vec::with_capacity(n);
                        for i in 0..n {
                            let raw = unsafe { args.expr_at(i) };
                            let child_expr = unsafe {
                                PgExprRef::from_raw_opt(raw).ok_or(
                                    BuildPredicateError::NullChild { expr_index },
                                )?
                            };
                            children.push(
                                self.dispatch_expr(child_expr, expr_index)?
                                    .into_predicate(expr_index)?,
                            );
                        }
                        let combined = if boolop == pg_sys::BoolExprType::AND_EXPR {
                            self.translator
                                .and(children)
                                .map_err(BuildPredicateError::Translator)?
                        } else {
                            self.translator
                                .or(children)
                                .map_err(BuildPredicateError::Translator)?
                        };
                        Ok(DispatchResult::Predicate(combined))
                    }
                    pg_sys::BoolExprType::NOT_EXPR => {
                        if n != 1 {
                            return Err(BuildPredicateError::MalformedNot {
                                expr_index,
                                arity: n,
                            });
                        }
                        let raw = unsafe { args.expr_at(0) };
                        let child_expr = unsafe {
                            PgExprRef::from_raw_opt(raw).ok_or(
                                BuildPredicateError::NullChild { expr_index },
                            )?
                        };
                        let child = self
                            .dispatch_expr(child_expr, expr_index)?
                            .into_predicate(expr_index)?;
                        let predicate = self
                            .translator
                            .not(child)
                            .map_err(BuildPredicateError::Translator)?;
                        Ok(DispatchResult::Predicate(predicate))
                    }
                    other => Err(BuildPredicateError::UnknownBoolOp {
                        expr_index,
                        boolop: other,
                    }),
                }
            }

            other => Err(BuildPredicateError::UnsupportedNodeTag {
                expr_index,
                tag: other,
            }),
        }
    }

    fn dispatch_scalar(
        &mut self,
        scalar: PgScalarExprRef<'_>,
        expr_index: usize,
    ) -> Result<DispatchResult<T>, BuildPredicateError<T::Error>> {
        let scalar = match scalar {
            PgScalarExprRef::Var(var) => {
                let varno = var.varno();
                let raw_attno = var.varattno();
                let attno = match self.var_resolver.resolve(varno, raw_attno) {
                    Ok(attno) => attno,
                    Err(ResolveScanVarError::UnexpectedVarno) => {
                        return Err(BuildPredicateError::OuterRelationVar {
                            expr_index,
                            varno,
                            scan_relid: self.var_resolver.expected_varno(),
                        });
                    }
                    Err(ResolveScanVarError::UnsupportedAttno) => {
                        return Err(BuildPredicateError::UnsupportedScanVarAttno {
                            expr_index,
                            attno: raw_attno,
                        });
                    }
                    Err(ResolveScanVarError::MappedResnoOutOfRange) => {
                        return Err(BuildPredicateError::MappedScanVarOutOfRange {
                            expr_index,
                            resno: raw_attno,
                            width: match self.var_resolver {
                                ScanVarResolver::Mapped { source_attnos, .. } => {
                                    source_attnos.len()
                                }
                                ScanVarResolver::Relation { .. } => 0,
                            },
                        });
                    }
                };
                let col = self.column_refs.lookup(expr_index, attno).ok_or(
                    BuildPredicateError::MissingColumnRef { expr_index, attno },
                )?;
                self.translator
                    .column(col)
                    .map_err(BuildPredicateError::Translator)?
            }
            PgScalarExprRef::Const(value) => self
                .translator
                .literal(PgLiteral::from_const(value))
                .map_err(BuildPredicateError::Translator)?,
            PgScalarExprRef::Param(param) => {
                let key = param.key();
                let resolved = self.resolved_params.lookup(key).ok_or(
                    BuildPredicateError::MissingParam {
                        expr_index,
                        paramkind: key.paramkind,
                        param_id: key.param_id,
                    },
                )?;
                self.translator
                    .param_value(resolved)
                    .map_err(BuildPredicateError::Translator)?
            }
        };
        Ok(DispatchResult::Scalar(scalar))
    }

    fn dispatch_leaf(
        &mut self,
        leaf: PgPredicateLeafRef<'_>,
        expr_index: usize,
    ) -> Result<DispatchResult<T>, BuildPredicateError<T::Error>> {
        let predicate = match leaf {
            PgPredicateLeafRef::Comparison { op, left, right } => {
                let left = self
                    .dispatch_scalar(left, expr_index)?
                    .into_scalar(expr_index)?;
                let right = self
                    .dispatch_scalar(right, expr_index)?
                    .into_scalar(expr_index)?;
                self.translator
                    .comparison(op, left, right)
                    .map_err(BuildPredicateError::Translator)?
            }
            PgPredicateLeafRef::NullTest { kind, value } => {
                let scalar = self
                    .dispatch_scalar(value, expr_index)?
                    .into_scalar(expr_index)?;
                match kind {
                    PgNullTestKind::IsNull => self
                        .translator
                        .is_null(scalar)
                        .map_err(BuildPredicateError::Translator)?,
                    PgNullTestKind::IsNotNull => self
                        .translator
                        .is_not_null(scalar)
                        .map_err(BuildPredicateError::Translator)?,
                }
            }
        };
        Ok(DispatchResult::Predicate(predicate))
    }
}

#[derive(Clone, Copy)]
struct ColumnRefs<'a>(&'a [ColumnRef]);

impl<'a> ColumnRefs<'a> {
    #[inline]
    fn new(refs: &'a [ColumnRef]) -> Self {
        Self(refs)
    }

    fn lookup(
        self,
        expr_index: usize,
        attno: pg_sys::AttrNumber,
    ) -> Option<PgColumnRef<'a>> {
        self.0
            .iter()
            .find(|c| c.expr_index == expr_index && c.attno == attno)
            .map(|c| PgColumnRef {
                rel_oid: c.rel_oid,
                attno: c.attno,
                atttypid: c.atttypid,
                attcollation: c.attcollation,
                name: c.name.as_deref(),
            })
    }
}

#[derive(Clone, Copy)]
struct ResolvedParams<'a>(&'a [ResolvedParam]);

impl<'a> ResolvedParams<'a> {
    #[inline]
    fn new(params: &'a [ResolvedParam]) -> Self {
        Self(params)
    }

    /// Lookup by full [`ParamKey`] (EXTERN vs EXEC must not collide on id alone).
    fn lookup(self, key: ParamKey) -> Option<PgParamValue<'a>> {
        self.0
            .iter()
            .find(|p| p.key() == key)
            .map(ResolvedParam::value)
    }
}

#[derive(Clone, Copy)]
struct PgExprList(*mut pg_sys::List);

impl PgExprList {
    #[inline]
    fn new(list: *mut pg_sys::List) -> Self {
        Self(list)
    }

    fn len(self) -> usize {
        if self.0.is_null() {
            return 0;
        }
        unsafe { (*self.0).length as usize }
    }

    unsafe fn expr_at(self, idx: usize) -> *mut pg_sys::Expr {
        if self.0.is_null() {
            return ptr::null_mut();
        }
        unsafe { pg_sys::list_nth(self.0, idx as c_int) as *mut pg_sys::Expr }
    }
}

impl<T: PgPredicateTranslator> DispatchResult<T> {
    fn into_scalar(
        self,
        expr_index: usize,
    ) -> Result<T::Scalar, BuildPredicateError<T::Error>> {
        match self {
            DispatchResult::Scalar(s) => Ok(s),
            DispatchResult::Predicate(_) => {
                Err(BuildPredicateError::UnexpectedPredicate { expr_index })
            }
        }
    }

    fn into_predicate(
        self,
        expr_index: usize,
    ) -> Result<T::Predicate, BuildPredicateError<T::Error>> {
        match self {
            DispatchResult::Predicate(p) => Ok(p),
            DispatchResult::Scalar(_) => {
                Err(BuildPredicateError::UnexpectedScalar { expr_index })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashMap};

    use proptest::prelude::*;

    use super::*;

    fn kind_of(idx: usize) -> pg_sys::ParamKind::Type {
        match idx {
            0 => pg_sys::ParamKind::PARAM_EXTERN,
            _ => pg_sys::ParamKind::PARAM_EXEC,
        }
    }

    fn make_value(
        kind: pg_sys::ParamKind::Type,
        id: c_int,
        datum_seed: usize,
    ) -> ResolvedParam {
        ResolvedParam::new(
            ParamKey {
                paramkind: kind,
                param_id: id,
            },
            pg_sys::Oid::from(23u32),
            pg_sys::Oid::INVALID,
            pg_sys::Datum::from(datum_seed),
            false,
        )
    }

    const ID_RANGE: c_int = 8;

    fn arb_keyset() -> impl Strategy<Value = Vec<(usize, c_int)>> {
        proptest::collection::vec((0usize..2, 0..ID_RANGE), 0..16).prop_map(|pairs| {
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for (kind, id) in pairs {
                if seen.insert((kind, id)) {
                    out.push((kind, id));
                }
            }
            out
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 256,
            ..ProptestConfig::default()
        })]

        #[test]
        fn lookup_param_keyed_by_param_key(keyset in arb_keyset()) {
            let mut values = Vec::with_capacity(keyset.len());
            let mut model = HashMap::with_capacity(keyset.len());
            for (idx, (kind_idx, id)) in keyset.iter().enumerate() {
                let value = make_value(kind_of(*kind_idx), *id, idx + 1);
                let key = value.key();
                prop_assert!(model.insert(key, idx + 1).is_none());
                values.push(value);
            }

            for (key, expected_seed) in &model {
                let resolved = ResolvedParams::new(&values)
                    .lookup(*key)
                    .expect("generated key must resolve");
                prop_assert_eq!(resolved.key(), *key);
                prop_assert_eq!(
                    unsafe { resolved.datum().as_raw() }.value(),
                    *expected_seed,
                );
            }
        }
    }

    #[test]
    fn extern_and_exec_with_same_id_resolve_independently() {
        let values = vec![
            make_value(pg_sys::ParamKind::PARAM_EXTERN, 1, 111),
            make_value(pg_sys::ParamKind::PARAM_EXEC, 1, 222),
        ];
        for (kind, expected) in [
            (pg_sys::ParamKind::PARAM_EXTERN, 111),
            (pg_sys::ParamKind::PARAM_EXEC, 222),
        ] {
            let value = ResolvedParams::new(&values)
                .lookup(ParamKey {
                    paramkind: kind,
                    param_id: 1,
                })
                .expect("parameter must resolve");
            assert_eq!(unsafe { value.datum().as_raw() }.value(), expected);
        }
    }

    #[test]
    fn absent_key_resolves_to_none() {
        let values = vec![make_value(pg_sys::ParamKind::PARAM_EXTERN, 1, 1)];
        assert!(
            ResolvedParams::new(&values)
                .lookup(ParamKey {
                    paramkind: pg_sys::ParamKind::PARAM_EXEC,
                    param_id: 1,
                })
                .is_none()
        );
    }
}
