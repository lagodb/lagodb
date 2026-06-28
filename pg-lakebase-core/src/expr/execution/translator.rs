//! Executor-side predicate translation via [`PgPredicateTranslator`].

use core::ffi::c_int;
use core::ptr;

use pgrx::pg_sys;

use crate::expr::nodes::{
    ParamKey, PgBoolExpr, PgColumnRef, PgConst, PgExprRef, PgLiteral, PgNullTest,
    PgOpExpr, PgParam, PgParamValue, PgVar,
};
use crate::expr::split::ColumnRef;

/// Translate resolved PG expression leaves and boolean nodes into a native predicate.
pub trait PgPredicateTranslator {
    type Scalar;
    type Predicate;
    type Error: std::error::Error + 'static;

    fn column(&mut self, col: PgColumnRef<'_>) -> Result<Self::Scalar, Self::Error>;
    fn literal(&mut self, lit: PgLiteral<'_>) -> Result<Self::Scalar, Self::Error>;
    fn param_value(
        &mut self,
        param: PgParamValue,
    ) -> Result<Self::Scalar, Self::Error>;
    fn comparison(
        &mut self,
        op: PgComparisonOp,
        left: Self::Scalar,
        right: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;
    fn is_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;
    fn is_not_null(
        &mut self,
        value: Self::Scalar,
    ) -> Result<Self::Predicate, Self::Error>;
    fn and(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error>;
    fn or(
        &mut self,
        items: Vec<Self::Predicate>,
    ) -> Result<Self::Predicate, Self::Error>;
    fn not(&mut self, item: Self::Predicate) -> Result<Self::Predicate, Self::Error>;
}

pub use crate::expr::nodes::PgComparisonOp;

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
    #[inline]
    pub fn new(
        translator: &'a mut T,
        exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [ColumnRef],
        resolved_params: &'a [PgParamValue],
        scan_relid: c_int,
    ) -> Self {
        Self {
            translator,
            exprs,
            column_refs: ColumnRefs::new(column_refs),
            resolved_params: ResolvedParams::new(resolved_params),
            var_resolver: ScanVarResolver::relation(scan_relid),
        }
    }

    pub(crate) fn with_var_resolver(
        translator: &'a mut T,
        exprs: &'a [*mut pg_sys::Expr],
        column_refs: &'a [ColumnRef],
        resolved_params: &'a [PgParamValue],
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

    /// Build one native predicate per pushed expression.
    ///
    /// # Safety
    ///
    /// Every pointer in `exprs` must either be NULL or point to a live PostgreSQL
    /// `Expr` node in the current backend memory context. `column_refs` and
    /// `resolved_params` must describe those expressions for the same planner/executor
    /// phase, and `scan_relid` must be the scan relation RTI used by their `Var`
    /// nodes.
    pub unsafe fn build_all(
        &mut self,
    ) -> Result<Vec<T::Predicate>, BuildPredicateError<T::Error>> {
        let mut out = Vec::with_capacity(self.exprs.len());
        for expr_index in 0..self.exprs.len() {
            out.push(unsafe { self.build_one(expr_index) }?);
        }
        Ok(out)
    }

    /// Build one native predicate for `exprs[expr_index]`.
    ///
    /// # Safety
    ///
    /// Same contract as [`PredicateBuilder::build_all`]. In addition,
    /// `expr_index` must refer to the expression whose column metadata uses the
    /// same `expr_index` values in `column_refs`.
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
        let expr = unsafe { PgExprRef::from_raw(raw) };
        let dispatched = unsafe { self.dispatch_expr(expr, expr_index) }?;
        match dispatched {
            DispatchResult::Predicate(p) => Ok(p),
            DispatchResult::Scalar(_) => {
                Err(BuildPredicateError::ExpectedPredicateAtTopLevel { expr_index })
            }
        }
    }
}

enum DispatchResult<T: PgPredicateTranslator> {
    Scalar(T::Scalar),
    Predicate(T::Predicate),
}

impl<'a, T: PgPredicateTranslator> PredicateBuilder<'a, T> {
    unsafe fn dispatch_expr(
        &mut self,
        expr: PgExprRef<'_>,
        expr_index: usize,
    ) -> Result<DispatchResult<T>, BuildPredicateError<T::Error>> {
        let expr = unsafe { expr.without_relabels() };
        let tag = unsafe { expr.node_tag() };

        match tag {
            pg_sys::NodeTag::T_Var => {
                let var = unsafe { PgVar::try_from_expr(expr) }
                    .expect("node_tag matched T_Var but try_from_expr failed");
                let varno = unsafe { var.varno() };
                let raw_attno = unsafe { var.varattno() };
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
                let scalar = self
                    .translator
                    .column(col)
                    .map_err(BuildPredicateError::Translator)?;
                Ok(DispatchResult::Scalar(scalar))
            }

            pg_sys::NodeTag::T_Const => {
                let c = unsafe { PgConst::try_from_expr(expr) }
                    .expect("node_tag matched T_Const but try_from_expr failed");
                let lit = unsafe { PgLiteral::from_const(c) };
                let scalar = self
                    .translator
                    .literal(lit)
                    .map_err(BuildPredicateError::Translator)?;
                Ok(DispatchResult::Scalar(scalar))
            }

            pg_sys::NodeTag::T_Param => {
                let param = unsafe { PgParam::try_from_expr(expr) }
                    .expect("node_tag matched T_Param but try_from_expr failed");
                let key = unsafe { param.key() };
                let resolved = self.resolved_params.lookup(key).ok_or(
                    BuildPredicateError::MissingParam {
                        expr_index,
                        paramkind: key.paramkind,
                        param_id: key.param_id,
                    },
                )?;
                let scalar = self
                    .translator
                    .param_value(resolved)
                    .map_err(BuildPredicateError::Translator)?;
                Ok(DispatchResult::Scalar(scalar))
            }

            pg_sys::NodeTag::T_OpExpr => {
                let op_expr = unsafe { PgOpExpr::try_from_expr(expr) }
                    .expect("node_tag matched T_OpExpr but try_from_expr failed");
                let op = unsafe { op_expr.comparison_op() };
                let args = PgExprList::new(unsafe { op_expr.args_list() });
                let arg_count = args.len();
                if arg_count != 2 {
                    return Err(BuildPredicateError::UnsupportedOpExprArity {
                        expr_index,
                        arity: arg_count,
                    });
                }
                let left_ptr = unsafe { args.expr_at(0) };
                let right_ptr = unsafe { args.expr_at(1) };
                let left_expr = unsafe {
                    PgExprRef::from_raw_opt(left_ptr)
                        .ok_or(BuildPredicateError::NullChild { expr_index })?
                };
                let right_expr = unsafe {
                    PgExprRef::from_raw_opt(right_ptr)
                        .ok_or(BuildPredicateError::NullChild { expr_index })?
                };
                let left = unsafe {
                    self.dispatch_expr(left_expr, expr_index)?
                        .into_scalar(expr_index)?
                };
                let right = unsafe {
                    self.dispatch_expr(right_expr, expr_index)?
                        .into_scalar(expr_index)?
                };
                let predicate = self
                    .translator
                    .comparison(op, left, right)
                    .map_err(BuildPredicateError::Translator)?;
                Ok(DispatchResult::Predicate(predicate))
            }

            pg_sys::NodeTag::T_BoolExpr => {
                let bool_expr = unsafe { PgBoolExpr::try_from_expr(expr) }
                    .expect("node_tag matched T_BoolExpr but try_from_expr failed");
                let boolop = unsafe { bool_expr.boolop() };
                let args = PgExprList::new(unsafe { bool_expr.args_list() });
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
                            let child = unsafe {
                                self.dispatch_expr(child_expr, expr_index)?
                                    .into_predicate(expr_index)?
                            };
                            children.push(child);
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
                        let child = unsafe {
                            self.dispatch_expr(child_expr, expr_index)?
                                .into_predicate(expr_index)?
                        };
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

            pg_sys::NodeTag::T_NullTest => {
                let nt = unsafe { PgNullTest::try_from_expr(expr) }
                    .expect("node_tag matched T_NullTest but try_from_expr failed");
                if unsafe { nt.argisrow() } {
                    return Err(BuildPredicateError::RowNullTest { expr_index });
                }
                let arg = unsafe { nt.arg() }
                    .ok_or(BuildPredicateError::NullChild { expr_index })?;
                let scalar = unsafe {
                    self.dispatch_expr(arg, expr_index)?
                        .into_scalar(expr_index)?
                };
                let predicate = match unsafe { nt.nulltesttype() } {
                    pg_sys::NullTestType::IS_NULL => self
                        .translator
                        .is_null(scalar)
                        .map_err(BuildPredicateError::Translator)?,
                    pg_sys::NullTestType::IS_NOT_NULL => self
                        .translator
                        .is_not_null(scalar)
                        .map_err(BuildPredicateError::Translator)?,
                    other => {
                        return Err(BuildPredicateError::UnknownNullTestType {
                            expr_index,
                            nulltesttype: other,
                        });
                    }
                };
                Ok(DispatchResult::Predicate(predicate))
            }

            other => Err(BuildPredicateError::UnsupportedNodeTag {
                expr_index,
                tag: other,
            }),
        }
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
struct ResolvedParams<'a>(&'a [PgParamValue]);

impl<'a> ResolvedParams<'a> {
    #[inline]
    fn new(params: &'a [PgParamValue]) -> Self {
        Self(params)
    }

    /// Lookup by full [`ParamKey`] (EXTERN vs EXEC must not collide on id alone).
    fn lookup(self, key: ParamKey) -> Option<PgParamValue> {
        self.0.iter().find(|p| p.key() == key).copied()
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

/// Errors from [`PredicateBuilder::build_all`] / [`PredicateBuilder::build_one`].
#[derive(Debug, thiserror::Error)]
pub enum BuildPredicateError<E>
where
    E: std::error::Error + 'static,
{
    #[error(
        "PredicateBuilder::build_one: expr_index {expr_index} is out of range \
         (pushed len {pushed_len})"
    )]
    ExprIndexOutOfRange {
        expr_index: usize,
        pushed_len: usize,
    },

    #[error(
        "PredicateBuilder: pushed expression {expr_index} is a scalar; \
         expected a boolean predicate"
    )]
    ExpectedPredicateAtTopLevel { expr_index: usize },

    #[error(
        "PredicateBuilder: scalar context for expression {expr_index} \
         received a predicate"
    )]
    UnexpectedPredicate { expr_index: usize },

    #[error(
        "PredicateBuilder: predicate context for expression {expr_index} \
         received a scalar"
    )]
    UnexpectedScalar { expr_index: usize },

    #[error(
        "PredicateBuilder: no column_refs entry for \
         (expr_index={expr_index}, attno={attno})"
    )]
    MissingColumnRef {
        expr_index: usize,
        attno: pg_sys::AttrNumber,
    },

    #[error(
        "PredicateBuilder: expression {expr_index} contains scan-relation \
         Var with unsupported attno {attno}; pushed predicates must reference \
         user columns only"
    )]
    UnsupportedScanVarAttno {
        expr_index: usize,
        attno: pg_sys::AttrNumber,
    },

    #[error(
        "PredicateBuilder: expression {expr_index} references projected scan \
         resno {resno}, outside layout width {width}"
    )]
    MappedScanVarOutOfRange {
        expr_index: usize,
        resno: pg_sys::AttrNumber,
        width: usize,
    },

    #[error(
        "PredicateBuilder: param ({paramkind:?}, id {param_id}) referenced \
         by expression {expr_index} is not in the resolved-params slice"
    )]
    MissingParam {
        expr_index: usize,
        paramkind: pg_sys::ParamKind::Type,
        param_id: c_int,
    },

    #[error(
        "PredicateBuilder: expression {expr_index} contains a Var with \
         varno={varno} != scan_relid={scan_relid}; \
         replace_nestloop_params should have rewritten it"
    )]
    OuterRelationVar {
        expr_index: usize,
        varno: c_int,
        scan_relid: c_int,
    },

    #[error(
        "PredicateBuilder: OpExpr in expression {expr_index} has arity \
         {arity}; only binary comparisons are supported"
    )]
    UnsupportedOpExprArity { expr_index: usize, arity: usize },

    #[error("PredicateBuilder: empty AND/OR BoolExpr in expression {expr_index}")]
    EmptyBoolExpr { expr_index: usize },

    #[error(
        "PredicateBuilder: NOT in expression {expr_index} has arity \
         {arity}; expected exactly 1 child"
    )]
    MalformedNot { expr_index: usize, arity: usize },

    #[error(
        "PredicateBuilder: row-level NullTest in expression {expr_index} \
         is not supported in v1"
    )]
    RowNullTest { expr_index: usize },

    #[error("PredicateBuilder: NULL child pointer in expression {expr_index}")]
    NullChild { expr_index: usize },

    #[error(
        "PredicateBuilder: unknown BoolExprType {boolop} in expression \
         {expr_index}"
    )]
    UnknownBoolOp {
        expr_index: usize,
        boolop: pg_sys::BoolExprType::Type,
    },

    #[error(
        "PredicateBuilder: unknown NullTestType {nulltesttype} in \
         expression {expr_index}"
    )]
    UnknownNullTestType {
        expr_index: usize,
        nulltesttype: pg_sys::NullTestType::Type,
    },

    #[error(
        "PredicateBuilder: unsupported NodeTag {tag:?} in expression \
         {expr_index}"
    )]
    UnsupportedNodeTag {
        expr_index: usize,
        tag: pg_sys::NodeTag,
    },

    #[error("PredicateBuilder: translator error: {0}")]
    Translator(E),
}

#[cfg(test)]
mod tests {
    //! Host-safe `ResolvedParams` tests; full builder coverage is in `pg-lakebase-core-tests`.

    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeSet, HashMap};

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
    ) -> PgParamValue {
        PgParamValue {
            param_id: id,
            paramkind: kind,
            type_oid: pg_sys::Oid::from(23u32),
            collid: pg_sys::Oid::INVALID,
            datum: pg_sys::Datum::from(datum_seed),
            is_null: false,
        }
    }

    const ID_RANGE: c_int = 8;

    fn arb_keyset() -> impl Strategy<Value = Vec<(usize, c_int)>> {
        proptest::collection::vec((0usize..2, 0..ID_RANGE), 0..16).prop_map(|pairs| {
            let mut seen = BTreeSet::new();
            let mut out = Vec::new();
            for (k, id) in pairs {
                if seen.insert((k, id)) {
                    out.push((k, id));
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
            let mut values: Vec<PgParamValue> = Vec::with_capacity(keyset.len());
            let mut model: HashMap<ParamKey, usize> =
                HashMap::with_capacity(keyset.len());
            for (idx, (kind_idx, id)) in keyset.iter().enumerate() {
                let kind = kind_of(*kind_idx);
                let datum_seed = idx + 1;
                let v = make_value(kind, *id, datum_seed);
                let key = v.key();
                prop_assert!(
                    model.insert(key, datum_seed).is_none(),
                    "duplicate ParamKey in generated set: {:?}",
                    key
                );
                values.push(v);
            }

            for (key, expected_seed) in &model {
                let resolved = ResolvedParams::new(&values).lookup(*key);
                prop_assert!(
                    resolved.is_some(),
                    "present key {:?} resolved to None",
                    key
                );
                let resolved = resolved.unwrap();
                prop_assert_eq!(
                    resolved.key(),
                    *key,
                    "resolved value key {:?} != requested key {:?}",
                    resolved.key(),
                    key
                );
                prop_assert_eq!(
                    resolved.datum.value(),
                    *expected_seed,
                    "key {:?} resolved to the wrong value's datum",
                    key
                );
            }

            for kind_idx in 0usize..2 {
                let kind = kind_of(kind_idx);
                for id in 0..(ID_RANGE + 4) {
                    let key = ParamKey { paramkind: kind, param_id: id };
                    if model.contains_key(&key) {
                        continue;
                    }
                    prop_assert!(
                        ResolvedParams::new(&values).lookup(key).is_none(),
                        "absent key {:?} resolved to Some",
                        key
                    );
                }
            }
        }
    }

    #[test]
    fn collision_extern_and_exec_same_id_resolve_independently() {
        let extern_v = make_value(pg_sys::ParamKind::PARAM_EXTERN, 1, 111);
        let exec_v = make_value(pg_sys::ParamKind::PARAM_EXEC, 1, 222);
        let values = vec![extern_v, exec_v];

        let extern_key = ParamKey {
            paramkind: pg_sys::ParamKind::PARAM_EXTERN,
            param_id: 1,
        };
        let exec_key = ParamKey {
            paramkind: pg_sys::ParamKind::PARAM_EXEC,
            param_id: 1,
        };

        let r_extern = ResolvedParams::new(&values)
            .lookup(extern_key)
            .expect("extern present");
        assert_eq!(r_extern.key(), extern_key);
        assert_eq!(r_extern.datum.value(), 111);

        let r_exec = ResolvedParams::new(&values)
            .lookup(exec_key)
            .expect("exec present");
        assert_eq!(r_exec.key(), exec_key);
        assert_eq!(r_exec.datum.value(), 222);
    }

    #[test]
    fn absent_key_resolves_to_none() {
        let values = vec![make_value(pg_sys::ParamKind::PARAM_EXTERN, 1, 1)];
        let absent_other_kind = ParamKey {
            paramkind: pg_sys::ParamKind::PARAM_EXEC,
            param_id: 1,
        };
        assert!(
            ResolvedParams::new(&values)
                .lookup(absent_other_kind)
                .is_none()
        );
        let absent_id = ParamKey {
            paramkind: pg_sys::ParamKind::PARAM_EXTERN,
            param_id: 99,
        };
        assert!(ResolvedParams::new(&values).lookup(absent_id).is_none());
    }
}
