//! Backend proptest for `PgOpExpr::arity` and `binary_operands`.

#[cfg(any(test, feature = "pg_test"))]
#[pgrx::pg_schema]
mod tests {
    use std::ptr;

    use crate::lakebase_core::support::pg::{
        INT4_EQ_OPNO, OpExprSpec, PgNodeBuilder,
    };
    use pg_lakebase_core::expr::nodes::{PgExprRef, PgOpExpr};
    use pgrx::pg_sys;
    use pgrx::pg_test;

    use proptest::prelude::*;
    use proptest::test_runner::TestRunner;

    /// `None` → NULL cell, `Some(v)` → int4 Const.
    unsafe fn build_args(spec: &[Option<i32>]) -> Vec<*mut pg_sys::Expr> {
        let nodes = PgNodeBuilder::new(1);
        spec.iter()
            .map(|cell| match cell {
                Some(v) => unsafe { nodes.int4_const(*v) },
                None => ptr::null_mut(),
            })
            .collect()
    }

    /// Assert `arity()` and `binary_operands()` for a constructed args list.
    fn opexpr_arity_case(spec: &[Option<i32>]) -> Result<(), TestCaseError> {
        unsafe {
            let nodes = PgNodeBuilder::new(1);
            let args = build_args(spec);
            let op_ptr = nodes
                .op_expr(OpExprSpec::int4_comparison(INT4_EQ_OPNO), &args)
                .cast::<pg_sys::OpExpr>();
            let expr = PgExprRef::from_raw(op_ptr.cast());
            let op = PgOpExpr::try_from_expr(expr)
                .expect("synthetic node is tagged T_OpExpr");

            prop_assert_eq!(
                op.arity(),
                spec.len(),
                "arity() must equal the constructed args list length",
            );

            let expect_pair =
                spec.len() == 2 && !args[0].is_null() && !args[1].is_null();

            match op.binary_operands() {
                Some((lhs, rhs)) => {
                    prop_assert!(
                        expect_pair,
                        "binary_operands() returned Some for arity {} \
                         (null cells: [{}]); expected None",
                        spec.len(),
                        args.iter()
                            .map(|p| if p.is_null() { "null" } else { "node" })
                            .collect::<Vec<_>>()
                            .join(","),
                    );
                    prop_assert_eq!(
                        lhs.as_ptr(),
                        args[0],
                        "binary_operands().0 must be the cell-0 operand pointer",
                    );
                    prop_assert_eq!(
                        rhs.as_ptr(),
                        args[1],
                        "binary_operands().1 must be the cell-1 operand pointer",
                    );
                    prop_assert_eq!(
                        lhs.node_tag(),
                        pg_sys::NodeTag::T_Const,
                        "cell-0 operand is the int4 Const we built",
                    );
                    prop_assert_eq!(
                        rhs.node_tag(),
                        pg_sys::NodeTag::T_Const,
                        "cell-1 operand is the int4 Const we built",
                    );
                }
                None => {
                    prop_assert!(
                        !expect_pair,
                        "binary_operands() returned None for a 2-arg OpExpr \
                         with both cells non-null; expected Some",
                    );
                }
            }

            Ok(())
        }
    }

    /// Randomized args (0..=4); arity and `binary_operands` match spec.
    /// Uses manual `TestRunner` because the macro expands to a host `#[test]`.
    #[pg_test]
    fn opexpr_arity_and_binary_operands() {
        let config = ProptestConfig {
            cases: 256,
            failure_persistence: None,
            ..ProptestConfig::default()
        };
        let mut runner = TestRunner::new(config);

        let strategy =
            proptest::collection::vec(proptest::option::of(any::<i32>()), 0..=4);

        runner
            .run(&strategy, |spec| opexpr_arity_case(&spec))
            .expect("OpExpr arity/binary_operands property failed");
    }

    /// Explicit arity cases (0–4 and null-operand variants).
    #[pg_test]
    fn opexpr_arity_explicit_cases() {
        let cases: &[(Vec<Option<i32>>, &str)] = &[
            (vec![], "arity 0 (NIL args)"),
            (vec![Some(10)], "arity 1"),
            (vec![Some(20), Some(21)], "arity 2, both non-null -> Some"),
            (vec![None, Some(31)], "arity 2, cell-0 NULL -> None"),
            (vec![Some(40), None], "arity 2, cell-1 NULL -> None"),
            (vec![None, None], "arity 2, both NULL -> None"),
            (vec![Some(50), Some(51), Some(52)], "arity 3"),
            (vec![Some(60), Some(61), Some(62), Some(63)], "arity 4"),
        ];

        for (spec, label) in cases {
            opexpr_arity_case(spec).unwrap_or_else(|e| {
                panic!("OpExpr arity case '{label}' failed: {e}")
            });
        }
    }
}
