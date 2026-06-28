//! Backend tests for Iceberg text collation capability oracle (syscache paths).

#[pgrx::pg_schema]
mod tests {
    use crate::customscan::pg_test::support::fixtures::{
        TEXT_ORDERED_OPNOS, TEXTEQ_OPNO, TEXTNE_OPNO,
    };
    use pg_lakebase_core::expr::nodes::PgComparisonOp;
    use pgrx::pg_sys;

    use crate::predicate::policy::{
        PredicateCapability, PredicatePushdownPolicy,
    };

    fn supported_predicate(
        type_oid: pg_sys::Oid,
        op_key: PgComparisonOp,
    ) -> PredicateCapability {
        PredicatePushdownPolicy::new().capability_for(type_oid, op_key)
    }

    fn is_c_or_posix_collation(oid: pg_sys::Oid) -> bool {
        PredicatePushdownPolicy::new().is_c_or_posix_collation(oid)
    }

    fn is_deterministic_collation(oid: pg_sys::Oid) -> bool {
        PredicatePushdownPolicy::new().is_deterministic_collation(oid)
    }

    /// `text` comparison triple; `inputcollid` is what `supported_predicate` reads.
    fn text_op(opno: u32, inputcollid: pg_sys::Oid) -> PgComparisonOp {
        PgComparisonOp {
            opno: pg_sys::Oid::from(opno),
            opfuncid: pg_sys::Oid::INVALID,
            opresulttype: pg_sys::Oid::INVALID,
            opcollid: pg_sys::Oid::INVALID,
            inputcollid,
        }
    }

    /// text `=` under a deterministic non-C collation is `ConservativePruning` (syscache path).
    #[pgrx::pg_test(schema = "tests")]
    fn cap_pg_text_eq_deterministic_collation_is_conservative_pruning() {
        let default_collid = pg_sys::DEFAULT_COLLATION_OID;

        // Guard the test's own premise: the default collation must NOT take either
        // short-circuit, so this genuinely exercises the syscache path.
        assert_ne!(
            default_collid,
            pg_sys::Oid::INVALID,
            "DEFAULT_COLLATION_OID must be a real OID",
        );
        assert!(
            !is_c_or_posix_collation(default_collid),
            "DEFAULT_COLLATION_OID must not be C/POSIX, so the determinism lookup \
         hits the syscache rather than short-circuiting",
        );

        // Syscache lookup: the default DB collation is deterministic.
        assert!(
            is_deterministic_collation(default_collid),
            "the default DB collation must resolve as deterministic via the syscache",
        );

        // Oracle verdict: text `=` under a deterministic collation -> ConservativePruning.
        assert_eq!(
            supported_predicate(
                pg_sys::TEXTOID,
                text_op(TEXTEQ_OPNO, default_collid)
            ),
            PredicateCapability::ConservativePruning,
            "text `=` under a deterministic collation must be ConservativePruning",
        );
        // varchar shares the text operators / branch.
        assert_eq!(
            supported_predicate(
                pg_sys::VARCHAROID,
                text_op(TEXTEQ_OPNO, default_collid)
            ),
            PredicateCapability::ConservativePruning,
            "varchar `=` under a deterministic collation must be ConservativePruning",
        );
    }

    /// text `=` under a non-deterministic collation is `Unsupported`.
    #[pgrx::pg_test(schema = "tests")]
    fn cap_pg_text_eq_non_deterministic_collation_is_unsupported() {
        use pgrx::Spi;

        // Create non-deterministic ICU collation in a subtransaction (rolled back at exit).
        Spi::run(
            "DO $$ \
         BEGIN \
           CREATE COLLATION nd_test_collation \
             (provider = icu, locale = 'und-u-ks-level2', deterministic = false); \
         EXCEPTION WHEN OTHERS THEN \
           NULL; \
         END $$;",
        )
        .expect("DO block to attempt non-deterministic collation creation");

        let nd_oid_raw = Spi::get_one::<i64>(
            "SELECT oid::int8 FROM pg_collation WHERE collname = 'nd_test_collation' \
         LIMIT 1",
        )
        .expect("collation OID lookup query");

        let Some(nd_oid_raw) = nd_oid_raw else {
            // ICU unavailable: assert built-in determinism semantics only.
            pgrx::log!(
                "cap_pg_text_eq_non_deterministic_collation_is_unsupported: could not \
             create a non-deterministic collation (no ICU support or incompatible \
             encoding); asserting built-in determinism semantics only",
            );
            // The default collation is deterministic -> text `=` is ConservativePruning.
            assert_eq!(
                supported_predicate(
                    pg_sys::TEXTOID,
                    text_op(TEXTEQ_OPNO, pg_sys::DEFAULT_COLLATION_OID),
                ),
                PredicateCapability::ConservativePruning,
            );
            return;
        };

        let nd_oid = pg_sys::Oid::from(nd_oid_raw as u32);

        // Syscache lookup: the created collation is non-deterministic.
        assert!(
            !is_deterministic_collation(nd_oid),
            "a `deterministic = false` collation must resolve as non-deterministic",
        );

        // Oracle verdict: text `=` under a non-deterministic collation -> Unsupported.
        assert_eq!(
            supported_predicate(pg_sys::TEXTOID, text_op(TEXTEQ_OPNO, nd_oid)),
            PredicateCapability::Unsupported,
            "text `=` under a non-deterministic collation must be Unsupported",
        );

        // Ordered text under a non-deterministic (hence non-C) collation is also
        // Unsupported — it fails the C/POSIX gate.
        for opno in TEXT_ORDERED_OPNOS {
            assert_eq!(
                supported_predicate(pg_sys::TEXTOID, text_op(opno, nd_oid)),
                PredicateCapability::Unsupported,
                "ordered text (opno {opno}) under a non-C collation must be Unsupported",
            );
        }
    }

    /// Ordered text under a non-C collation (and `<>`) is `Unsupported`.
    #[pgrx::pg_test(schema = "tests")]
    fn cap_pg_ordered_text_non_c_collation_is_unsupported() {
        let default_collid = pg_sys::DEFAULT_COLLATION_OID;

        // Premise: the default collation is not C/POSIX.
        assert!(
            !is_c_or_posix_collation(default_collid),
            "DEFAULT_COLLATION_OID must not be C/POSIX for this case to be meaningful",
        );

        for opno in TEXT_ORDERED_OPNOS {
            assert_eq!(
                supported_predicate(pg_sys::TEXTOID, text_op(opno, default_collid)),
                PredicateCapability::Unsupported,
                "ordered text (opno {opno}) under the default (non-C) collation must \
             be Unsupported",
            );
        }

        // `<>` never prunes against `[min, max]` metrics, regardless of collation.
        assert_eq!(
            supported_predicate(
                pg_sys::TEXTOID,
                text_op(TEXTNE_OPNO, default_collid)
            ),
            PredicateCapability::Unsupported,
            "text `<>` must be Unsupported",
        );
    }

    /// Ordered text under C / POSIX is `ConservativePruning`.
    #[pgrx::pg_test(schema = "tests")]
    fn cap_pg_ordered_text_c_posix_collation_is_conservative_pruning() {
        for collid in [pg_sys::C_COLLATION_OID, pg_sys::POSIX_COLLATION_OID] {
            assert!(
                is_c_or_posix_collation(collid),
                "{collid:?} must be recognized as C/POSIX",
            );
            for opno in TEXT_ORDERED_OPNOS {
                assert_eq!(
                    supported_predicate(pg_sys::TEXTOID, text_op(opno, collid)),
                    PredicateCapability::ConservativePruning,
                    "ordered text (opno {opno}) under C/POSIX must be ConservativePruning",
                );
            }
            // text `=` under C/POSIX is also pushable (C/POSIX are deterministic).
            assert_eq!(
                supported_predicate(pg_sys::TEXTOID, text_op(TEXTEQ_OPNO, collid)),
                PredicateCapability::ConservativePruning,
                "text `=` under C/POSIX must be ConservativePruning",
            );
        }
    }

    /// Unresolvable collation OID fail-safes to `Unsupported` for text comparisons.
    #[pgrx::pg_test(schema = "tests")]
    fn cap_pg_text_failsafe_unresolvable_collation_is_unsupported() {
        // Bogus OID with no `pg_collation` row — forces syscache-miss error path.
        let bogus = pg_sys::Oid::from(2_000_000_000u32);
        // Sanity: the bogus OID really has no catalog row, so this is a genuine
        // unresolvable case rather than an accidental hit.
        {
            use pgrx::Spi;
            let exists = Spi::get_one::<i64>(&format!(
                "SELECT count(*)::int8 FROM pg_collation WHERE oid = {}",
                u32::from(bogus),
            ))
            .expect("collation existence probe");
            assert_eq!(
                exists,
                Some(0),
                "the bogus OID must have no pg_collation row for the fail-safe path",
            );
        }

        // Fail-safe: the catch path downgrades the cache-lookup error to `false`.
        assert!(
            !is_deterministic_collation(bogus),
            "an unresolvable collation OID must fail-safe to non-deterministic",
        );
        // Fast path: InvalidOid is never deterministic.
        assert!(
            !is_deterministic_collation(pg_sys::Oid::INVALID),
            "InvalidOid must fail-safe to non-deterministic",
        );

        // Oracle verdict for text `=`: Unsupported under both unresolvable cases.
        assert_eq!(
            supported_predicate(pg_sys::TEXTOID, text_op(TEXTEQ_OPNO, bogus)),
            PredicateCapability::Unsupported,
            "text `=` under an unresolvable collation must be Unsupported",
        );
        assert_eq!(
            supported_predicate(
                pg_sys::TEXTOID,
                text_op(TEXTEQ_OPNO, pg_sys::Oid::INVALID)
            ),
            PredicateCapability::Unsupported,
            "text `=` under InvalidOid must be Unsupported",
        );

        // Ordered text under both unresolvable cases is also Unsupported (fails the
        // C/POSIX gate, which does not touch the syscache).
        for opno in TEXT_ORDERED_OPNOS {
            assert_eq!(
                supported_predicate(pg_sys::TEXTOID, text_op(opno, bogus)),
                PredicateCapability::Unsupported,
                "ordered text (opno {opno}) under an unresolvable collation must be \
             Unsupported",
            );
            assert_eq!(
                supported_predicate(
                    pg_sys::TEXTOID,
                    text_op(opno, pg_sys::Oid::INVALID)
                ),
                PredicateCapability::Unsupported,
                "ordered text (opno {opno}) under InvalidOid must be Unsupported",
            );
        }
    }
}
