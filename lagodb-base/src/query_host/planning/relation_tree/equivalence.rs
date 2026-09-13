//! PostgreSQL equivalence-class rewrites for join keys hidden by SEMI/ANTI.

use lagodb_core::expr::{ColumnRef, ExprType};
use lagodb_query::plan::JoinKey;
use pgrx::{PgList, pg_sys};

use super::{RelationNode, RelationTreePlanner};

impl RelationTreePlanner {
    /// Orient a key across the current children, replacing an endpoint that a
    /// nested SEMI/ANTI join has removed with a PostgreSQL-proven equivalent.
    pub(super) unsafe fn orient_join_key(
        &mut self,
        left: &RelationNode,
        right: &RelationNode,
        key: JoinKey,
    ) -> Option<JoinKey> {
        if let Some(oriented) = Self::orient_visible_key(left, right, key) {
            self.expressions.record_join_key(oriented).ok()?;
            return Some(oriented);
        }

        let left_visible = Self::is_output_visible(left, right, key.left());
        let right_visible = Self::is_output_visible(left, right, key.right());
        if left_visible && right_visible {
            return None;
        }

        let left_equivalents = if left_visible {
            Vec::new()
        } else {
            unsafe { self.output_equivalents(left, right, key.left()) }?
        };
        let right_equivalents = if right_visible {
            Vec::new()
        } else {
            unsafe { self.output_equivalents(left, right, key.right()) }?
        };

        match (left_visible, right_visible) {
            (true, false) => right_equivalents.into_iter().find_map(|replacement| {
                self.orient_and_record(left, right, key.left(), replacement, key)
            }),
            (false, true) => left_equivalents.into_iter().find_map(|replacement| {
                self.orient_and_record(left, right, replacement, key.right(), key)
            }),
            (false, false) => {
                for replacement_left in left_equivalents {
                    for &replacement_right in &right_equivalents {
                        if let Some(oriented) = self.orient_and_record(
                            left,
                            right,
                            replacement_left,
                            replacement_right,
                            key,
                        ) {
                            return Some(oriented);
                        }
                    }
                }
                None
            }
            (true, true) => None,
        }
    }

    fn orient_and_record(
        &mut self,
        left: &RelationNode,
        right: &RelationNode,
        key_left: ColumnRef,
        key_right: ColumnRef,
        original: JoinKey,
    ) -> Option<JoinKey> {
        let candidate =
            JoinKey::try_new(key_left, key_right, original.operator()).ok()?;
        let oriented = Self::orient_visible_key(left, right, candidate)?;
        self.expressions.record_join_key(oriented).ok()?;
        Some(oriented)
    }

    fn orient_visible_key(
        left: &RelationNode,
        right: &RelationNode,
        key: JoinKey,
    ) -> Option<JoinKey> {
        if left.emits(key.left().scan) && right.emits(key.right().scan) {
            return Some(key);
        }
        if left.emits(key.right().scan) && right.emits(key.left().scan) {
            return JoinKey::try_new(key.right(), key.left(), key.operator()).ok();
        }
        None
    }

    #[inline]
    fn is_output_visible(
        left: &RelationNode,
        right: &RelationNode,
        column: ColumnRef,
    ) -> bool {
        left.emits(column.scan) || right.emits(column.scan)
    }

    /// Return the output-visible members of the equivalence class containing
    /// `pruned`. PostgreSQL creates these classes from merge/hash equality, so
    /// a replacement is a planner-proven semantic identity, not an inferred
    /// transitive equality owned by LagoDB.
    unsafe fn output_equivalents(
        &self,
        left: &RelationNode,
        right: &RelationNode,
        pruned: ColumnRef,
    ) -> Option<Vec<ColumnRef>> {
        let origin = self.scan_origin(pruned.scan)?;
        let root = origin.root;
        let pruned_rti = origin.rti;
        let equivalence_classes = unsafe {
            PgList::<pg_sys::EquivalenceClass>::from_pg((*root).eq_classes)
        };

        for equivalence_class in equivalence_classes.iter_ptr() {
            let members = unsafe {
                PgList::<pg_sys::EquivalenceMember>::from_pg(
                    (*equivalence_class).ec_members,
                )
            };
            let mut contains_pruned = false;
            let mut replacements = Vec::new();

            for member in members.iter_ptr() {
                let Some(var) = (unsafe {
                    Self::equivalence_member_var((*member).em_expr.cast())
                }) else {
                    continue;
                };
                if unsafe { (*var).varlevelsup } != 0
                    || unsafe { (*var).varattno } <= 0
                {
                    continue;
                }
                let rti = unsafe { (*var).varno } as pg_sys::Index;
                let attno = unsafe { (*var).varattno };
                if rti == pruned_rti && attno == pruned.attno {
                    contains_pruned = true;
                    continue;
                }

                let Some(binding) = self.binding(root, rti) else {
                    continue;
                };
                if !left.emits(binding.scan) && !right.emits(binding.scan) {
                    continue;
                }
                let value_type = ExprType {
                    type_oid: unsafe { (*var).vartype },
                    typmod: unsafe { (*var).vartypmod },
                    collation: unsafe { (*var).varcollid },
                };
                let replacement = ColumnRef {
                    scan: binding.scan,
                    attno,
                    declared_type: value_type,
                    value_type,
                };
                if !replacements.contains(&replacement) {
                    replacements.push(replacement);
                }
            }

            if contains_pruned {
                return (!replacements.is_empty()).then_some(replacements);
            }
        }
        None
    }

    /// PostgreSQL may wrap an EC Var in binary relabels or a PlaceHolderVar.
    /// Only the underlying base-column identity participates in this rewrite.
    unsafe fn equivalence_member_var(
        mut node: *mut pg_sys::Node,
    ) -> Option<*mut pg_sys::Var> {
        while !node.is_null() {
            node = match unsafe { (*node).type_ } {
                pg_sys::NodeTag::T_RelabelType => unsafe {
                    (*node.cast::<pg_sys::RelabelType>()).arg.cast()
                },
                pg_sys::NodeTag::T_PlaceHolderVar => unsafe {
                    (*node.cast::<pg_sys::PlaceHolderVar>()).phexpr.cast()
                },
                pg_sys::NodeTag::T_Var => return Some(node.cast()),
                _ => return None,
            };
        }
        None
    }
}
