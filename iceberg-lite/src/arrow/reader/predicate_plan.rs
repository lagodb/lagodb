// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Plans the exact filtering stages for one file scan task.
//!
//! Predicates over generated metadata columns cannot run until after record
//! batch transformation. Physical top-level conjuncts are still safe to run
//! as an exact Parquet row filter and can also be reused for row-group and
//! page-index pruning. Mixed `OR` and `NOT` subtrees remain intact in the
//! post-transform residual.
//!
//! TODO(metadata-column SQL): this planner deliberately accepts an already
//! bound predicate with reserved metadata field IDs. The preferred PostgreSQL
//! SQL surface is an explicit function or table function so ordinary catalog
//! resolution can run without a PostgreSQL parser hook. The PG integration
//! must normalize that representation to reserved metadata field IDs, and the
//! scan-level binder described in `crate::scan` must separate physical manifest
//! pruning from full row evaluation before this file-level plan is built. This
//! reader must not depend on PostgreSQL syntax. A future bare `_pos` syntax may
//! use a PostgreSQL core parser-setup hook, but it must produce the same bound
//! predicate rather than adding another reader path.

use std::collections::HashSet;

use super::predicate_visitor::CollectFieldIdVisitor;
use crate::error::Result;
use crate::expr::BoundPredicate;
use crate::expr::visitors::bound_predicate_visitor::visit;
use crate::metadata_columns::is_metadata_field;

pub(super) struct FilePredicatePlan {
    parquet_filter_predicate: Option<BoundPredicate>,
    post_transform_residual: Option<BoundPredicate>,
    post_transform_field_ids: HashSet<i32>,
}

impl FilePredicatePlan {
    pub(super) fn try_new(
        predicate: Option<BoundPredicate>,
    ) -> Result<Self> {
        let Some(predicate) = predicate else {
            return Ok(Self {
                parquet_filter_predicate: None,
                post_transform_residual: None,
                post_transform_field_ids: HashSet::new(),
            });
        };

        let field_ids = Self::collect_field_ids(&predicate)?;
        if field_ids
            .iter()
            .all(|field_id| !is_metadata_field(*field_id))
        {
            return Ok(Self {
                parquet_filter_predicate: Some(predicate),
                post_transform_residual: None,
                post_transform_field_ids: HashSet::new(),
            });
        }

        let mut plan = Self {
            parquet_filter_predicate: None,
            post_transform_residual: None,
            post_transform_field_ids: HashSet::new(),
        };
        plan.partition_conjuncts(&predicate)?;
        Ok(plan)
    }

    pub(super) fn parquet_filter_predicate(
        &self,
    ) -> Option<&BoundPredicate> {
        self.parquet_filter_predicate.as_ref()
    }

    pub(super) fn post_transform_field_ids(&self) -> &HashSet<i32> {
        &self.post_transform_field_ids
    }

    pub(super) fn into_post_transform_residual(
        self,
    ) -> Option<BoundPredicate> {
        self.post_transform_residual
    }

    fn partition_conjuncts(&mut self, predicate: &BoundPredicate) -> Result<()> {
        if let BoundPredicate::And(expression) = predicate {
            let [left, right] = expression.inputs();
            self.partition_conjuncts(left)?;
            self.partition_conjuncts(right)?;
            return Ok(());
        }

        let field_ids = Self::collect_field_ids(predicate)?;
        if field_ids
            .iter()
            .any(|field_id| is_metadata_field(*field_id))
        {
            self.post_transform_field_ids.extend(field_ids);
            Self::append_conjunct(
                &mut self.post_transform_residual,
                predicate.clone(),
            );
        } else {
            Self::append_conjunct(
                &mut self.parquet_filter_predicate,
                predicate.clone(),
            );
        }

        Ok(())
    }

    fn collect_field_ids(predicate: &BoundPredicate) -> Result<HashSet<i32>> {
        let mut visitor = CollectFieldIdVisitor {
            field_ids: HashSet::new(),
        };
        visit(&mut visitor, predicate)?;
        Ok(visitor.field_ids())
    }

    fn append_conjunct(
        conjunction: &mut Option<BoundPredicate>,
        predicate: BoundPredicate,
    ) {
        let next = match (conjunction.take(), predicate) {
            (None, BoundPredicate::AlwaysTrue) => None,
            (current, BoundPredicate::AlwaysTrue) => current,
            (_, BoundPredicate::AlwaysFalse) => {
                Some(BoundPredicate::AlwaysFalse)
            }
            (Some(BoundPredicate::AlwaysFalse), _) => {
                Some(BoundPredicate::AlwaysFalse)
            }
            (None, predicate) => Some(predicate),
            (Some(current), predicate) => Some(current.and(predicate)),
        };
        *conjunction = next;
    }
}

#[cfg(test)]
mod tests {
    use std::ops::Not;
    use std::sync::Arc;

    use super::*;
    use crate::expr::accessor::StructAccessor;
    use crate::expr::{
        BinaryExpression, Bind, BoundReference, PredicateOperator, Reference,
    };
    use crate::metadata_columns::{
        RESERVED_COL_NAME_POS, RESERVED_FIELD_ID_POS,
    };
    use crate::spec::{
        Datum, NestedField, PrimitiveType, Schema, Type,
    };

    fn equality_predicate(
        field_id: i32,
        field_name: &str,
        field_type: PrimitiveType,
        literal: Datum,
    ) -> BoundPredicate {
        let field = Arc::new(NestedField::optional(
            field_id,
            field_name,
            Type::Primitive(field_type.clone()),
        ));
        let reference = BoundReference::new(
            field_name,
            field,
            Arc::new(StructAccessor::new(0, field_type)),
        );
        BoundPredicate::Binary(BinaryExpression::new(
            PredicateOperator::Eq,
            reference,
            literal,
        ))
    }

    fn physical_predicate(field_id: i32, value: i32) -> BoundPredicate {
        equality_predicate(
            field_id,
            &format!("field_{field_id}"),
            PrimitiveType::Int,
            Datum::int(value),
        )
    }

    fn position_predicate(value: i64) -> BoundPredicate {
        equality_predicate(
            RESERVED_FIELD_ID_POS,
            RESERVED_COL_NAME_POS,
            PrimitiveType::Long,
            Datum::long(value),
        )
    }

    #[test]
    fn pure_physical_predicate_stays_in_parquet_filter() {
        let predicate = physical_predicate(1, 10);
        let plan = FilePredicatePlan::try_new(Some(predicate.clone())).unwrap();

        assert_eq!(plan.parquet_filter_predicate(), Some(&predicate));
        assert!(plan.post_transform_field_ids().is_empty());
        assert!(plan.into_post_transform_residual().is_none());
    }

    #[test]
    fn mixed_conjunction_is_split_between_exact_filter_and_residual() {
        let physical = physical_predicate(1, 10);
        let metadata = position_predicate(5);
        let plan = FilePredicatePlan::try_new(Some(
            physical.clone().and(metadata.clone()),
        ))
        .unwrap();

        assert_eq!(plan.parquet_filter_predicate(), Some(&physical));
        assert_eq!(
            plan.post_transform_field_ids(),
            &HashSet::from([RESERVED_FIELD_ID_POS])
        );
        assert_eq!(plan.into_post_transform_residual(), Some(metadata));
    }

    #[test]
    fn physical_or_subtree_is_pushed_as_one_conjunct() {
        let physical_or = physical_predicate(1, 10)
            .or(physical_predicate(2, 20));
        let metadata = position_predicate(5);
        let plan = FilePredicatePlan::try_new(Some(
            physical_or.clone().and(metadata.clone()),
        ))
        .unwrap();

        assert_eq!(plan.parquet_filter_predicate(), Some(&physical_or));
        assert_eq!(plan.into_post_transform_residual(), Some(metadata));
    }

    #[test]
    fn mixed_or_subtree_remains_whole_in_post_transform_residual() {
        let physical = physical_predicate(1, 10);
        let metadata = position_predicate(5);
        let mixed_or = physical.or(metadata);
        let plan = FilePredicatePlan::try_new(Some(mixed_or.clone())).unwrap();

        assert!(plan.parquet_filter_predicate().is_none());
        assert_eq!(
            plan.post_transform_field_ids(),
            &HashSet::from([1, RESERVED_FIELD_ID_POS])
        );
        assert_eq!(plan.into_post_transform_residual(), Some(mixed_or));
    }

    #[test]
    fn mixed_not_subtree_remains_whole_in_post_transform_residual() {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(
                        1,
                        "field_1",
                        Type::Primitive(PrimitiveType::Int),
                    )
                    .into(),
                    NestedField::optional(
                        RESERVED_FIELD_ID_POS,
                        RESERVED_COL_NAME_POS,
                        Type::Primitive(PrimitiveType::Long),
                    )
                    .into(),
                ])
                .build()
                .unwrap(),
        );
        let predicate = Reference::new("field_1")
            .equal_to(Datum::int(10))
            .and(
                Reference::new(RESERVED_COL_NAME_POS)
                    .equal_to(Datum::long(5)),
            )
            .not()
            .bind(schema, true)
            .unwrap();
        let plan = FilePredicatePlan::try_new(Some(predicate.clone())).unwrap();

        assert!(plan.parquet_filter_predicate().is_none());
        assert_eq!(plan.into_post_transform_residual(), Some(predicate));
    }
}
