//! Provider-neutral predicate-tree negotiation.

use super::PredicateExpr;

/// Provider-owned result of negotiating one predicate tree.
pub enum PredicatePlan<P> {
    Unsupported,
    /// A safe widened tree whose provider artifact omits at least one child.
    /// The framework must reconstruct this predicate from independently
    /// normalized children before accepting it, so omitted runtime-value slots
    /// are compacted out and the retained ids are rebased.
    Partial(P),
    Conservative(P),
    Exact(P),
    /// Exact for its current truth set, but the provider predicate's native
    /// complement does not preserve SQL NULL semantics.
    ExactNoComplement(P),
}

impl<P> PredicatePlan<P> {
    pub fn into_predicate(self) -> Option<P> {
        match self {
            Self::Unsupported => None,
            Self::Partial(predicate)
            | Self::Conservative(predicate)
            | Self::Exact(predicate)
            | Self::ExactNoComplement(predicate) => Some(predicate),
        }
    }

    /// Negotiate a conjunction. Unsupported children are safely omitted and
    /// mark the artifact dependency-incomplete; no accepted child means
    /// unsupported.
    pub fn conjunction(
        children: impl IntoIterator<Item = Self>,
        combine: impl FnOnce(Vec<P>) -> P,
    ) -> Self {
        let mut accepted = Vec::new();
        let mut exact = true;
        let mut complementable = true;
        let mut partial = false;
        for child in children {
            match child {
                Self::Unsupported => {
                    exact = false;
                    partial = true;
                }
                Self::Partial(predicate) => {
                    exact = false;
                    partial = true;
                    accepted.push(predicate);
                }
                Self::Conservative(predicate) => {
                    exact = false;
                    accepted.push(predicate);
                }
                Self::Exact(predicate) => accepted.push(predicate),
                Self::ExactNoComplement(predicate) => {
                    complementable = false;
                    accepted.push(predicate);
                }
            }
        }
        if accepted.is_empty() {
            Self::Unsupported
        } else if partial {
            Self::Partial(combine(accepted))
        } else if exact && complementable {
            Self::Exact(combine(accepted))
        } else if exact {
            Self::ExactNoComplement(combine(accepted))
        } else {
            Self::Conservative(combine(accepted))
        }
    }

    /// Negotiate a disjunction. Every branch must be accepted because
    /// omitting one would create false negatives.
    pub fn disjunction(
        children: impl IntoIterator<Item = Self>,
        combine: impl FnOnce(Vec<P>) -> P,
    ) -> Self {
        let mut accepted = Vec::new();
        let mut exact = true;
        let mut complementable = true;
        let mut partial = false;
        for child in children {
            match child {
                Self::Unsupported => return Self::Unsupported,
                Self::Partial(predicate) => {
                    exact = false;
                    partial = true;
                    accepted.push(predicate);
                }
                Self::Conservative(predicate) => {
                    exact = false;
                    accepted.push(predicate);
                }
                Self::Exact(predicate) => accepted.push(predicate),
                Self::ExactNoComplement(predicate) => {
                    complementable = false;
                    accepted.push(predicate);
                }
            }
        }
        if accepted.is_empty() {
            Self::Unsupported
        } else if partial {
            Self::Partial(combine(accepted))
        } else if exact && complementable {
            Self::Exact(combine(accepted))
        } else if exact {
            Self::ExactNoComplement(combine(accepted))
        } else {
            Self::Conservative(combine(accepted))
        }
    }

    /// Negation is safe only for an exact child whose provider-native
    /// complement preserves the same SQL truth set.
    pub fn negate(self, negate: impl FnOnce(P) -> P) -> Self {
        match self {
            Self::Exact(predicate) => Self::Exact(negate(predicate)),
            Self::ExactNoComplement(_)
            | Self::Partial(_)
            | Self::Conservative(_)
            | Self::Unsupported => Self::Unsupported,
        }
    }
}

/// Typed provider adapter for one [`PredicateExpr`] specialization.
///
/// The shared tree walker owns AND widening, complete-OR, and exact-NOT
/// semantics. Implementations decide only leaf capability and construction of
/// their provider-owned immutable artifact.
pub trait PredicatePlanner<S, O> {
    type Predicate;
    type Error;

    fn always_true(&self) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn always_false(&self) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn strict_true(
        &self,
        value: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn strict_false(
        &self,
        value: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn comparison(
        &self,
        operator: &O,
        left: &S,
        right: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn is_null(
        &self,
        value: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn is_not_null(
        &self,
        value: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn is_nan(
        &self,
        value: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn is_not_nan(
        &self,
        value: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn starts_with(
        &self,
        value: &S,
        prefix: &S,
    ) -> Result<PredicatePlan<Self::Predicate>, Self::Error>;

    fn conjunction(&self, children: Vec<Self::Predicate>) -> Self::Predicate;

    fn disjunction(&self, children: Vec<Self::Predicate>) -> Self::Predicate;

    fn negate(&self, predicate: Self::Predicate) -> Self::Predicate;
}

impl<S, O> PredicateExpr<S, O> {
    pub fn plan_with<P>(
        &self,
        planner: &P,
    ) -> Result<PredicatePlan<P::Predicate>, P::Error>
    where
        P: PredicatePlanner<S, O>,
    {
        match self {
            Self::AlwaysTrue => planner.always_true(),
            Self::AlwaysFalse => planner.always_false(),
            Self::StrictTrue(value) => planner.strict_true(value),
            Self::StrictFalse(value) => planner.strict_false(value),
            Self::Comparison {
                operator,
                left,
                right,
            } => planner.comparison(operator, left, right),
            Self::IsNull(value) => planner.is_null(value),
            Self::IsNotNull(value) => planner.is_not_null(value),
            Self::IsNan(value) => planner.is_nan(value),
            Self::IsNotNan(value) => planner.is_not_nan(value),
            Self::StartsWith { value, prefix } => planner.starts_with(value, prefix),
            Self::And(children) => {
                let children = children
                    .iter()
                    .map(|child| child.plan_with(planner))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PredicatePlan::conjunction(children, |children| {
                    planner.conjunction(children)
                }))
            }
            Self::Or(children) => {
                let children = children
                    .iter()
                    .map(|child| child.plan_with(planner))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(PredicatePlan::disjunction(children, |children| {
                    planner.disjunction(children)
                }))
            }
            Self::Not(child) => {
                let child = child.plan_with(planner)?;
                Ok(child.negate(|predicate| planner.negate(predicate)))
            }
        }
    }
}
