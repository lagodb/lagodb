//! Storage collection shape, independent of its encoding.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ObjectLocationKind {
    Exact,
    Prefix,
}
