//! PostgreSQL `List`s of expressions.

use pgrx::{PgList, pg_sys};

/// `exprs` as a PostgreSQL `List` allocated in the current memory context,
/// NIL without any. The list owns only its cells; the expressions stay with
/// their owner.
pub(crate) fn expr_list(
    exprs: impl IntoIterator<Item = *mut pg_sys::Expr>,
) -> *mut pg_sys::List {
    let mut list = PgList::new();
    for expr in exprs {
        list.push(expr);
    }
    list.into_pg()
}
