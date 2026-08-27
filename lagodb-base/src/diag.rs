use std::fmt;

pub(crate) fn info(message: impl fmt::Display) {
    lagodb_core::diag::log_info(message);
}

pub(crate) fn warning(message: impl fmt::Display) {
    lagodb_core::diag::report_warning(message);
}
