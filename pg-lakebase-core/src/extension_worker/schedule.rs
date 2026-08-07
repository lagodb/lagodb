use std::time::Duration;

/// Schedule returned by a database-local extension worker.
///
/// This describes when another invocation should be dispatched. Returning a
/// schedule does not mean that the current PostgreSQL background process has
/// stopped; the runtime supervisor confirms that separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerSchedule {
    Idle,
    RunImmediately,
    RunAfter(Duration),
}

impl WorkerSchedule {
    /// Returns the raw worker ABI result: `-1` restarts immediately, `0`
    /// stops cleanly, and a positive value restarts after that many milliseconds.
    pub fn into_raw(self) -> i64 {
        match self {
            Self::Idle => 0,
            Self::RunImmediately => -1,
            Self::RunAfter(delay) if delay.is_zero() => -1,
            Self::RunAfter(delay) => {
                i64::try_from(delay.as_millis()).unwrap_or(i64::MAX)
            }
        }
    }
}
