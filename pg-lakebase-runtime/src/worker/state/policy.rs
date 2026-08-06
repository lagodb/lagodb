use std::time::Duration;

pub(crate) struct RestartPolicy {
    initial: Duration,
    maximum: Duration,
    healthy: Duration,
}

impl RestartPolicy {
    pub(crate) const fn new(
        initial: Duration,
        maximum: Duration,
        healthy: Duration,
    ) -> Self {
        Self {
            initial,
            maximum,
            healthy,
        }
    }

    pub(crate) fn failure_count_after_crash(
        &self,
        failure_count: i32,
        start_time_ms: i64,
        now_ms: i64,
    ) -> i32 {
        let previous = if start_time_ms != 0
            && now_ms.saturating_sub(start_time_ms)
                >= i64::try_from(self.healthy.as_millis()).unwrap_or(i64::MAX)
        {
            0
        } else {
            failure_count
        };
        previous.saturating_add(1)
    }

    pub(crate) fn crash_backoff(&self, failure_count: i32) -> Duration {
        let exponent =
            u32::try_from(failure_count.saturating_sub(1)).unwrap_or(u32::MAX);
        let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
        self.initial.saturating_mul(multiplier).min(self.maximum)
    }
}
