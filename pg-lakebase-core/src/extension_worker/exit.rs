use std::time::Duration;

/// Scheduling directive returned by a database-local extension worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerExit {
    Dormant,
    RestartImmediately,
    RestartAfter(Duration),
}

impl WorkerExit {
    pub fn decode(code: i64) -> Result<Self, WorkerExitCodeError> {
        match code {
            0 => Ok(Self::Dormant),
            -1 => Ok(Self::RestartImmediately),
            value if value > 0 => {
                Ok(Self::RestartAfter(Duration::from_millis(value as u64)))
            }
            _ => Err(WorkerExitCodeError(code)),
        }
    }

    pub fn encode(self) -> i64 {
        match self {
            Self::Dormant => 0,
            Self::RestartImmediately => -1,
            Self::RestartAfter(delay) if delay.is_zero() => -1,
            Self::RestartAfter(delay) => {
                i64::try_from(delay.as_millis().max(1)).unwrap_or(i64::MAX)
            }
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid worker exit code {0}")]
pub struct WorkerExitCodeError(i64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exit_code_round_trip_and_saturation() {
        for directive in [
            WorkerExit::Dormant,
            WorkerExit::RestartImmediately,
            WorkerExit::RestartAfter(Duration::from_millis(42)),
        ] {
            assert_eq!(WorkerExit::decode(directive.encode()).unwrap(), directive);
        }
        assert!(WorkerExit::decode(-2).is_err());
        assert_eq!(WorkerExit::RestartAfter(Duration::ZERO).encode(), -1);
        assert_eq!(
            WorkerExit::RestartAfter(Duration::from_micros(1)).encode(),
            1
        );
        assert_eq!(WorkerExit::RestartAfter(Duration::MAX).encode(), i64::MAX);
    }
}
