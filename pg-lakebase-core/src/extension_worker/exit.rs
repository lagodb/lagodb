use std::time::Duration;

pub const WORKER_DIRECTIVE_ABI_VERSION: u16 = 2;
const ABI_TAG: u64 = (0x4c00_u64 | WORKER_DIRECTIVE_ABI_VERSION as u64) << 48;
const ABI_TAG_MASK: u64 = 0xffff_0000_0000_0000;
const PAYLOAD_MASK: u64 = !ABI_TAG_MASK;

/// Scheduling directive returned by a database-local extension worker.
///
/// This describes when another invocation should be dispatched. Returning a
/// directive does not mean that the current PostgreSQL background process has
/// stopped; the runtime supervisor confirms that separately.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WorkerDirective {
    Idle,
    RunImmediately,
    RunAfter(Duration),
}

impl WorkerDirective {
    pub fn decode(code: i64) -> Result<Self, WorkerDirectiveCodeError> {
        let bits = u64::try_from(code).map_err(|_| WorkerDirectiveCodeError(code))?;
        if bits & ABI_TAG_MASK != ABI_TAG {
            return Err(WorkerDirectiveCodeError(code));
        }
        match bits & PAYLOAD_MASK {
            0 => Ok(Self::Idle),
            1 => Ok(Self::RunImmediately),
            payload => Ok(Self::RunAfter(Duration::from_millis(payload - 1))),
        }
    }

    pub fn encode(self) -> i64 {
        let payload = match self {
            Self::Idle => 0,
            Self::RunImmediately => 1,
            Self::RunAfter(delay) if delay.is_zero() => 1,
            Self::RunAfter(delay) => u64::try_from(delay.as_millis())
                .unwrap_or(u64::MAX)
                .saturating_add(1)
                .min(PAYLOAD_MASK),
        };
        i64::try_from(ABI_TAG | payload)
            .expect("worker directive ABI tag must fit in i64")
    }
}

#[derive(Debug, thiserror::Error)]
#[error("invalid worker directive code {0}")]
pub struct WorkerDirectiveCodeError(i64);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn directive_code_round_trip_and_saturation() {
        for directive in [
            WorkerDirective::Idle,
            WorkerDirective::RunImmediately,
            WorkerDirective::RunAfter(Duration::from_millis(42)),
        ] {
            assert_eq!(
                WorkerDirective::decode(directive.encode()).unwrap(),
                directive
            );
        }
        assert!(WorkerDirective::decode(0).is_err());
        assert!(WorkerDirective::decode(-1).is_err());
        assert_eq!(
            WorkerDirective::RunAfter(Duration::ZERO).encode(),
            WorkerDirective::RunImmediately.encode()
        );
        assert_eq!(
            WorkerDirective::decode(
                WorkerDirective::RunAfter(Duration::MAX).encode()
            )
            .unwrap(),
            WorkerDirective::RunAfter(Duration::from_millis(PAYLOAD_MASK - 1))
        );
    }
}
