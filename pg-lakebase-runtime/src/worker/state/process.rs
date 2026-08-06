#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum ProcessState {
    Stopped = 0,
    Starting = 1,
    Running = 2,
    Restarting = 3,
    NotStarted = 4,
}

impl ProcessState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Stopped => "stopped",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Restarting => "restarting",
            Self::NotStarted => "not_started",
        }
    }

    pub(crate) const fn is_active(self) -> bool {
        !matches!(self, Self::Stopped | Self::NotStarted)
    }
}
