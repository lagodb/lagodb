use pgrx::{PGRXSharedMemory, pg_sys};

mod coordinator;
mod policy;
mod process;
mod slot;

pub(crate) use coordinator::{CoordinatorSlot, CoordinatorStopDisposition};
pub(crate) use policy::RestartPolicy;
pub(crate) use process::ProcessState;
pub(crate) use slot::{Identity, Slot, WorkerKey, WorkerStopDisposition};

pub(crate) const INVALID_OID: u32 = pg_sys::InvalidOid.to_u32();
pub(crate) const MAX_WORKER_NAME_BYTES: usize = 255;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(crate) enum RegistrationState {
    Empty = 0,
    Registered = 1,
    Removing = 2,
}

impl RegistrationState {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Empty => "empty",
            Self::Registered => "registered",
            Self::Removing => "removing",
        }
    }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub(crate) struct SharedState {
    pub(crate) supervisor_pid: i32,
    pub(crate) supervisor_proc_number: i32,
    pub(crate) rescan_all: u8,
    pub(crate) _padding: [u8; 3],
}

impl Default for SharedState {
    fn default() -> Self {
        Self {
            supervisor_pid: 0,
            supervisor_proc_number: pg_sys::INVALID_PROC_NUMBER,
            rescan_all: 1,
            _padding: [0; 3],
        }
    }
}

// SAFETY: SharedState is repr(C), Copy, contains only scalar fields, and
// contains no references or process-local pointers.
unsafe impl PGRXSharedMemory for SharedState {}
