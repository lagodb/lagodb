//! Command-level COPY endpoint classification shared by utility consumers.

use std::ffi::CStr;

/// The I/O contract of one COPY command.
///
/// External URIs are handled by provider callbacks. Server files and programs
/// use PostgreSQL's I/O and require its privileged roles during preparation.
/// The representation matches `LagodbCopyEndpoint` in `lagodb_copy.h`.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CopyEndpoint {
    ClientStream = 0,
    ServerFile = 1,
    ServerProgram = 2,
    ExternalUri = 3,
}

impl CopyEndpoint {
    /// Classify PostgreSQL's COPY filename without allocating or decoding it.
    ///
    /// PROGRAM always selects a server program, even when its command contains
    /// a URI. A missing filename selects STDIN/STDOUT. URI recognition does not
    /// claim a provider; unsupported schemes remain subject to utility routing.
    pub fn from_filename(filename: Option<&CStr>, is_program: bool) -> Self {
        if is_program {
            return Self::ServerProgram;
        }
        let Some(filename) = filename else {
            return Self::ClientStream;
        };
        let bytes = filename.to_bytes();
        if let Some(separator) = bytes.windows(3).position(|window| window == b"://")
        {
            let scheme = &bytes[..separator];
            if scheme.first().is_some_and(u8::is_ascii_alphabetic)
                && scheme.iter().all(|byte| {
                    byte.is_ascii_alphanumeric()
                        || matches!(*byte, b'+' | b'-' | b'.')
                })
            {
                return Self::ExternalUri;
            }
        }
        Self::ServerFile
    }
}
