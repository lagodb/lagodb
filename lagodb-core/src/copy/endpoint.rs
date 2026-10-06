//! Command-level COPY endpoint classification shared by utility consumers.

use std::ffi::CStr;

use crate::storage::profile::ObjectUri;

/// The I/O contract of one COPY command.
///
/// External URIs are handled by provider callbacks. Server files and programs
/// require PostgreSQL's privileged roles during preparation. Native formats
/// retain that permission contract while a provider owns the file I/O.
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
        if ObjectUri::has_uri_scheme(filename.to_bytes()) {
            return Self::ExternalUri;
        }
        Self::ServerFile
    }
}
