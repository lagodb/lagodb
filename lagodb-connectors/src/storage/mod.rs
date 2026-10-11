//! Local-file and object-store access shared by COPY and foreign tables.

mod input;
mod input_file;
mod local;
mod local_input;
mod local_output;
mod location;
mod output;
mod path;
mod read_progress;
mod resolved_location;
mod upload;
mod uri;

pub(crate) use input::{InputFiles, StorageInput};
pub(crate) use input_file::InputFile;
pub(crate) use location::ObjectLocationKind;
pub(crate) use output::{AllocatedObject, ObjectFileSuffix, StorageOutput};
pub(crate) use path::StoragePath;
pub(crate) use read_progress::{ProgressReader, ReadProgress};
pub(crate) use resolved_location::ResolvedStorageLocation;
pub(crate) use upload::{FilePublication, OutputWriter};
pub(crate) use uri::ObjectUri;
