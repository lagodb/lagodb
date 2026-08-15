//! Shared single-object and rolling-object write lifecycle.

use crate::error::ConnectorError;
use crate::storage::{
    ObjectFileSuffix, ObjectOutput, StagedObjectUpload, StagedObjectWriter,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FileWriteProgress {
    estimated_file_bytes: u64,
}

impl FileWriteProgress {
    pub(crate) const fn new(estimated_file_bytes: u64) -> Self {
        Self {
            estimated_file_bytes,
        }
    }

    const fn estimated_file_bytes(self) -> u64 {
        self.estimated_file_bytes
    }
}

pub(crate) trait ObjectFileEncoder {
    type Input: ?Sized;

    /// Write one format-safe split unit and report the current encoded file
    /// size from incremental O(1) state.
    fn write(
        &mut self,
        input: &Self::Input,
    ) -> Result<FileWriteProgress, ConnectorError>;

    fn finish(self) -> Result<StagedObjectWriter, ConnectorError>;
}

pub(crate) trait ObjectFileEncoderFactory {
    type Input: ?Sized;
    type Encoder: ObjectFileEncoder<Input = Self::Input>;

    /// Canonical suffix for every independently readable file opened by this
    /// factory.
    fn file_suffix(&self) -> ObjectFileSuffix;

    fn open(
        &mut self,
        writer: StagedObjectWriter,
    ) -> Result<Self::Encoder, ConnectorError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EmptyOutputPolicy {
    EmitFile,
    Skip,
}

impl EmptyOutputPolicy {
    const fn should_open_empty(
        self,
        has_current: bool,
        completed_object: bool,
    ) -> bool {
        matches!(self, Self::EmitFile) && !has_current && !completed_object
    }
}

struct OpenObject<E> {
    encoder: E,
    upload: StagedObjectUpload,
}

/// Owns all files produced by one statement-scoped output. Prefix targets are
/// approximate: rollover happens only after one complete encoder input.
pub(crate) struct ObjectSetWriter<F>
where
    F: ObjectFileEncoderFactory,
{
    output: ObjectOutput,
    factory: F,
    current: Option<OpenObject<F::Encoder>>,
    completed_object: bool,
}

impl<F> ObjectSetWriter<F>
where
    F: ObjectFileEncoderFactory,
{
    pub(crate) fn new(output: ObjectOutput, factory: F) -> Self {
        Self {
            output,
            factory,
            current: None,
            completed_object: false,
        }
    }

    pub(crate) fn write(&mut self, input: &F::Input) -> Result<(), ConnectorError> {
        let progress = self.ensure_open()?.encoder.write(input)?;
        if self.output.should_roll(progress.estimated_file_bytes()) {
            self.finish_current()?;
        }
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        empty: EmptyOutputPolicy,
    ) -> Result<(), ConnectorError> {
        if empty.should_open_empty(self.current.is_some(), self.completed_object) {
            self.open_object()?;
        }
        self.finish_current()
    }

    fn ensure_open(&mut self) -> Result<&mut OpenObject<F::Encoder>, ConnectorError> {
        if self.current.is_none() {
            self.open_object()?;
        }
        Ok(self
            .current
            .as_mut()
            .expect("the current object was initialized"))
    }

    fn open_object(&mut self) -> Result<(), ConnectorError> {
        let allocation = self.output.allocate_next(self.factory.file_suffix())?;
        let (writer, upload) = StagedObjectUpload::start(allocation)?;
        let encoder = self.factory.open(writer)?;
        self.current = Some(OpenObject { encoder, upload });
        Ok(())
    }

    fn finish_current(&mut self) -> Result<(), ConnectorError> {
        let Some(OpenObject { encoder, upload }) = self.current.take() else {
            return Ok(());
        };
        let writer = encoder.finish()?;
        writer.finish_local()?;
        upload.finish()?;
        self.completed_object = true;
        Ok(())
    }
}
