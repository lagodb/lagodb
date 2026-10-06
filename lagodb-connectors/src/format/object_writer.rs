//! Shared single-object and rolling-object write lifecycle.

use lagodb_core::copy::CopyOutputProgress;

use crate::error::ConnectorError;
use crate::storage::{FilePublication, ObjectFileSuffix, ObjectOutput, OutputWriter};

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

    fn bytes_written(&self) -> u64;

    fn finish(self) -> Result<OutputWriter, ConnectorError>;
}

pub(crate) trait ObjectFileEncoderFactory {
    type Input: ?Sized;
    type Encoder: ObjectFileEncoder<Input = Self::Input>;

    /// Canonical suffix for every independently readable file opened by this
    /// factory.
    fn file_suffix(&self) -> ObjectFileSuffix;

    fn open(&mut self, writer: OutputWriter)
    -> Result<Self::Encoder, ConnectorError>;
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
    upload: FilePublication,
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
    completed_bytes: u64,
    copy_progress: Option<CopyOutputProgress>,
}

impl<F> ObjectSetWriter<F>
where
    F: ObjectFileEncoderFactory,
{
    pub(crate) fn new(
        output: ObjectOutput,
        factory: F,
    ) -> Result<Self, ConnectorError> {
        let mut writer = Self {
            output,
            factory,
            current: None,
            completed_object: false,
            completed_bytes: 0,
            copy_progress: None,
        };
        if writer.output.open_before_execution() {
            writer.open_object()?;
        }
        Ok(writer)
    }

    pub(crate) fn set_copy_progress(&mut self, progress: CopyOutputProgress) {
        self.copy_progress = Some(progress);
    }

    pub(crate) fn write(&mut self, input: &F::Input) -> Result<(), ConnectorError> {
        let progress = self.ensure_open()?.encoder.write(input)?;
        if self.output.should_roll(progress.estimated_file_bytes()) {
            self.finish_current()?;
        }
        Ok(())
    }

    pub(crate) fn finish(
        self,
        empty: EmptyOutputPolicy,
    ) -> Result<(), ConnectorError> {
        self.finish_with_bytes(empty).map(|_| ())
    }

    pub(crate) fn finish_with_bytes(
        mut self,
        empty: EmptyOutputPolicy,
    ) -> Result<u64, ConnectorError> {
        if empty.should_open_empty(self.current.is_some(), self.completed_object) {
            self.open_object()?;
        }
        self.finish_current()?;
        Ok(self.completed_bytes)
    }

    pub(crate) fn bytes_written(&self) -> u64 {
        self.completed_bytes.saturating_add(
            self.current
                .as_ref()
                .map_or(0, |current| current.encoder.bytes_written()),
        )
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
        let (writer, upload) = FilePublication::start(allocation)?;
        let encoder = self.factory.open(writer)?;
        self.current = Some(OpenObject { encoder, upload });
        Ok(())
    }

    fn finish_current(&mut self) -> Result<(), ConnectorError> {
        let Some(OpenObject { encoder, upload }) = self.current.take() else {
            return Ok(());
        };
        let writer = encoder.finish()?;
        let bytes_written = writer.bytes_written();
        writer.finish_file()?;
        self.completed_bytes = self.completed_bytes.saturating_add(bytes_written);
        // The encoder has emitted framing and compression trailers. Publish
        // physical bytes before synchronous local publication or remote upload.
        // This is a file-boundary operation, never an encoder-write operation.
        if let Some(progress) = &self.copy_progress {
            progress.publish(self.completed_bytes);
        }
        upload.finish()?;
        self.completed_object = true;
        Ok(())
    }
}
