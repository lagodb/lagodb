// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::{HashMap, HashSet};

use crate::Result;
use crate::compression::CompressionCodec;
use crate::io::{OutputFile, OutputFileWriter};
use crate::puffin::blob::Blob;
use crate::puffin::metadata::{BlobMetadata, FileMetadata, Flag};
use crate::puffin::validate_puffin_compression;

/// Puffin writer
pub struct PuffinWriter {
    writer: OutputFileWriter,
    is_header_written: bool,
    num_bytes_written: u64,
    written_blobs_metadata: Vec<BlobMetadata>,
    properties: HashMap<String, String>,
    footer_compression_codec: CompressionCodec,
    flags: HashSet<Flag>,
}

impl PuffinWriter {
    /// Returns a new Puffin writer
    pub fn new(
        output_file: OutputFile,
        properties: HashMap<String, String>,
        compress_footer: bool,
    ) -> Result<Self> {
        let mut flags = HashSet::<Flag>::new();
        let footer_compression_codec = if compress_footer {
            flags.insert(Flag::FooterPayloadCompressed);
            CompressionCodec::Lz4
        } else {
            CompressionCodec::None
        };

        let writer = output_file.create_writer()?;
        Ok(Self {
            writer,
            is_header_written: false,
            num_bytes_written: 0,
            written_blobs_metadata: Vec::new(),
            properties,
            footer_compression_codec,
            flags,
        })
    }

    /// Adds blob to Puffin file
    pub fn add(
        &mut self,
        blob: Blob,
        compression_codec: CompressionCodec,
    ) -> Result<()> {
        self.add_with_metadata(blob, compression_codec).map(|_| ())
    }

    /// Adds blob to Puffin file and returns the generated blob metadata.
    pub fn add_with_metadata(
        &mut self,
        blob: Blob,
        compression_codec: CompressionCodec,
    ) -> Result<BlobMetadata> {
        validate_puffin_compression(compression_codec)?;

        self.write_header_once()?;

        let offset = self.num_bytes_written;
        let compressed_bytes = compression_codec.compress(blob.data)?;
        let length = compressed_bytes.len().try_into()?;
        self.write(&compressed_bytes)?;
        let metadata = BlobMetadata {
            r#type: blob.r#type,
            fields: blob.fields,
            snapshot_id: blob.snapshot_id,
            sequence_number: blob.sequence_number,
            offset,
            length,
            compression_codec,
            properties: blob.properties,
        };
        self.written_blobs_metadata.push(metadata.clone());

        Ok(metadata)
    }

    /// Finalizes the Puffin file
    pub fn close(mut self) -> Result<()> {
        self.write_header_once()?;
        self.write_footer()?;
        self.writer.finish()
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        use std::io::Write;
        let length = bytes.len();
        self.writer.write_all(bytes)?;
        self.num_bytes_written += length as u64;
        Ok(())
    }

    fn write_header_once(&mut self) -> Result<()> {
        if !self.is_header_written {
            self.write(&FileMetadata::MAGIC)?;
            self.is_header_written = true;
        }
        Ok(())
    }

    fn footer_payload_bytes(&self) -> Result<Vec<u8>> {
        let file_metadata = FileMetadata {
            blobs: self.written_blobs_metadata.clone(),
            properties: self.properties.clone(),
        };
        let json = serde_json::to_string::<FileMetadata>(&file_metadata)?;
        let bytes = json.as_bytes();
        self.footer_compression_codec.compress(bytes.to_vec())
    }

    fn flags_bytes(&self) -> [u8; FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH as usize] {
        let mut result = [0; FileMetadata::FOOTER_STRUCT_FLAGS_LENGTH as usize];
        for flag in &self.flags {
            let byte_idx: usize = flag.byte_idx().into();
            result[byte_idx] |= 0x1 << flag.bit_idx();
        }
        result
    }

    fn write_footer(&mut self) -> Result<()> {
        let mut footer_payload_bytes = self.footer_payload_bytes()?;
        let footer_payload_bytes_length =
            u32::to_le_bytes(footer_payload_bytes.len().try_into()?);

        let mut footer_bytes = Vec::new();
        footer_bytes.extend(&FileMetadata::MAGIC);
        footer_bytes.append(&mut footer_payload_bytes);
        footer_bytes.extend(footer_payload_bytes_length);
        footer_bytes.extend(self.flags_bytes());
        footer_bytes.extend(&FileMetadata::MAGIC);

        self.write(&footer_bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tempfile::TempDir;

    use crate::Result;
    use crate::compression::CompressionCodec;
    use crate::io::{FileIO, InputFile, OutputFile};
    use crate::puffin::blob::Blob;
    use crate::puffin::metadata::FileMetadata;
    use crate::puffin::reader::PuffinReader;
    use crate::puffin::test_utils::{
        blob_0, blob_1, empty_footer_payload, empty_footer_payload_bytes,
        file_properties, java_empty_uncompressed_input_file,
        java_uncompressed_metric_input_file, java_zstd_compressed_metric_input_file,
        uncompressed_metric_file_metadata, zstd_compressed_metric_file_metadata,
    };
    use crate::puffin::writer::PuffinWriter;

    fn write_puffin_file(
        temp_dir: &TempDir,
        blobs: Vec<(Blob, CompressionCodec)>,
        properties: HashMap<String, String>,
    ) -> Result<OutputFile> {
        let file_io = FileIO::local();

        let path_buf = temp_dir.path().join("temp_puffin.bin");
        let temp_path = path_buf.to_str().unwrap();
        let output_file = file_io.new_output(temp_path)?;

        let mut writer = PuffinWriter::new(output_file, properties, false)?;
        for (blob, compression_codec) in blobs {
            writer.add(blob, compression_codec)?;
        }
        writer.close()?;

        file_io.new_output(temp_path)
    }

    fn read_all_blobs_from_puffin_file(input_file: InputFile) -> Vec<Blob> {
        let puffin_reader = PuffinReader::new(input_file);
        let mut blobs = Vec::new();
        let blobs_metadata = puffin_reader.file_metadata().unwrap().clone().blobs;
        for blob_metadata in blobs_metadata {
            blobs.push(puffin_reader.blob(&blob_metadata).unwrap());
        }
        blobs
    }

    #[test]
    fn test_write_uncompressed_empty_file() {
        let temp_dir = TempDir::new().unwrap();

        let input_file = write_puffin_file(&temp_dir, Vec::new(), HashMap::new())
            .unwrap()
            .to_input_file();

        assert_eq!(
            FileMetadata::read(&input_file).unwrap(),
            empty_footer_payload()
        );

        assert_eq!(
            input_file.read().unwrap().len(),
            FileMetadata::MAGIC_LENGTH as usize
                // no blobs since puffin file is empty
                + FileMetadata::MAGIC_LENGTH as usize
                + empty_footer_payload_bytes().len()
                + FileMetadata::FOOTER_STRUCT_LENGTH as usize
        )
    }

    fn blobs_with_compression(
        blobs: Vec<Blob>,
        compression_codec: CompressionCodec,
    ) -> Vec<(Blob, CompressionCodec)> {
        blobs
            .into_iter()
            .map(|blob| (blob, compression_codec))
            .collect()
    }

    #[test]
    fn test_write_uncompressed_metric_data() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression =
            blobs_with_compression(blobs.clone(), CompressionCodec::None);

        let input_file =
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .unwrap()
                .to_input_file();

        assert_eq!(
            FileMetadata::read(&input_file).unwrap(),
            uncompressed_metric_file_metadata()
        );

        assert_eq!(read_all_blobs_from_puffin_file(input_file), blobs)
    }

    #[test]
    fn test_write_zstd_compressed_metric_data() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression =
            blobs_with_compression(blobs.clone(), CompressionCodec::zstd_default());

        let input_file =
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .unwrap()
                .to_input_file();

        assert_eq!(
            FileMetadata::read(&input_file).unwrap(),
            zstd_compressed_metric_file_metadata()
        );

        assert_eq!(read_all_blobs_from_puffin_file(input_file), blobs)
    }

    #[test]
    fn test_write_lz4_compressed_metric_data() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression =
            blobs_with_compression(blobs.clone(), CompressionCodec::Lz4);

        assert_eq!(
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .unwrap_err()
                .to_string(),
            "FeatureUnsupported => LZ4 compression is not supported currently"
        );
    }

    fn get_file_as_byte_vec(input_file: InputFile) -> Vec<u8> {
        input_file.read().unwrap().to_vec()
    }

    fn assert_files_are_bit_identical(actual: OutputFile, expected: InputFile) {
        let actual_bytes = get_file_as_byte_vec(actual.to_input_file());
        let expected_bytes = get_file_as_byte_vec(expected);
        assert_eq!(actual_bytes, expected_bytes);
    }

    #[test]
    fn test_uncompressed_empty_puffin_file_is_bit_identical_to_java_generated_file() {
        let temp_dir = TempDir::new().unwrap();

        assert_files_are_bit_identical(
            write_puffin_file(&temp_dir, Vec::new(), HashMap::new()).unwrap(),
            java_empty_uncompressed_input_file(),
        )
    }

    #[test]
    fn test_uncompressed_metric_data_is_bit_identical_to_java_generated_file() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression =
            blobs_with_compression(blobs, CompressionCodec::None);

        assert_files_are_bit_identical(
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .unwrap(),
            java_uncompressed_metric_input_file(),
        )
    }

    #[test]
    fn test_zstd_compressed_metric_data_is_bit_identical_to_java_generated_file() {
        let temp_dir = TempDir::new().unwrap();
        let blobs = vec![blob_0(), blob_1()];
        let blobs_with_compression =
            blobs_with_compression(blobs, CompressionCodec::zstd_default());

        assert_files_are_bit_identical(
            write_puffin_file(&temp_dir, blobs_with_compression, file_properties())
                .unwrap(),
            java_zstd_compressed_metric_input_file(),
        )
    }
}
