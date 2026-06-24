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

use std::sync::Arc;

use crate::encryption::EncryptionManager;
use crate::error::Result;
use crate::io::FileIO;
use crate::spec::{ManifestList, SnapshotRef, TableMetadataRef};

/// Synchronous manifest-list loader.
///
/// Upstream iceberg-rust moved manifest-list loading behind a reader object so
/// callers can plug in async prefetch/encryption. iceberg-lite keeps the same
/// structural entry point, but executes through the local synchronous IO
/// abstraction.
pub struct ManifestListReader {
    snapshot: SnapshotRef,
    file_io: FileIO,
    table_metadata: TableMetadataRef,
    encryption_manager: Option<Arc<EncryptionManager>>,
}

impl ManifestListReader {
    /// Create a new synchronous manifest-list reader.
    pub(crate) fn new(
        snapshot: SnapshotRef,
        file_io: FileIO,
        table_metadata: TableMetadataRef,
        encryption_manager: Option<Arc<EncryptionManager>>,
    ) -> Self {
        Self {
            snapshot,
            file_io,
            table_metadata,
            encryption_manager,
        }
    }

    /// Load and parse the snapshot's manifest list.
    pub fn load(&self) -> Result<ManifestList> {
        self.snapshot.load_manifest_list_with_encryption(
            &self.file_io,
            &self.table_metadata,
            self.encryption_manager.as_deref(),
        )
    }
}
