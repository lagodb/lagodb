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

//! Owned REST transaction requests prepared before catalog publication.

use std::fmt::{Debug, Formatter};

use super::request::HttpRequest;

/// A serialized REST transaction request ready for transport.
///
/// The request owns its URL, headers and JSON body. Sending it does not need
/// to revisit table metadata or serialize Iceberg requirements and updates.
pub struct PreparedRestCommit {
    pub(super) request: HttpRequest,
    table_count: usize,
}

impl PreparedRestCommit {
    pub(super) fn new(request: HttpRequest, table_count: usize) -> Self {
        Self {
            request,
            table_count,
        }
    }

    /// Returns the number of table changes in the atomic catalog request.
    pub const fn table_count(&self) -> usize {
        self.table_count
    }
}

impl Debug for PreparedRestCommit {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedRestCommit")
            .field("method", self.request.method())
            .field("url", &self.request.url_str())
            .field("table_count", &self.table_count)
            .finish_non_exhaustive()
    }
}
