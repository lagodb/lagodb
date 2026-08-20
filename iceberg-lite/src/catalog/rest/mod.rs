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

//! Synchronous Iceberg REST catalog implementation.

pub mod auth;
mod catalog;
mod client;
mod endpoint;
mod request;
mod transaction;
mod types;

pub use auth::*;
pub use catalog::{
    REST_CATALOG_PROP_AUTH_TYPE, REST_CATALOG_PROP_DISABLE_HEADER_REDACTION,
    REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE, RestCatalog,
    RestCatalogBuilder, RestSessionCatalog, RestSessionCatalogBuilder,
};
pub use client::{HttpClient, HttpTransport};
pub use endpoint::Endpoint;
pub use request::{HttpRequest, HttpRequestBody};
pub use transaction::PreparedRestCommit;
pub use types::*;

#[cfg(test)]
mod tests;
