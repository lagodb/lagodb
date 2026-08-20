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

//! Session-aware catalog API.

use std::collections::HashMap;
use std::fmt::Debug;

#[cfg(test)]
use mockall::automock;
use typed_builder::TypedBuilder;
use uuid::Uuid;

use crate::Result;
use crate::catalog::{
    Namespace, NamespaceIdent, TableCommit, TableCreation, TableIdent,
};
use crate::sensitive::SensitiveString;
use crate::table::Table;

/// Context carried with operations on a [`SessionCatalog`].
#[derive(Debug, Clone, TypedBuilder)]
pub struct SessionContext {
    #[builder(default = Uuid::new_v4().to_string(), setter(into))]
    session_id: String,
    #[builder(default, setter(strip_option, into))]
    identity: Option<String>,
    #[builder(default)]
    properties: HashMap<String, String>,
    #[builder(default)]
    credentials: HashMap<String, SensitiveString>,
}

impl SessionContext {
    /// Creates an empty context with a new session identifier.
    pub fn empty() -> Self {
        Self::builder().build()
    }

    /// Returns this session's unique identifier.
    pub fn session_id(&self) -> &str {
        &self.session_id
    }

    /// Returns the user or principal associated with this session.
    pub fn identity(&self) -> Option<&str> {
        self.identity.as_deref()
    }

    /// Returns session properties.
    pub fn properties(&self) -> &HashMap<String, String> {
        &self.properties
    }

    /// Returns session credentials.
    pub fn credentials(&self) -> &HashMap<String, SensitiveString> {
        &self.credentials
    }
}

/// Catalog API whose calls carry an explicit session context.
#[cfg_attr(test, automock)]
pub trait SessionCatalog: Debug + Send + Sync {
    // The explicit lifetime is required so `automock` can generate a valid mock
    // for this sync method; clippy's `needless_lifetimes` is a false positive here.
    #[allow(clippy::needless_lifetimes)]
    fn list_namespaces<'a>(
        &self,
        context: &SessionContext,
        parent: Option<&'a NamespaceIdent>,
    ) -> Result<Vec<NamespaceIdent>>;

    fn create_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<Namespace>;

    fn get_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Namespace>;

    fn namespace_exists(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<bool>;

    fn update_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        properties: HashMap<String, String>,
    ) -> Result<()>;

    fn drop_namespace(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<()>;

    fn list_tables(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
    ) -> Result<Vec<TableIdent>>;

    fn create_table(
        &self,
        context: &SessionContext,
        namespace: &NamespaceIdent,
        creation: TableCreation,
    ) -> Result<Table>;

    fn load_table(
        &self,
        context: &SessionContext,
        table: &TableIdent,
    ) -> Result<Table>;

    fn drop_table(&self, context: &SessionContext, table: &TableIdent) -> Result<()>;

    fn purge_table(&self, context: &SessionContext, table: &TableIdent)
    -> Result<()>;

    fn table_exists(
        &self,
        context: &SessionContext,
        table: &TableIdent,
    ) -> Result<bool>;

    fn rename_table(
        &self,
        context: &SessionContext,
        src: &TableIdent,
        dest: &TableIdent,
    ) -> Result<()>;

    fn register_table(
        &self,
        context: &SessionContext,
        table: &TableIdent,
        metadata_location: String,
    ) -> Result<Table>;

    fn update_table(
        &self,
        context: &SessionContext,
        commit: TableCommit,
    ) -> Result<Table>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::SessionContext;
    use crate::sensitive::SensitiveString;

    #[test]
    fn empty_sessions_have_unique_uuid_ids() {
        let left = SessionContext::empty();
        let right = SessionContext::empty();
        assert!(uuid::Uuid::parse_str(left.session_id()).is_ok());
        assert_ne!(left.session_id(), right.session_id());
    }

    #[test]
    fn debug_redacts_session_credentials() {
        let secret = "session-secret-value";
        let context = SessionContext::builder()
            .credentials(HashMap::from([(
                "key".to_owned(),
                SensitiveString::from(secret.to_owned()),
            )]))
            .build();
        assert!(!format!("{context:?}").contains(secret));
    }
}
